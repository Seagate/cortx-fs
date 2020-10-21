/*
 * Filename:         cortxfs_fops.c
 * Description:      CORTXFS file system file operations
 *
 * Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 * For any questions about this software or licensing,
 * please email opensource@seagate.com or cortx-questions@seagate.com.
 */

#include <string.h> /* memset */
#include <kvstore.h> /* kvstore */
#include <dstore.h> /* dstore */
#include <cortxfs.h> /* cfs_access */
#include "cortxfs_fh.h"
#include "cortxfs_internal.h" /* cfs_set_ino_oid */
#include <common/log.h> /* log_* */
#include <common/helpers.h> /* RC_* */
#include <sys/param.h> /* DEV_SIZE */
#include "kvtree.h"
#include <errno.h>
#include "operation.h"

int cfs_creat(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *parent_ino,
              char *name, mode_t mode, cfs_ino_t *newfile_ino)
{
	int rc;
	dstore_oid_t  oid;
	struct cfs_fh *parent_fh = NULL;
	struct stat *parent_stat = NULL;
	struct dstore *dstore = dstore_get();

	dassert(dstore);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent_ino, &parent_fh);

	parent_stat = cfs_fh_stat(parent_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_WRITE);

	/* Create tree entries, get new inode */
	RC_WRAP_LABEL(rc, out, cfs_create_entry, parent_fh, cred, name, NULL,
		      mode, newfile_ino, CFS_FT_FILE);

	/* Get new unique extstore kfid */
	RC_WRAP_LABEL(rc, out, dstore_get_new_objid, dstore, &oid);

	/* Set the ino-kfid key-val in kvs */
	RC_WRAP_LABEL(rc, out, cfs_set_ino_oid, cfs_fs, newfile_ino, &oid);

	/* Create the backend object with passed kfid */
	RC_WRAP_LABEL(rc, out, dstore_obj_create, dstore, cfs_fs, &oid);

out:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	log_trace("parent_ino=%llu name=%s newfile_ino=%llu rc=%d",
		  *parent_ino, name, *newfile_ino, rc);
	return rc;
}

int cfs_creat_ex(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *parent,
		 char *name, mode_t mode, struct stat *stat_in,
		 int stat_in_flags, cfs_ino_t *newfile,
		 struct stat *stat_out)
{
	int rc;
	cfs_ino_t object = 0;
	struct kvstore *kvstor = kvstore_get();
	struct kvs_idx index;

	dassert(kvstor && cfs_fs && parent && name && stat_in && newfile &&
		stat_out);

	index = cfs_fs->kvtree->index;

	/* NOTE: The following operations must be done within a single
	 * transaction.
	 */

	RC_WRAP(kvs_begin_transaction, kvstor, &index);

	RC_WRAP_LABEL(rc, out, cfs_creat, cfs_fs, cred, parent, name,
		      mode, &object);
	RC_WRAP_LABEL(rc, out, cfs_setattr, cfs_fs, cred, &object, stat_in,
		      stat_in_flags);
	RC_WRAP_LABEL(rc, out, cfs_getattr, cfs_fs, cred, &object, stat_out);

	RC_WRAP(kvs_end_transaction, kvstor, &index);

	*newfile = object;
	object = 0;

out:
	if (object != 0) {
		/* We don't have transactions, so that let's just remove the
		 * object.
		 */
		(void) cfs_unlink(cfs_fs, cred, parent, &object, name);
		(void) kvs_discard_transaction(kvstor, &index);
	}
	return rc;
}

static inline ssize_t __cfs_write(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
				  cfs_file_open_t *fd, void *buf,
				  size_t count, off_t offset)
{
	int rc;
	struct stat stat;
	dstore_oid_t oid;
	struct dstore *dstore = dstore_get();
	struct kvnode node = KVNODE_INIT_EMTPY;
	struct dstore_obj *obj = NULL;

	dassert(dstore);

	RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, &fd->ino, &oid);

	RC_WRAP(cfs_access, cfs_fs, cred, &fd->ino, CFS_ACCESS_WRITE);

	if (count == 0) {
		rc = 0;
		goto out;
	}

	ssize_t bs = dstore_get_bsize(dstore, &oid);
	if (bs < 0) {
		rc = bs;
		goto out;
	}

	RC_WRAP_LABEL(rc, out, dstore_obj_open, dstore, &oid, &obj);
	RC_WRAP_LABEL(rc, out, dstore_pwrite, obj, offset, count,
		      bs, (char *)buf);

	RC_WRAP(cfs_getattr, cfs_fs, cred, &fd->ino, &stat);
	RC_WRAP_LABEL(rc, out, cfs_amend_stat, &stat, STAT_MTIME_SET| STAT_CTIME_SET);

	if ((offset+count) > stat.st_size) {
		stat.st_size = offset+count;
		stat.st_blocks = (stat.st_size + DEV_BSIZE - 1) / DEV_BSIZE;
	}

	RC_WRAP_LABEL(rc, out, cfs_kvnode_init, &node, cfs_fs->kvtree, &fd->ino,
		      &stat);
	RC_WRAP_LABEL(rc, out, cfs_set_stat, &node);
	rc = count;
out:
	if (obj != NULL) {
		dstore_obj_close(obj);
	}
	kvnode_fini(&node);
	log_trace("cfs_write: ino=%llu fd=%p count=%lu offset=%ld rc=%d",
		  fd->ino, fd, count, (long)offset, rc);
	return rc;
}

ssize_t cfs_write(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_file_open_t *fd,
		 void *buf, size_t count, off_t offset)
{
	size_t rc;

	perfc_trace_inii(PFT_CFS_WRITE, PEM_CFS_TO_NFS);
	perfc_trace_attr(PEA_R_C_COUNT, count);
	perfc_trace_attr(PEA_R_C_OFFSET, offset);

	rc = __cfs_write(cfs_fs, cred, fd, buf, count, offset);

	perfc_trace_attr(PEA_R_C_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_VERIFY);

	return rc;
}

int cfs_truncate(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *ino,
		 struct stat *new_stat, int new_stat_flags)
{
	int rc;
	dstore_oid_t oid;
	struct dstore *dstore = dstore_get();
	struct dstore_obj *obj = NULL;
	struct stat stat;
	size_t old_size;
	size_t new_size;

	dassert(ino && new_stat && dstore);
	dassert((new_stat_flags & STAT_SIZE_SET) != 0);

	/* TODO:PERF: The caller can pass the current size */
	RC_WRAP_LABEL(rc, out, cfs_getattr, cfs_fs, cred, ino, &stat);

	old_size = stat.st_size;
	new_size = new_stat->st_size;
	new_stat->st_blocks = (new_size + DEV_BSIZE - 1) / DEV_BSIZE;

	/* If the caller wants to set mtime explicitly then
	 * mtime and ctime will be different. Othewise,
	 * we should keep them synchronous with each other.
	 */
	if ((new_stat_flags & STAT_MTIME_SET) == 0) {
		RC_WRAP_LABEL(rc, out, cfs_amend_stat, new_stat,
			      STAT_MTIME_SET | STAT_CTIME_SET);
		new_stat_flags |= (STAT_MTIME_SET | STAT_CTIME_SET);
	}

	RC_WRAP_LABEL(rc, out, cfs_setattr, cfs_fs, cred, ino, new_stat,
		      new_stat_flags);

	RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, ino, &oid);
	RC_WRAP_LABEL(rc, out, dstore_obj_open, dstore, &oid, &obj);
	RC_WRAP_LABEL(rc, out, dstore_obj_resize, obj, old_size, new_size);
out:
	if (obj != NULL) {
		dstore_obj_close(obj);
	}
	return rc;
}

static inline ssize_t __cfs_read(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
				 cfs_file_open_t *fd, void *buf,
				 size_t count, off_t offset)
{
	int rc;
	struct stat stat;
	dstore_oid_t oid;
	struct dstore *dstore = dstore_get();
	struct kvnode node = KVNODE_INIT_EMTPY;
	struct dstore_obj *obj = NULL;

	dassert(dstore);

	RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, &fd->ino, &oid);
	RC_WRAP(cfs_getattr, cfs_fs, cred, &fd->ino, &stat);
	RC_WRAP(cfs_access, cfs_fs, cred, &fd->ino, CFS_ACCESS_READ);

	/* Following are the cases which needs to be handled to ensure we are
	 * not reading the data more than data written on file
	 * 1. If file is empty( i.e: stat->st_size == 0) or count is zero
	 * return immediately with data read = 0
	 * 2. If read offset is beyond/equal to the data written( i.e:
	 * stat->st_size <= offset from where we are reading) then return
	 * immediately with data read = 0.
	 * 3.If amount of data to be read exceed the EOF( i.e: stat->st_size <
	 * ( offset + buffer_size ) ) then read the only available data with
	 * read_bytes = available bytes.
	 * 4. Read is within the written data so read the requested data.
	 */
	if (stat.st_size == 0 || stat.st_size <= offset || count == 0) {
		rc = 0;
		goto out;
	} else if (stat.st_size <= (offset + count)) {
		/* Let's read only written bytes */
		count = stat.st_size - offset;
	}

	ssize_t bs = dstore_get_bsize(dstore, &oid);
	if (bs < 0) {
		rc = bs;
		goto out;
	}

	RC_WRAP_LABEL(rc, out, dstore_obj_open, dstore, &oid, &obj);
	RC_WRAP_LABEL(rc, out, dstore_pread, obj, offset, count,
		      bs, (char *)buf);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, &stat, STAT_ATIME_SET);
	RC_WRAP_LABEL(rc, out, cfs_kvnode_init, &node, cfs_fs->kvtree, &fd->ino,
		      &stat);
	RC_WRAP_LABEL(rc, out, cfs_set_stat, &node);
	rc = count;
out:
	if (obj != NULL) {
		dstore_obj_close(obj);
	}

	kvnode_fini(&node);
	log_trace("cfs_read: ino=%llu fd=%p count=%lu offset=%ld rc=%d",
		  fd->ino, fd, count, (long)offset, rc);
	return rc;
}

ssize_t cfs_read(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_file_open_t *fd,
		 void *buf, size_t count, off_t offset)
{
	size_t rc;

	perfc_trace_inii(PFT_CFS_READ, PEM_CFS_TO_NFS);
	perfc_trace_attr(PEA_R_C_COUNT, count);
	perfc_trace_attr(PEA_R_C_OFFSET, offset);

	rc = __cfs_read(cfs_fs, cred, fd, buf, count, offset);

	perfc_trace_attr(PEA_R_C_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_VERIFY);

	return rc;
}

