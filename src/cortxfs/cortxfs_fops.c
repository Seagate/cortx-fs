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
#include <cfs_perfc.h>

int cfs_creat(struct cfs_fh *parent_fh, cfs_cred_t *cred, char *name,
              mode_t mode, cfs_ino_t *newfile_ino)
{
	int rc;
	dstore_oid_t  oid;
	cfs_ino_t child_ino = 0LL;
	cfs_ino_t *parent_ino = NULL;
	struct cfs_fs *cfs_fs = NULL;
	struct stat *parent_stat = NULL;
	struct dstore *dstore = dstore_get();

	perfc_trace_inii(PFT_CFS_CREATE, PEM_CFS_TO_NFS);
	dassert(dstore && parent_fh && cred && name && newfile_ino);

	cfs_fs = cfs_fs_from_fh(parent_fh);
	parent_stat = cfs_fh_stat(parent_fh);
	parent_ino = cfs_fh_ino(parent_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_WRITE);

	/* Create tree entries, get new inode */
	RC_WRAP_LABEL(rc, out, cfs_create_entry, parent_fh, cred, name, NULL,
		      mode, &child_ino, CFS_FT_FILE);

	/* Get new unique extstore kfid */
	RC_WRAP_LABEL(rc, out, dstore_get_new_objid, dstore, &oid);

	/* Set the ino-kfid key-val in kvs */
	RC_WRAP_LABEL(rc, out, cfs_set_ino_oid, cfs_fs, &child_ino, &oid);

	/* Create the backend object with passed kfid */
	RC_WRAP_LABEL(rc, out, dstore_obj_create, dstore, cfs_fs, &oid);
	*newfile_ino = child_ino;

out:
	log_trace("parent_ino=%llu name=%s child_ino=%llu rc=%d",
		  *parent_ino, name, child_ino, rc);
	perfc_trace_attr(PEA_CFS_CREATE_PARENT_INODE, *parent_ino);
	perfc_trace_attr(PEA_CFS_NEW_FILE_INODE, child_ino);
	perfc_trace_attr(PEA_CFS_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);
	return rc;
}

int cfs_creat_ex(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *parent,
		 char *name, mode_t mode, struct stat *stat_in,
		 int stat_in_flags, cfs_ino_t *newfile,
		 struct stat *stat_out)
{
	int rc;
	cfs_ino_t object = 0;
	struct cfs_fh *parent_fh = NULL;
	struct cfs_fh *child_fh = NULL;
	struct kvstore *kvstor = kvstore_get();
	struct kvs_idx index;

	perfc_trace_inii(PFT_CFS_CREATE_EX, PEM_CFS_TO_NFS);
	dassert(kvstor && cfs_fs && parent && name && stat_in && newfile &&
		stat_out);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent, &parent_fh);

	index = cfs_fs->kvtree->index;

	/* NOTE: The following operations must be done within a single
	 * transaction.
	 */

	RC_WRAP(kvs_begin_transaction, kvstor, &index);

	RC_WRAP_LABEL(rc, out, cfs_creat, parent_fh, cred, name, mode,
		      &object);
	RC_WRAP_LABEL(rc, cleanup, cfs_fh_from_ino,
		      cfs_fs, &object, &child_fh);
	RC_WRAP_LABEL(rc, cleanup, cfs_setattr, child_fh, cred, stat_in,
		      stat_in_flags);
	memcpy(stat_out, cfs_fh_stat(child_fh), sizeof(struct stat));

	RC_WRAP(kvs_end_transaction, kvstor, &index);

	*newfile = object;
	object = 0;

cleanup:
	if (object != 0) {
		/* We don't have transactions, so that let's just remove the
		 * object.
		 */
		(void) cfs_unlink2(parent_fh, child_fh, cred, name);
		(void) kvs_discard_transaction(kvstor, &index);

		if (child_fh != NULL) {
			cfs_fh_destroy(child_fh);
		}

		log_err("cfs_fs=%p parent_ino=%llu object=%llu name=%s rc=%d",
			cfs_fs, *parent, object, name, rc);
	}

out:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	log_debug("cfs_fs=%p parent_ino=%llu new_ino=%llu name=%s rc=%d",
		  cfs_fs, *parent, rc==0 ? *newfile : object, name, rc);

	perfc_trace_attr(PEA_CFS_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static inline ssize_t __cfs_write(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
				  cfs_file_open_t *fd, void *buf,
				  size_t count, off_t offset)
{
	int rc;
	dstore_oid_t oid;
	struct stat *stat = NULL;
	struct cfs_fh *fh = NULL;
	struct dstore *dstore = dstore_get();
	struct dstore_obj *obj = NULL;

	dassert(cfs_fs && cred && fd && buf);
	dassert(dstore);

	if (count == 0) {
		rc = 0;
		goto out;
	}

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, &fd->ino, &fh);
	stat = cfs_fh_stat(fh);

	RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, &fd->ino, &oid);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, stat, CFS_ACCESS_WRITE);

	RC_WRAP_LABEL(rc, out, dstore_obj_open, dstore, &oid, &obj);
	RC_WRAP_LABEL(rc, out, dstore_pwrite, obj, offset, count,
		      stat->st_blksize, (char *)buf);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, stat,
		      STAT_MTIME_SET|STAT_CTIME_SET);

	if ((offset + count) > stat->st_size) {
		stat->st_size = offset + count;
		/*  TODO: Check if DEV_BSIZE should be stat->st_blksize */
		stat->st_blocks = (stat->st_size + DEV_BSIZE - 1) / DEV_BSIZE;
	}
	rc = count;

out:
	if (obj != NULL) {
		dstore_obj_close(obj);
	}

	if (fh != NULL) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	log_trace("cfs_fs=%p ino=%llu fd=%p count=%lu offset=%ld rc=%d",
		  cfs_fs, fd->ino, fd, count, (long)offset, rc);
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
	struct cfs_fh *fh = NULL;
	struct stat *stat = NULL;
	size_t old_size;
	size_t new_size;

	dassert(ino && new_stat && dstore);
	dassert((new_stat_flags & STAT_SIZE_SET) != 0);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, ino, &fh);
	stat = cfs_fh_stat(fh);

	old_size = stat->st_size;
	new_size = new_stat->st_size;
	/*  TODO: Check if DEV_BSIZE should be stat->st_blksize */
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

	RC_WRAP_LABEL(rc, out, cfs_setattr, fh, cred, new_stat,
			new_stat_flags);

	RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, ino, &oid);
	RC_WRAP_LABEL(rc, out, dstore_obj_open, dstore, &oid, &obj);
	RC_WRAP_LABEL(rc, out, dstore_obj_resize, obj, old_size, new_size,
		      stat->st_blksize);
out:
	if (obj != NULL) {
		dstore_obj_close(obj);
	}

	if (fh != NULL) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	return rc;
}

static inline ssize_t __cfs_read(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
				 cfs_file_open_t *fd, void *buf,
				 size_t count, off_t offset)
{
	int rc;
	dstore_oid_t oid;
	size_t  byte_to_read = count;
	struct stat *stat = NULL;
	struct cfs_fh *fh = NULL;
	struct dstore *dstore = dstore_get();
	struct dstore_obj *obj = NULL;

	dassert(cfs_fs && cred && fd && buf);
	dassert(dstore);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, &fd->ino, &fh);
	stat = cfs_fh_stat(fh);

	RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, &fd->ino, &oid);
	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, stat, CFS_ACCESS_READ);

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
	if (stat->st_size == 0 || stat->st_size <= offset ||
	    byte_to_read == 0) {
		rc = 0;
		goto out;
	} else if (stat->st_size <= (offset + byte_to_read)) {
		/* Let's read only written bytes */
		byte_to_read = stat->st_size - offset;
	}

	RC_WRAP_LABEL(rc, out, dstore_obj_open, dstore, &oid, &obj);
	RC_WRAP_LABEL(rc, out, dstore_pread, obj, offset, byte_to_read,
		      stat->st_blksize, (char *)buf);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, stat, STAT_ATIME_SET);
	rc = byte_to_read;

out:
	if (obj != NULL) {
		dstore_obj_close(obj);
	}

	if (fh != NULL) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	log_trace("cfs_fs=%p ino=%llu fd=%p count=%lu offset=%ld rc=%d", cfs_fs,
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

