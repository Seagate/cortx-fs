/*
 * Filename:         cortxfs_ops.c
 * Description:      CORTXFS file system operations
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

#include <common/log.h> /* log_debug() */
#include <kvstore.h> /* struct kvstore */
#include <cortxfs.h> /* cfs_get_stat() */
#include <debug.h> /* dassert() */
#include <common/helpers.h> /* RC_WRAP_LABEL() */
#include <limits.h> /* PATH_MAX */
#include <string.h> /* memcpy() */
#include <sys/time.h> /* gettimeofday() */
#include <errno.h>  /* errno, -EINVAL */
#include <cortxfs_fh.h> /* cfs_fh */
#include "cortxfs_internal.h" /* dstore_obj_delete() */
#include <common.h> /* likely */
#include "kvtree.h"
#include "operation.h"
#include <cfs_perfc.h>

static int cfs_detach2(struct cfs_fh *parent_fh, struct cfs_fh *child_fh,
                       const cfs_cred_t *cred, const char *name);

/* Internal cortxfs structure which holds the information given
 * by upper layer in case of readdir operation
 */
struct cfs_readdir_ctx {
	cfs_readdir_cb_t cb;
	void *ctx;
};

struct stat *cfs_get_stat2(const struct kvnode *node)
{
	uint16_t attr_size;
	struct stat *attr_buff = NULL;

	dassert(node);
	dassert(node->tree);
	dassert(node->basic_attr);

	attr_size = kvnode_get_basic_attr_buff(node, (void **)&attr_buff);

	dassert(attr_buff);
	dassert(attr_size == sizeof(struct stat));

	log_trace("efs_get_stat2: " NODE_ID_F, NODE_ID_P(&node->node_id));
	return attr_buff;
}

int cfs_set_stat(struct kvnode *node)
{
	int rc;

	dassert(node);
	dassert(node->tree);
	dassert(node->basic_attr);

	rc = kvnode_dump(node);

	log_trace("efs_set_stat" NODE_ID_F "rc : %d",
		  NODE_ID_P(&node->node_id), rc);

	return rc;
}

static inline int __cfs_getattr(struct cfs_fs *cfs_fs, const cfs_cred_t *cred,
			 const cfs_ino_t *ino, struct stat *bufstat)
{
	int rc;
	struct stat *stat = NULL;
	struct cfs_fh *fh = NULL;

	dassert(cfs_fs && cred && ino && bufstat);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, ino, &fh);

	stat = cfs_fh_stat(fh);

	memcpy(bufstat, stat, sizeof(struct stat));
out:
	if (fh != NULL) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	log_debug("ino=%d rc=%d", (int)bufstat->st_ino, rc);
	return rc;
}

int cfs_getattr(struct cfs_fs *cfs_fs, const cfs_cred_t *cred,
		const cfs_ino_t *ino, struct stat *bufstat)
{
	size_t rc;

	perfc_trace_inii(PFT_CFS_GETATTR, PEM_CFS_TO_NFS);

	rc = __cfs_getattr(cfs_fs, cred, ino, bufstat);

	perfc_trace_attr(PEA_GETATTR_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

static inline int __cfs_setattr(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
                         cfs_ino_t *ino, struct stat *setstat, int statflag)
{
	struct cfs_fh *fh = NULL;
	struct stat *stat = NULL;
	struct timeval t;
	mode_t ifmt;
	int rc;

	dassert(cfs_fs && cred && ino && setstat);

	if (statflag == 0) {
		rc = 0;
		/* Nothing to do */
		goto out;
	}

	rc = gettimeofday(&t, NULL);
	dassert(rc == 0);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, ino, &fh);

	stat = cfs_fh_stat(fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, stat,
		      CFS_ACCESS_SETATTR);

	/* ctime is to be updated if md are changed */
	stat->st_ctim.tv_sec = t.tv_sec;
	stat->st_ctim.tv_nsec = 1000 * t.tv_usec;

	if (statflag & STAT_MODE_SET) {
		ifmt = stat->st_mode & S_IFMT;
		stat->st_mode = setstat->st_mode | ifmt;
	}

	if (statflag & STAT_UID_SET) {
		stat->st_uid = setstat->st_uid;
	}

	if (statflag & STAT_GID_SET) {
		stat->st_gid = setstat->st_gid;
	}

	if (statflag & STAT_SIZE_SET) {
		stat->st_size = setstat->st_size;
		stat->st_blocks = setstat->st_blocks;
	}

	if (statflag & STAT_SIZE_ATTACH) {
		dassert(0); /* Unsupported */
	}

	if (statflag & STAT_ATIME_SET) {
		stat->st_atim.tv_sec = setstat->st_atim.tv_sec;
		stat->st_atim.tv_nsec = setstat->st_atim.tv_nsec;
	}

	if (statflag & STAT_MTIME_SET) {
		stat->st_mtim.tv_sec = setstat->st_mtim.tv_sec;
		stat->st_mtim.tv_nsec = setstat->st_mtim.tv_nsec;
	}

	if (statflag & STAT_CTIME_SET) {
		stat->st_ctim.tv_sec = setstat->st_ctim.tv_sec;
		stat->st_ctim.tv_nsec = setstat->st_ctim.tv_nsec;
	}

out:
	if (fh != NULL) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	log_debug("rc=%d", rc);
	return rc;
}

int cfs_setattr(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *ino,
                struct stat *setstat, int statflag)
{
    size_t rc;

    perfc_trace_inii(PFT_CFS_SETATTR, PEM_CFS_TO_NFS);

    rc = __cfs_setattr(cfs_fs, cred, ino, setstat, statflag);

    perfc_trace_attr(PEA_SETATTR_RES_RC, rc);
    perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

    return rc;
}
static int __cfs_access(struct cfs_fs *cfs_fs, const cfs_cred_t *cred,
			const cfs_ino_t *ino, int flags)
{
	int rc = 0;
	struct stat stat;

	dassert(cred && ino);

	RC_WRAP_LABEL(rc, out, cfs_getattr, cfs_fs, cred, ino, &stat);
	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, &stat, flags);
out:
	return rc;
}

int cfs_access(struct cfs_fs *cfs_fs, const cfs_cred_t *cred,
	       const cfs_ino_t *ino, int flags)
{
	size_t rc;

	perfc_trace_inii(PFT_CFS_ACCESS, PEM_CFS_TO_NFS);
	perfc_trace_attr(PEA_ACCESS_FLAGS, flags);

	rc = __cfs_access(cfs_fs, cred, ino, flags);

	perfc_trace_attr(PEA_ACCESS_RES_RC, rc);
	perfc_trace_finii(PERFC_TLS_POP_DONT_VERIFY);

	return rc;
}

bool cfs_readdir_cb(void *cb_ctx, const char *name, const struct kvnode *node)
{
	bool retval = false;
	struct cfs_readdir_ctx *cb_info = cb_ctx;
	cfs_ino_t child_inode;

	node_id_to_ino(&node->node_id, &child_inode);
	retval = cb_info->cb(cb_info->ctx, name, child_inode);

	log_trace("efs_readdir_cb:" NODE_ID_F ", retVal = %d",
		  NODE_ID_P(&node->node_id), (int)retval);

	return retval;
}

static inline int __cfs_readdir(struct cfs_fs *cfs_fs,
		const cfs_cred_t *cred, const cfs_ino_t *dir_ino,
		cfs_readdir_cb_t cb, void *cb_ctx)
{
	int rc;
	struct cfs_readdir_ctx cb_info = { .cb = cb, .ctx = cb_ctx};
	struct cfs_fh *fh = NULL;
	struct stat *stat = NULL;
	node_id_t *node_id = NULL;

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, dir_ino, &fh);
	node_id = cfs_node_id_from_fh(fh);
	stat = cfs_fh_stat(fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, stat,
		      CFS_ACCESS_LIST_DIR);

	RC_WRAP_LABEL(rc, out, kvtree_iter_children, cfs_fs->kvtree,
		      node_id, cfs_readdir_cb, &cb_info);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, stat, STAT_ATIME_SET);

out:
	if (fh != NULL ) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	log_debug("cfs_fs=%p dir_ino=%llu rc=%d", cfs_fs, *dir_ino, rc);
	return rc;
}

int cfs_readdir(struct cfs_fs *cfs_fs,
		const cfs_cred_t *cred, const cfs_ino_t *dir_ino,
		cfs_readdir_cb_t cb, void *cb_ctx)
{
	int rc;

	perfc_trace_inii(PFT_CFS_READDIR, PEM_CFS_TO_NFS);
	rc = __cfs_readdir(cfs_fs, cred, dir_ino, cb, cb_ctx);
	perfc_trace_finii(PERFC_TLS_POP_VERIFY);

	return rc;
}

int cfs_readdir_v1(struct cfs_fs *cfs_fs,
		   const cfs_cred_t *cred, const cfs_ino_t *dir_ino,
		   cfs_readdir_cb_t cb, void *cb_ctx)
{
	int rc;
	struct cfs_readdir_ctx cb_info = { .cb = cb, .ctx = cb_ctx};
	struct cfs_fh *fh = NULL;
	struct stat *stat = NULL;
	node_id_t *node_id = NULL;

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, dir_ino, &fh);
	node_id = cfs_node_id_from_fh(fh);
	stat = cfs_fh_stat(fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, stat,
		      CFS_ACCESS_LIST_DIR);

	RC_WRAP_LABEL(rc, out, kvtree_iter_children_v1, cfs_fs->kvtree,
		      node_id, cfs_readdir_cb, &cb_info);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, stat, STAT_ATIME_SET);

out:
	if (fh != NULL ) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	log_debug("cfs_fs=%p dir_ino=%llu rc=%d", cfs_fs, *dir_ino, rc);
	return rc;
}

static inline int __cfs_mkdir(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
			cfs_ino_t *parent, char *name,
			mode_t mode, cfs_ino_t *newdir)
{
	int rc;
	dstore_oid_t oid;
	struct dstore *dstore = dstore_get();
	struct cfs_fh *parent_fh = NULL;
	struct stat *parent_stat = NULL;

	dassert(dstore && cfs_fs && cred && parent && name && newdir);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent, &parent_fh);

	parent_stat = cfs_fh_stat(parent_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_WRITE);

	RC_WRAP_LABEL(rc, out, cfs_create_entry, parent_fh, cred, name, NULL,
		      mode, newdir, CFS_FT_DIR);

	/* Get a new unique oid */
	RC_WRAP_LABEL(rc, out, dstore_get_new_objid, dstore, &oid);

	/* Set the ino-oid mapping for this directory in kvs.*/
	RC_WRAP_LABEL(rc, out, cfs_set_ino_oid, cfs_fs, newdir, &oid);

out:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	log_trace("parent_ino=%llu name=%s newdir_ino=%llu mode=0x%X rc=%d",
		   *parent, name, *newdir, mode, rc);
	return rc;
}

int cfs_mkdir(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *parent,
	      char *name, mode_t mode, cfs_ino_t *newdir)
{
	int rc;

	perfc_trace_inii(PFT_CFS_MKDIR, PEM_CFS_TO_NFS);
	rc = __cfs_mkdir(cfs_fs, cred, parent, name, mode, newdir);
	perfc_trace_finii(PERFC_TLS_POP_VERIFY);

	return rc;
}

int cfs_lookup(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *parent,
	       char *name, cfs_ino_t *ino)

{
	/* Porting notes:
	 * This call is used by CORTXFS in many places so that it cannot be
	 * direcly replaced by cfs_fh_lookup
	 * without modifying them.
	 * However, we should not have multiple versions of the same
	 * file operation. There should be only one implementation
	 * of lookup to make sure that we are testing/debugging
	 * the right thing but not some old/deprecated thing.
	 * So that, this function is modified to use cfs_fh
	 * internally but it preserves the old interface.
	 * Eventually the other code will slowly migrate to cortxfs_fh_lookup
	 * and this code will be dissolved.
	 */
	int rc;
	struct cfs_fh *parent_fh = NULL;
	struct cfs_fh *fh = NULL;

	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent, &parent_fh);
	RC_WRAP_LABEL(rc, out, cfs_fh_lookup, cred, parent_fh, name, &fh);

	*ino = *cfs_fh_ino(fh);

out:
	if (parent_fh) {
		cfs_fh_destroy(parent_fh);
	}

	if (fh) {
		cfs_fh_destroy(fh);
	}
	return rc;
}

int cfs_readlink(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *lnk,
		 char *content, size_t *size)
{
	int rc;
	struct kvstore *kvstor = kvstore_get();
	struct kvnode *node = NULL;
	struct cfs_fh *fh = NULL;
	struct stat *stat = NULL;
	buff_t value;

	dassert(cfs_fs && cred && lnk && size && content);
	dassert(*size != 0);

	buff_init(&value, NULL, 0);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, errfree, cfs_fh_from_ino, cfs_fs, lnk, &fh);
	stat = cfs_fh_stat(fh);
	node = cfs_kvnode_from_fh(fh);

	RC_WRAP_LABEL(rc, errfree, cfs_amend_stat, stat, STAT_ATIME_SET);

	/* Get symlink attributes */
	RC_WRAP_LABEL(rc, errfree, cfs_get_sysattr, node, &value,
		      CFS_SYS_ATTR_SYMLINK);

	dassert(value.len <= PATH_MAX);

	if (value.len > *size) {
		rc = -ENOBUFS;
		goto errfree;
	}

	memcpy(content, value.buf, value.len);
	*size = value.len;

	log_debug("Got link: content='%.*s'", (int) *size, content);

errfree:
	if (fh != NULL) {
		cfs_fh_destroy_and_dump_stat(fh);
	}

	if (value.buf) {
		kvs_free(kvstor, value.buf);
	}

	log_trace("cfs_fs=%p: ino=%llu rc=%d", cfs_fs, *lnk, rc);
	return rc;
}

/** Default mode for a symlink object.
 * Here is a quote from `man 7 symlink`:
 *        On Linux, the permissions of a symbolic link are not used in any
 *        operations; the permissions are always 0777 (read, write, and execute
 *        for all user categories), and can't be changed.
 */
#define CFS_SYMLINK_MODE 0777

int cfs_symlink(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *parent_ino,
                char *name, char *content, cfs_ino_t *newlnk_ino)
{
	int rc;
	struct cfs_fh *parent_fh = NULL;
	struct stat *parent_stat = NULL;

	dassert(cfs_fs && cred && parent_ino && name && newlnk_ino && content);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent_ino, &parent_fh);

	parent_stat = cfs_fh_stat(parent_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_WRITE);

	RC_WRAP_LABEL(rc, out, cfs_create_entry, parent_fh, cred, name, content,
		      CFS_SYMLINK_MODE, newlnk_ino, CFS_FT_SYMLINK);

out:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	log_trace("parent_ino=%llu name=%s newlnk_ino=%llu content=%s rc=%d",
		  *parent_ino, name, *newlnk_ino, content, rc);
	return rc;
}

int cfs_link(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *ino,
	     cfs_ino_t *dino, char *dname)
{
	int rc;
	str256_t k_name;
	struct kvstore *kvstor = kvstore_get();
	struct kvs_idx index;
	struct cfs_fh *parent_fh = NULL;
	struct cfs_fh *child_fh = NULL;
	struct stat *parent_stat = NULL;
	struct  stat *child_stat = NULL;
	node_id_t *dnode_id = NULL;
	node_id_t new_node_id;

	dassert(cred && ino && dname && dino && kvstor);

	index = cfs_fs->kvtree->index;

	RC_WRAP(kvs_begin_transaction, kvstor, &index);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, aborted, cfs_fh_from_ino, cfs_fs, dino, &parent_fh);
	dnode_id = cfs_node_id_from_fh(parent_fh);
	parent_stat = cfs_fh_stat(parent_fh);

	RC_WRAP_LABEL(rc, aborted, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_WRITE);

	rc = cfs_fh_lookup(cred, parent_fh, dname, &child_fh);
	if (rc == 0)
	{
		rc = -EEXIST;
		goto aborted;
	}

	str256_from_cstr(k_name, dname, strlen(dname));

	ino_to_node_id(ino, &new_node_id);

	RC_WRAP_LABEL(rc, aborted, kvtree_attach, cfs_fs->kvtree, dnode_id,
		      &new_node_id, &k_name);

	RC_WRAP_LABEL(rc, aborted, cfs_fh_from_ino, cfs_fs, ino, &child_fh);
	child_stat = cfs_fh_stat(child_fh);

	RC_WRAP_LABEL(rc, aborted, cfs_amend_stat, child_stat,
		      STAT_CTIME_SET|STAT_INCR_LINK);

	RC_WRAP_LABEL(rc, aborted, cfs_amend_stat, parent_stat,
		      STAT_MTIME_SET|STAT_CTIME_SET);

	RC_WRAP(kvs_end_transaction, kvstor, &index);

aborted:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	if (child_fh != NULL ) {
		cfs_fh_destroy_and_dump_stat(child_fh);
	}

	if (rc != 0) {
		kvs_discard_transaction(kvstor, &index);
	}

	log_trace("cfs_fs=%p rc=%d ino=%llu dino=%llu dname=%s", cfs_fs, rc,
		  *ino, *dino, dname);

	return rc;
}

static inline bool cfs_file_has_links(struct stat *stat)
{
	return stat->st_nlink > 0;
}

static int cfs_destroy_orphaned_file2(struct cfs_fh *fh)
{
	int rc;
	dstore_oid_t oid;
	cfs_ino_t *ino = NULL;
	struct cfs_fs *cfs_fs = NULL;
	struct stat *stat = NULL;
	struct kvnode *node = NULL;
	struct kvstore *kvstor = kvstore_get();
	struct dstore *dstore = dstore_get();
	struct kvs_idx index;

	dassert(kvstor && dstore && fh);

	cfs_fs = cfs_fs_from_fh(fh);
	ino = cfs_fh_ino(fh);
	stat = cfs_fh_stat(fh);

	index = cfs_fs->kvtree->index;

	if (cfs_file_has_links(stat)) {
		rc = 0;
		goto out;
	}

	kvs_begin_transaction(kvstor, &index);

	node = cfs_kvnode_from_fh(fh);
	RC_WRAP_LABEL(rc, out, cfs_del_stat, node);

	if (S_ISLNK(stat->st_mode)) {
		/* Delete symlink */
		RC_WRAP_LABEL(rc, out, cfs_del_sysattr, node,
			      CFS_SYS_ATTR_SYMLINK);
	} else if (S_ISREG(stat->st_mode)) {
		RC_WRAP_LABEL(rc, out, cfs_ino_to_oid, cfs_fs, ino, &oid);
		RC_WRAP_LABEL(rc, out, dstore_obj_delete,
			      dstore, cfs_fs, &oid);
		RC_WRAP_LABEL(rc, out, cfs_del_oid, cfs_fs, ino);
	} else {
		/* Impossible: rmdir handles DIR; LNK and REG are handled by
		 * this function, the other types cannot be created
		 * at all.
		 */
		dassert(0);
		log_err("Attempt to remove unsupported object type (%d)",
			(int) stat->st_mode);
	}
	/* TODO: Delete File Xattrs here */
	kvs_end_transaction(kvstor, &index);

out:
	if (rc != 0) {
		kvs_discard_transaction(kvstor, &index);
	}

	log_trace("inode=%llu rc=%d", *ino, rc);
	return rc;
}

int cfs_destroy_orphaned_file(struct cfs_fs *cfs_fs,
                              const cfs_ino_t *ino)
{
	int rc;
	struct cfs_fh *fh = NULL;

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, ino, &fh);
	RC_WRAP_LABEL(rc, out, cfs_destroy_orphaned_file2, fh);

out:
	if (fh != NULL) {
		cfs_fh_destroy(fh);
	}
	log_trace("inode=%llu rc=%d", *ino, rc);
	return rc;
}

/* TODO: Seems to be a huge fuction there needs to be a further investigation
 * to find out possibility of optimization and breaking down this APIs to
 * smaller APIs to improve readability
 */
int cfs_rename(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
	       cfs_ino_t *sino_dir, char *sname, const cfs_ino_t *psrc,
	       cfs_ino_t *dino_dir, char *dname, const cfs_ino_t *pdst,
	       const struct cfs_rename_flags *pflags)
{
	int rc;
	bool overwrite_dst = false;
	bool rename_inplace = false;
	bool is_dst_non_empty_dir = false;
	str256_t k_sname;
	str256_t k_dname;
	mode_t s_mode = 0;
	mode_t d_mode = 0;
	cfs_ino_t *sdir_ino = NULL;
	cfs_ino_t *ddir_ino = NULL;
	cfs_ino_t *dst_ino = NULL;
	node_id_t *sdir_id = NULL;
	node_id_t *ddir_id = NULL;
	node_id_t *src_id = NULL;
	node_id_t *dst_id = NULL;
	struct cfs_fh *sdir_fh = NULL;
	struct cfs_fh *ddir_fh = NULL;
	struct cfs_fh *src_fh = NULL;
	struct cfs_fh *dst_fh = NULL;
	struct stat *sdir_stat = NULL;
	struct stat *ddir_stat = NULL;
	struct stat *src_stat = NULL;
	struct stat *dst_stat = NULL;
	struct kvstore *kvstor = kvstore_get();
	const struct cfs_rename_flags flags = pflags ? *pflags :
		(const struct cfs_rename_flags) CFS_RENAME_FLAGS_INIT;

	dassert(kvstor);
	dassert(cred);
	dassert(sino_dir && dino_dir);
	dassert(sname && dname);
	dassert(strlen(sname) <= NAME_MAX);
	dassert(strlen(dname) <= NAME_MAX);
	dassert((*sino_dir != *dino_dir || strcmp(sname, dname) != 0));
	dassert(cfs_fs);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, sino_dir, &sdir_fh);
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, dino_dir, &ddir_fh);

	sdir_ino = cfs_fh_ino(sdir_fh);
	sdir_stat = cfs_fh_stat(sdir_fh);
	sdir_id = cfs_node_id_from_fh(sdir_fh);
	ddir_ino = cfs_fh_ino(ddir_fh);
	ddir_stat = cfs_fh_stat(ddir_fh);
	ddir_id = cfs_node_id_from_fh(ddir_fh);

	rename_inplace = (*sdir_ino == *ddir_ino);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, sdir_stat,
		      CFS_ACCESS_DELETE_ENTITY);

	if (!rename_inplace) {
		RC_WRAP_LABEL(rc, out, cfs_access_check, cred, ddir_stat,
			      CFS_ACCESS_CREATE_ENTITY);
	}

	RC_WRAP_LABEL(rc, out, cfs_fh_lookup, cred, sdir_fh, sname, &src_fh);
	src_stat = cfs_fh_stat(src_fh);
	src_id = cfs_node_id_from_fh(src_fh);

	/* Destination file/dir may or may not be present
	 * based on lookup result we have to take descision
	 */
	rc = cfs_fh_lookup(cred, ddir_fh, dname, &dst_fh);
	if (rc < 0 && rc != -ENOENT) {
		goto out;
	}
	overwrite_dst = (rc != -ENOENT);

	if (overwrite_dst) {
		dst_ino = cfs_fh_ino(dst_fh);
		dst_stat = cfs_fh_stat(dst_fh);

		/* Fetch 'st_mode' for source and destination. */
		s_mode = src_stat->st_mode;
		d_mode = dst_stat->st_mode;

		if (S_ISDIR(s_mode) != S_ISDIR(d_mode)) {
			log_warn("Incompatible source and destination %d,%d.",
				 (int) s_mode, (int) d_mode);
			rc = -ENOTDIR;
			goto out;
		}

		if (S_ISDIR(d_mode)) {
			dst_id = cfs_node_id_from_fh(dst_fh);
			RC_WRAP_LABEL(rc, out, kvtree_has_children,
				      cfs_fs->kvtree, dst_id,
				      &is_dst_non_empty_dir);
		}

		if (is_dst_non_empty_dir) {
			log_warn("Destination is not empty (%llu:%s)",
				 *dst_ino, dname);
			rc = -EEXIST;
			goto out;
		}

		if (S_ISDIR(d_mode)) {
			/* FIXME: rmdir() does not have an option to destroy
			 * a dir object (unlinked from the tree), therefore
			 * we might lose some data here if the following
			 * operations (relinking) fail.
			 */
			RC_WRAP_LABEL(rc, out, cfs_rmdir, cfs_fs, cred,
				      dino_dir, dname);
		} else {
			/* Make an ophaned file: it will be destoyed either
			 * at the end of the function or when the file
			 * will be closed.
			 */
			log_trace("Detaching a file from the tree "
				  "(%llu, %llu, %s)", *dino_dir, *dst_ino,
				  dname);
			RC_WRAP_LABEL(rc, out, cfs_detach2, ddir_fh, dst_fh,
				      cred, dname);
		}
	} /* if (overwrite_dst) */

	str256_from_cstr(k_sname, sname, strlen(sname));
	str256_from_cstr(k_dname, dname, strlen(dname));

	if (rename_inplace) {
		/* a shortcut for renaming only a dentry
		 * without re-linking of the inodes.
		 */
		RC_WRAP_LABEL(rc, out, cfs_tree_rename_link, sdir_fh, src_fh,
			      &k_sname, &k_dname);
	} else {
		s_mode = src_stat->st_mode;

		RC_WRAP_LABEL(rc, out, kvtree_detach, cfs_fs->kvtree, sdir_id,
			      &k_sname);

		RC_WRAP_LABEL(rc, out, kvtree_attach, cfs_fs->kvtree, ddir_id,
			      src_id, &k_dname);

		if (S_ISDIR(s_mode)) {
			RC_WRAP_LABEL(rc, out, cfs_amend_stat, sdir_stat,
				      STAT_DECR_LINK);
			RC_WRAP_LABEL(rc, out, cfs_amend_stat, ddir_stat,
				      STAT_INCR_LINK);
		}
	}

	if (overwrite_dst && !S_ISDIR(d_mode) && !flags.is_dst_open) {
		/* Remove the actual 'destination' object only if all
		 * previous operations have completed successfully.
		 */
		log_trace("Removing detached file (%llu)", *dst_ino);
		RC_WRAP_LABEL(rc, out, cfs_destroy_orphaned_file2, dst_fh);
	}

out:
	if (sdir_fh) {
		cfs_fh_destroy_and_dump_stat(sdir_fh);
	}

	if (ddir_fh) {
		cfs_fh_destroy_and_dump_stat(ddir_fh);
	}

	if (src_fh) {
		cfs_fh_destroy_and_dump_stat(src_fh);
	}

	if (dst_fh) {
		cfs_fh_destroy(dst_fh);
	}

	log_debug("cfs_fs=%p sdir_ino=%llu ddir_ino=%llu src_ino=%llu"
		  "sname=%s dst_ino=%llu dname=%s rc=%d",cfs_fs, *sino_dir,
		  *dino_dir, psrc?*psrc:0LL, sname, pdst?*pdst:0LL, dname, rc);

	return rc;
}

static inline int __cfs_rmdir(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
                              cfs_ino_t *parent_ino, char *name)
{
	int rc;
	bool is_non_empty_dir;
	str256_t kname;
	struct kvstore *kvstor = kvstore_get();
	struct kvs_idx index;
	struct cfs_fh *parent_fh = NULL;
	struct cfs_fh *child_fh = NULL;
	struct stat *parent_stat = NULL;
	struct kvnode *child_node = NULL;
	struct kvnode *parent_node = NULL;
	cfs_ino_t *child_ino = NULL;
	node_id_t *cnode_id = NULL;
	node_id_t *pnode_id = NULL;

	dassert(cfs_fs && cred && parent_ino && name && kvstor);
	dassert(strlen(name) <= NAME_MAX);

	index = cfs_fs->kvtree->index;

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent_ino, &parent_fh);

	parent_stat = cfs_fh_stat(parent_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_WRITE);

	RC_WRAP_LABEL(rc, out, cfs_fh_lookup, cred, parent_fh, name, &child_fh);

	child_ino = cfs_fh_ino(child_fh);
	cnode_id = cfs_node_id_from_fh(child_fh);

	/* Check if directory empty */
	RC_WRAP_LABEL(rc, out, kvtree_has_children, cfs_fs->kvtree, cnode_id,
		      &is_non_empty_dir);

	if (is_non_empty_dir) {
		 rc = -ENOTEMPTY;
		 log_debug("cfs_fs=%p parent_ino=%llu child_ino=%llu name=%s"
			   " not empty", cfs_fs, *parent_ino, *child_ino, name);
		 goto out;
	}

	RC_WRAP_LABEL(rc, out, kvs_begin_transaction, kvstor, &index);

	str256_from_cstr(kname, name, strlen(name));

	/* Detach the inode */
	pnode_id = cfs_node_id_from_fh(parent_fh);

	RC_WRAP_LABEL(rc, aborted, kvtree_detach, cfs_fs->kvtree, pnode_id,
		      &kname);

	/* Remove its stat */
	child_node = cfs_kvnode_from_fh(child_fh);
	RC_WRAP_LABEL(rc, aborted, cfs_del_stat, child_node);

	/* Child dir has a "hardlink" to the parent ("..") */
	parent_node = cfs_kvnode_from_fh(parent_fh);
	RC_WRAP_LABEL(rc, aborted, cfs_update_stat, parent_node,
		      STAT_DECR_LINK|STAT_MTIME_SET|STAT_CTIME_SET);

	RC_WRAP_LABEL(rc, aborted, cfs_del_oid, cfs_fs, child_ino);

	/* TODO: Remove all xattrs when cortxfs_remove_all_xattr is implemented
	 */
	RC_WRAP_LABEL(rc, out, kvs_end_transaction, kvstor, &index);

aborted:
	if (rc < 0) {
		/* FIXME: error code is overwritten */
		RC_WRAP_LABEL(rc, out, kvs_discard_transaction, kvstor, &index);
	}

out:

	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	if (child_fh != NULL) {
		cfs_fh_destroy(child_fh);
	}

	log_debug("cfs_fs=%p parent_ino=%llu child_ino=%llu name=%s rc=%d",
		  cfs_fs, *parent_ino,  child_ino ? *child_ino : 0LL, name, rc);
	return rc;
}

int cfs_rmdir(struct cfs_fs *cfs_fs, cfs_cred_t *cred,
		cfs_ino_t *parent, char *name)
{
	int rc;

	perfc_trace_inii(PFT_CFS_RMDIR, PEM_CFS_TO_NFS);
	rc = __cfs_rmdir(cfs_fs, cred, parent, name);
	perfc_trace_finii(PERFC_TLS_POP_VERIFY);

	return rc;
}

int cfs_unlink2(struct cfs_fh *parent_fh, struct cfs_fh *child_fh,
                cfs_cred_t *cred, char *name)
{
	int rc;

	dassert(parent_fh && child_fh && cred && name);
	dassert(cfs_fh_invariant(parent_fh));
	dassert(cfs_fh_invariant(child_fh));

	RC_WRAP_LABEL(rc, out, cfs_detach2, parent_fh, child_fh, cred, name);
	RC_WRAP_LABEL(rc, out, cfs_destroy_orphaned_file2, child_fh);

out:
	log_debug("cfs_fs=%p parent_ino=%llu child_ino=%llu name=%s rc=%d",
		  cfs_fs_from_fh(parent_fh), *cfs_fh_ino(parent_fh),
		  *cfs_fh_ino(child_fh), name, rc);
	return rc;
}


int cfs_unlink(struct cfs_fs *cfs_fs, cfs_cred_t *cred, cfs_ino_t *dir,
	       cfs_ino_t *fino, char *name)
{
	int rc;
	cfs_ino_t child_ino = 0LL;
	struct stat *child_stat = NULL;
	struct cfs_fh *child_fh = NULL;
	struct cfs_fh *parent_fh = NULL;

	dassert(cfs_fs && cred && dir && name);

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, dir,
		      &parent_fh);

	if (likely(fino != NULL)) {
		RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, fino,
			      &child_fh);
		child_ino = *fino;
	} else {
		RC_WRAP_LABEL(rc, out, cfs_fh_lookup, cred, parent_fh, name,
			      &child_fh);
		child_ino = *cfs_fh_ino(child_fh);
	}

	child_stat = cfs_fh_stat(child_fh);
	RC_WRAP_LABEL(rc, out, cfs_unlink2, parent_fh, child_fh, cred, name);

out:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	if (child_fh != NULL) {
		dassert(child_stat);

		if (cfs_file_has_links(child_stat)) {
			cfs_fh_destroy_and_dump_stat(child_fh);
		} else {
			cfs_fh_destroy(child_fh);
		}
	}

	log_debug("cfs_fs=%p parent_ino=%llu child_ino=%llu name=%s rc=%d",
		  cfs_fs, *dir, child_ino, name, rc);
	return rc;
}

static int cfs_detach2(struct cfs_fh *parent_fh, struct cfs_fh *child_fh,
                       const cfs_cred_t *cred, const char *name)
{
	int rc;
	str256_t k_name;
	struct kvs_idx index;
	struct kvstore *kvstor = kvstore_get();
	struct cfs_fs *cfs_fs = NULL;
	struct stat *parent_stat = NULL;
	struct stat *child_stat = NULL;
	node_id_t *pnode_id = NULL;

	dassert(kvstor && parent_fh && cred && child_fh && name);

	cfs_fs = cfs_fs_from_fh(parent_fh);
	index = cfs_fs->kvtree->index;
	kvs_begin_transaction(kvstor, &index);

	parent_stat = cfs_fh_stat(parent_fh);
	child_stat = cfs_fh_stat(child_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check, cred, parent_stat,
		      CFS_ACCESS_DELETE_ENTITY);

	pnode_id = cfs_node_id_from_fh(parent_fh);
	str256_from_cstr(k_name, name, strlen(name));
	RC_WRAP_LABEL(rc, out, kvtree_detach, cfs_fs->kvtree, pnode_id,
		      &k_name);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, child_stat,
		      STAT_CTIME_SET|STAT_DECR_LINK);

	RC_WRAP_LABEL(rc, out, cfs_amend_stat, parent_stat,
		      STAT_CTIME_SET|STAT_MTIME_SET);

	kvs_end_transaction(kvstor, &index);

out:
	if (rc != 0) {
		kvs_discard_transaction(kvstor, &index);
	}

	log_trace("cfs_fs=%p parent_ino=%llu name=%s child_ino=%llu rc=%d",
		  cfs_fs, (unsigned long long int)parent_stat->st_ino, name,
		  (unsigned long long int)child_stat->st_ino, rc);

	return rc;
}

int cfs_detach(struct cfs_fs *cfs_fs, const cfs_cred_t *cred,
               const cfs_ino_t *parent_ino, const cfs_ino_t *child_ino,
               const char *name)
{
	int rc;
	struct cfs_fh *parent_fh = NULL;
	struct cfs_fh *child_fh = NULL;

	/* TODO:Temp_FH_op - to be removed
	 * Should get rid of creating and destroying FH operation in this
	 * API when caller pass the valid FH instead of inode number
	 */
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, parent_ino, &parent_fh);
	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, cfs_fs, child_ino, &child_fh);
	RC_WRAP_LABEL(rc, out, cfs_detach2, parent_fh, child_fh, cred, name);

out:
	if (parent_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(parent_fh);
	}

	if (child_fh != NULL) {
		cfs_fh_destroy_and_dump_stat(child_fh);
	}

	log_trace("cfs_fs=%p parent_ino=%llu name=%s child_ino=%llu rc=%d",
		  cfs_fs, *parent_ino, name, *child_ino, rc);
	return rc;
}
