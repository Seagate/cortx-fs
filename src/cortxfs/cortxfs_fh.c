/*
 * Filename: cortxfs_fh.c
 * Description: CORTXFS File Handle API implementation.
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

#include <kvstore.h>
#include "cortxfs.h"
#include "cortxfs_internal.h"
#include "cortxfs_fh.h"
#include <debug.h>
#include <string.h> /* strcmp() */
#include <str.h> /* str256_t */
#include <errno.h> /* EINVAL */
#include <common/helpers.h> /* RC_WRAP_LABEL */
#include <common.h> /* unlikely */
#include <common/log.h> /* log_err() */
#include "kvtree.h" /* kvtree_lookup() */
#include "operation.h" /* perf tracepoints */

/**
 * A unique key to be used in containers (maps, sets).
 * TODO: It will be replaced by fid of the file
 * or a combination of FSFid+FileFid.
 * NOTE: This is not to be stored in kvs(kvstore).
 * This is only in memory.
 * @todo: find a better name than cfs_fh_key,
 * since *_key names are used by keys stored in kvs.
 */
struct cfs_fh_key {
	struct cfs_fs *fs;
	uint64_t file;
};

struct cfs_fh {
	/* In memory representation of a NSAL kvnode which is linked to a
	 * kvtree. This contains the basic attributes (stats) of a file
	 */
	struct kvnode f_node;

	/* A file system context on which file/directory/symlink is created */
	struct cfs_fs *fs;

	/* TODO: Add FID as an unique key, which will be used in containers
	 * maps, sets
	 */
	struct cfs_fh_key key;

	/* @TODO: Following things can be implemented
	 * 1. Add synchronization primitives to resolve the concurrency
	 *    issue
	 * 2. Add ref count to know about file handle in use by multiple
	 *    front ends (eg: NFS, CIFS etc) which help to take decision whether
	 *    to release FH or not
	 * 3. Cache system attributes associated with a file
	 * 4. Add a file state to make certain decision when multiple users
	 *    are using it, like delete on close
	 */
};

/** Initialize an empty invalid FH instance */
#define CFS_FH_INIT (struct cfs_fh) { .f_node = KVNODE_INIT_EMTPY }

static inline
bool cfs_fh_invariant(const struct cfs_fh *fh)
{
	bool rc = false;

	/* A FH should have
	 *	Filesystem pointer set.
	 *	File handle kvnode should be initialized properly
	 *	File handle should have valid inode
	 */
	if (fh->fs != NULL && kvnode_invariant(&fh->f_node)) {
		struct stat *stat = cfs_fh_stat(fh);
		cfs_ino_t ino = stat->st_ino;
		rc = (ino >= CFS_ROOT_INODE);
	}

	return rc;
}

struct cfs_fh_serialized {
	uint64_t fsid;
	cfs_ino_t ino_num;
};

struct cfs_fs *cfs_fs_from_fh(const struct cfs_fh *fh)
{
	dassert(fh);
	dassert(cfs_fh_invariant(fh));
	return fh->fs;
}

struct stat *cfs_fh_stat(const struct cfs_fh *fh)
{
	dassert(fh);
	return cfs_get_stat2(&fh->f_node);
}

struct kvnode *cfs_kvnode_from_fh(struct cfs_fh *fh)
{
	dassert(fh);
	dassert(cfs_fh_invariant(fh));
	return &fh->f_node;
}

static inline
void cfs_fh_init_key(struct cfs_fh *fh)
{
	struct stat *stat = cfs_fh_stat(fh);
	fh->key.file = stat->st_ino;
	fh->key.fs = fh->fs;
}

node_id_t *cfs_node_id_from_fh(struct cfs_fh *fh)
{
	dassert(fh);
	dassert(cfs_fh_invariant(fh));
	return &fh->f_node.node_id;
}

cfs_ino_t *cfs_fh_ino(struct cfs_fh *fh)
{
	struct stat *stat = cfs_fh_stat(fh);
	return (cfs_ino_t *)&stat->st_ino;
}

int cfs_fh_from_ino(struct cfs_fs *fs, const cfs_ino_t *ino_num,
                    struct cfs_fh **fh)
{
	int rc;
	struct cfs_fh *newfh = NULL;
	struct kvstore *kvstor =  kvstore_get();
	struct kvnode node = KVNODE_INIT_EMTPY;

	dassert(kvstor && fs && ino_num && fh);

	/* A caller for this API who uses/caches this FH, will be responsible
	 * for freeing up this FH, caller should be calling cfs_fh_destroy to
	 * release this FH
	 */
	RC_WRAP_LABEL(rc, out, kvs_alloc, kvstor, (void **) &newfh,
		      sizeof(struct cfs_fh));

	*newfh = CFS_FH_INIT;

	RC_WRAP_LABEL(rc, out, cfs_kvnode_load, &node, fs->kvtree,
		      ino_num);

	newfh->f_node = node;
	newfh->fs = fs;
	cfs_fh_init_key(newfh);
	dassert(cfs_fh_invariant(newfh));
	*fh = newfh;
	newfh = NULL;
out:
	if (unlikely(newfh)) {
		cfs_fh_destroy(newfh);
	}
	return rc;
}

static inline int __cfs_fh_lookup(const cfs_cred_t *cred,
				  struct cfs_fh *parent_fh,
				  const char *name, struct cfs_fh **fh)
{
	int rc;
	str256_t kname;
	struct cfs_fh *newfh = NULL;
	struct stat *parent_stat = NULL;
	struct kvstore *kvstor = kvstore_get();
	struct kvnode node = KVNODE_INIT_EMTPY;
	cfs_ino_t ino;
	node_id_t pid, id;

	dassert(cred && parent_fh && name && fh && kvstor);
	dassert(cfs_fh_invariant(parent_fh));

	parent_stat = cfs_fh_stat(parent_fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check,
		      (cfs_cred_t *) cred, parent_stat, CFS_ACCESS_READ);

	if ((parent_stat->st_ino == CFS_ROOT_INODE) &&
	    (strcmp(name, "..") == 0)) {
		ino = CFS_ROOT_INODE;
	} else {
		str256_from_cstr(kname, name, strlen(name));
		pid = parent_fh->f_node.node_id;

		RC_WRAP_LABEL(rc, out, kvtree_lookup, parent_fh->fs->kvtree,
			      &pid, &kname, &id);

		node_id_to_ino(&id, &ino);
	}

	dassert(ino >= CFS_ROOT_INODE);

	RC_WRAP_LABEL(rc, out, cfs_kvnode_load, &node,
		      parent_fh->fs->kvtree, &ino);

	RC_WRAP_LABEL(rc, out, kvs_alloc, kvstor, (void **) &newfh,
		      sizeof(struct cfs_fh));

	newfh->fs = parent_fh->fs;
	newfh->f_node = node;
	cfs_fh_init_key(newfh);
	dassert(cfs_fh_invariant(newfh));
	*fh = newfh;
	newfh = NULL;

	/* FIXME: Shouldn't we update parent.atime here? */
out:
	if (newfh) {
		cfs_fh_destroy(newfh);
	}
	return rc;
}

int cfs_fh_lookup(const cfs_cred_t *cred, struct cfs_fh *parent_fh,
                const char *name, struct cfs_fh **fh)
{
	int rc;

	perfc_trace_inii(PFT_CFS_LOOKUP, PEM_CFS_TO_NFS);
	rc = __cfs_fh_lookup(cred, parent_fh, name, fh);
	perfc_trace_finii(PERFC_TLS_POP_VERIFY);

	return rc;
}

void cfs_fh_destroy(struct cfs_fh *fh)
{
	struct kvstore *kvstor = kvstore_get();

	dassert(kvstor && fh);
	dassert(cfs_fh_invariant(fh));

	/* Note: As of now, destroying FH does not update the stats in backend
	 * because those are stale, the reason for that is FH is not supplied
	 * as input param to each of the cortxfs API which can modify the stats
	 * of file directly in FH.
	 * TODO: Temp_FH_op - to be removed
	 * Uncomment the logic to dump the stats associated with FH once FH is
	 * present everywhere all the update happens to FH
	 */
	/* cfs_set_stat(&fh->f_node); */
	kvnode_fini(&fh->f_node);
	kvs_free(kvstor, fh);
}

void cfs_fh_destroy_and_dump_stat(struct cfs_fh *fh)
{
	struct kvstore *kvstor = kvstore_get();

	dassert(kvstor && fh);
	dassert(cfs_fh_invariant(fh));

	cfs_set_stat(&fh->f_node);
	kvnode_fini(&fh->f_node);
	kvs_free(kvstor, fh);
}

int cfs_fh_getroot(struct cfs_fs *fs, const cfs_cred_t *cred,
                   struct cfs_fh **pfh)
{
	int rc;
	struct cfs_fh *fh = NULL;
	struct stat *stat = NULL;
	cfs_ino_t root_ino = CFS_ROOT_INODE;

	dassert(fs && cred && pfh);

	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, fs, &root_ino, &fh);

	stat = cfs_fh_stat(fh);

	RC_WRAP_LABEL(rc, out, cfs_access_check,
		      (cfs_cred_t *) cred, stat, CFS_ACCESS_READ);

	*pfh = fh;
	fh = NULL;

out:
	if (unlikely(fh)) {
		cfs_fh_destroy(fh);
	}
	return rc;
}

int cfs_fh_serialize(const struct cfs_fh *fh, void* buffer, size_t max_size)
{
	int rc = 0;
	struct stat *stat = NULL;
	struct cfs_fh_serialized data = { .ino_num = 0 };

	dassert(fh && buffer);
	dassert(cfs_fh_invariant(fh));

	if (max_size < sizeof(struct cfs_fh_serialized)) {
		rc = -ENOBUFS;
		goto out;
	}

	stat = cfs_fh_stat(fh);
	data.ino_num = (cfs_ino_t)stat->st_ino;
	/* fsid is ignored */

	memcpy(buffer, &data, sizeof(data));
	rc = sizeof(data);

out:
	return rc;
}

int cfs_fh_deserialize(struct cfs_fs *fs,
		       const cfs_cred_t *cred,
		       const void* buffer, size_t buffer_size,
		       struct cfs_fh** pfh)
{
	int rc = 0;
	struct cfs_fh_serialized data = { .ino_num = 0 };

	dassert(fs && cred && buffer && pfh);

	/* FIXME: We need to check if this function is a subject
	 * to access checks.
	 */
	(void) cred;

	if (buffer_size != sizeof(struct cfs_fh_serialized)) {
		rc = -EINVAL;
		goto out;
	}

	memcpy(&data, buffer, sizeof(data));

	/* data.fsid is ignored */

	RC_WRAP_LABEL(rc, out, cfs_fh_from_ino, fs, &data.ino_num, pfh);

out:
	return rc;
}

size_t cfs_fh_serialized_size(void)
{
	return sizeof(struct cfs_fh_serialized);
}

int cfs_fh_ser_with_fsid(const struct cfs_fh *fh, uint64_t fsid, void *buffer,
			 size_t max_size)
{
	int rc = 0;
	struct stat *stat = NULL;
	struct cfs_fh_serialized data = { .ino_num = 0 };

	dassert(fh && buffer);
	dassert(cfs_fh_invariant(fh));

	if (max_size < sizeof(struct cfs_fh_serialized)) {
		rc = -ENOBUFS;
		goto out;
	}

	stat = cfs_fh_stat(fh);
	data.ino_num = stat->st_ino;;
	data.fsid = fsid;

	memcpy(buffer, &data, sizeof(data));
	rc = sizeof(data);

out:
	return rc;
}

void cfs_fh_key(const struct cfs_fh *fh, void **pbuffer, size_t *psize)
{
	dassert(fh && pbuffer && psize);
	*pbuffer = (void *) &fh->key;
	*psize = sizeof(fh->key);
}

