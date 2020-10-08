/*
 * Filename: fs.c
 * Description: CORTXFS Filesystem functions API.
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

#include <errno.h> /* errono */
#include <string.h> /* memcpy */
#include "common.h" /* container_of */
#include "internal/fs.h" /* fs interface */
#include "namespace.h" /* namespace methods */
#include "tenant.h" /* tenant method */
#include "common/helpers.h" /* RC_WRAP_LABEL */
#include "common/log.h" /* logging */
#include "kvtree.h"
#include "kvnode.h"

/* data types */
/* fs node : in memory data structure. */
struct cfs_fs_node {
	struct cfs_fs cfs_fs; /* fs object */
	LIST_ENTRY(cfs_fs_node) link;
};

LIST_HEAD(list, cfs_fs_node) fs_list = LIST_HEAD_INITIALIZER();

/* global endpoint operations for cortxfs*/
static const struct cfs_endpoint_ops *g_e_ops;

static int fs_node_init(struct cfs_fs_node *fs_node,
			struct namespace *ns, size_t ns_size)
{
	int rc = 0;

	str256_t *fs_name = NULL;
	ns_get_name(ns, &fs_name);

	fs_node->cfs_fs.ns = calloc(1, ns_size);
	if (fs_node->cfs_fs.ns == NULL) {
		goto out;
	}

	memcpy(fs_node->cfs_fs.ns, ns, ns_size);

	fs_node->cfs_fs.kvtree = calloc(1, sizeof(struct kvtree));
	if (!fs_node->cfs_fs.kvtree) {
		rc = -ENOMEM;
		goto kvtree_alloc_fail;
	}

	rc = kvtree_init(fs_node->cfs_fs.ns, fs_node->cfs_fs.kvtree);
	if (rc != 0) {
		log_err("failed to load FS: " STR256_F
			" , kvtree_init() failed!",
			STR256_P(fs_name));
		goto kvtree_init_fail;
	}

	fs_node->cfs_fs.root_node = calloc(1, sizeof(struct kvnode));
	if (!fs_node->cfs_fs.root_node) {
		rc = -ENOMEM;
		goto kvnode_alloc_fail;
	}

	*(fs_node->cfs_fs.root_node) = KVNODE_INIT_EMTPY;

	rc = kvnode_load(fs_node->cfs_fs.kvtree,
			 &(fs_node->cfs_fs.kvtree->root_node_id),
			 fs_node->cfs_fs.root_node);
	if (rc != 0) {
		log_err("failed to load FS: " STR256_F
			" , kvnode_load() failed!",
			STR256_P(fs_name));
		goto kvnode_load_fail;
	}

	goto out;

kvnode_load_fail:
	free(fs_node->cfs_fs.root_node);
kvnode_alloc_fail:
	kvtree_fini(fs_node->cfs_fs.kvtree);
kvtree_init_fail:
	free(fs_node->cfs_fs.kvtree);
kvtree_alloc_fail:
	free(fs_node->cfs_fs.ns);
out:
	if (rc != -ENOMEM) {
		log_info("fs node initialization result for fs_name=" STR256_F 
			 " rc=%d", STR256_P(fs_name), rc);
	}
	return rc;
}

void fs_node_deinit(struct cfs_fs_node *fs_node)
{
	kvnode_fini(fs_node->cfs_fs.root_node);
	kvtree_fini(fs_node->cfs_fs.kvtree);
	free(fs_node->cfs_fs.ns);
	free(fs_node->cfs_fs.root_node);
	free(fs_node->cfs_fs.kvtree);
}

static int cfs_fs_is_empty(const struct cfs_fs *fs)
{
	//@todo
	//return -ENOTEMPTY;
	return 0;
}

void cfs_fs_get_id(struct cfs_fs *fs, uint16_t *fs_id)
{
	dassert(fs);
	ns_get_id(fs->ns, fs_id);
}

int cfs_fs_lookup(const str256_t *name, struct cfs_fs **fs)
{
	int rc = -ENOENT;
	struct cfs_fs_node *fs_node;
	str256_t *fs_name = NULL;

	if (fs != NULL) {
		*fs = NULL;
	}

	LIST_FOREACH(fs_node, &fs_list, link) {
		ns_get_name(fs_node->cfs_fs.ns, &fs_name);
		if (str256_cmp(name, fs_name) == 0) {
			rc = 0;
			if (fs != NULL) {
				*fs = &fs_node->cfs_fs;
			}
			goto out;
		}
	}

out:
	if (rc == 0) {
		/**
		 * Any incore struct cfs_fs entry found in fs_list must have
		 * kvtree for the root node attached.
		 * If not, then it is a bug!
		 **/
		dassert(fs_node->cfs_fs.kvtree != NULL);
	}

	log_debug(STR256_F " rc=%d", STR256_P(name), rc);
	return rc;
}

void fs_ns_scan_cb(struct namespace *ns, size_t ns_size)
{
	str256_t *fs_name = NULL;
	struct cfs_fs_node *fs_node = NULL;
	int rc = 0;

	/**
	 * Before this fs can be added to the incore list fs_list, we must
	 * initialize the incore structure to be usable by others.
	 */
	ns_get_name(ns, &fs_name);
	log_info("trying to load FS: " STR256_F,
		 STR256_P(fs_name));

	fs_node = calloc(1, sizeof(struct cfs_fs_node));
	if (fs_node == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	rc = fs_node_init(fs_node, ns, ns_size);
	if (rc != 0) {
		goto fs_init_fail;
	}

	LIST_INSERT_HEAD(&fs_list, fs_node, link);
	log_info("FS:" STR256_F " loaded from disk, ptr:%p",
		 STR256_P(fs_name), &fs_node->cfs_fs);
	return;

fs_init_fail:
	free(fs_node);
out:
	if (rc != -ENOMEM) {
		log_info("FS:" STR256_F " failed to load from disk",
			 STR256_P(fs_name));
	}
	return;
}

static int endpoint_tenant_scan_cb(void *cb_ctx, struct tenant *tenant)
{
	int rc = 0;
	str256_t *endpoint_name = NULL;
	struct cfs_fs *fs = NULL;
	struct cfs_fs_node *fs_node = NULL;

	if (tenant == NULL) {
		rc = -ENOENT;
		goto out;
	}

	tenant_get_name(tenant, &endpoint_name);

	rc = cfs_fs_lookup(endpoint_name, &fs);
	log_debug("FS for tenant " STR256_F " is %p, rc = %d",
		  STR256_P(endpoint_name), fs, rc);

	/* TODO: We don't have auto-recovery for such cases,
	* so that we just halt execution if we cannot find
	* the corresponding filesystem. Later on,
	* CORTXFS should make an attempt to recover the filesystem,
	* and report to the user (using some alert mechanism
	* that can be implemented in CSM) in case if recovery
	* is not possible.
	*/
	if (rc != 0) {
		log_err("Tenant list and FS list is not consistent %d.", rc);
	}
	dassert(rc == 0);

	/* update fs_list */
	fs_node = container_of(fs, struct cfs_fs_node, cfs_fs);
	RC_WRAP_LABEL(rc, out, tenant_copy, tenant, &fs_node->cfs_fs.tenant);
out:
	return rc;
}

int cfs_fs_init(const struct cfs_endpoint_ops *e_ops)
{
	int rc = 0;
	/* initailize the cortxfs module endpoint operation */
	dassert(e_ops != NULL);
	g_e_ops = e_ops;
	RC_WRAP_LABEL(rc, out, ns_scan, fs_ns_scan_cb);
	RC_WRAP_LABEL(rc, out, cfs_endpoint_init);
out:
	log_debug("filesystem initialization, rc=%d", rc);
	return rc;
}

int cfs_endpoint_init(void)
{
	int rc = 0;

	RC_WRAP_LABEL(rc, out, tenant_scan, endpoint_tenant_scan_cb, NULL);
	dassert(g_e_ops->init != NULL);
	RC_WRAP_LABEL(rc, out, g_e_ops->init);
out:
	log_debug("endpoint initialization, rc=%d", rc);
	return rc;
}

int cfs_endpoint_fini(void)
{
	int rc = 0;
	struct cfs_fs_node *fs_node = NULL, *fs_node_ptr = NULL;

	dassert(g_e_ops->fini != NULL);
	RC_WRAP_LABEL(rc, out, g_e_ops->fini);
	LIST_FOREACH_SAFE(fs_node, &fs_list, link, fs_node_ptr) {
		fs_node->cfs_fs.tenant = NULL;
	}

out:
	log_debug("endpoint finalize, rc=%d", rc);
	return rc;
}

int cfs_fs_fini(void)
{
	int rc = 0;
	struct cfs_fs_node *fs_node = NULL, *fs_node_ptr = NULL;

	RC_WRAP_LABEL(rc, out, cfs_endpoint_fini);
	LIST_FOREACH_SAFE(fs_node, &fs_list, link, fs_node_ptr) {
		LIST_REMOVE(fs_node, link);
		tenant_free(fs_node->cfs_fs.tenant);
		fs_node_deinit(fs_node);
		free(fs_node);
	}

out:
	log_debug("filesystem finalize, rc=%d", rc);
	return rc;
}

int cfs_fs_scan_list(int (*fs_scan_cb)(const struct cfs_fs_list_entry *list,
		     void *args), void *args)
{
	int rc = 0;
	struct cfs_fs_node *fs_node = NULL;
	struct cfs_fs_list_entry fs_entry;
	LIST_FOREACH(fs_node, &fs_list, link) {
		dassert(fs_node != NULL);
		dassert(fs_node->cfs_fs.ns != NULL);
		cfs_fs_get_name(&fs_node->cfs_fs, &fs_entry.fs_name);
		cfs_fs_get_endpoint(&fs_node->cfs_fs,
				     (void **)&fs_entry.endpoint_info);
		RC_WRAP_LABEL(rc, out, fs_scan_cb, &fs_entry, args);
	}
out:
	return rc;
}

int cfs_endpoint_scan(int (*cfs_scan_cb)(const struct cfs_endpoint_info *info,
                     void *args), void *args)
{
	int rc = 0;
	struct cfs_fs_node *fs_node = NULL;
	struct cfs_endpoint_info ep_list;

	LIST_FOREACH(fs_node, &fs_list, link) {
		dassert(fs_node != NULL);
		dassert(fs_node->cfs_fs.ns != NULL);

		if (fs_node->cfs_fs.tenant == NULL)
			continue;

		cfs_fs_get_name(&fs_node->cfs_fs, &ep_list.ep_name);
		cfs_fs_get_id(&fs_node->cfs_fs, &ep_list.ep_id);
		cfs_fs_get_endpoint(&fs_node->cfs_fs,
				     (void **)&ep_list.ep_info);
		RC_WRAP_LABEL(rc, out, cfs_scan_cb, &ep_list, args);
	}
out:
	return rc;
}

int cfs_fs_create(const str256_t *fs_name)
{
        int rc = 0;
	struct namespace *ns;
	struct cfs_fs_node *fs_node;
	size_t ns_size = 0;

	rc = cfs_fs_lookup(fs_name, NULL);
        if (rc == 0) {
		log_err(STR256_F " already exist rc=%d\n",
			STR256_P(fs_name), rc);
		rc = -EEXIST;
		goto out;
        }

	/* create new node in fs_list */
	fs_node = calloc(1, sizeof(struct cfs_fs_node));
	if (!fs_node) {
		rc = -ENOMEM;
		goto out;
	}

	RC_WRAP_LABEL(rc, free_fs_node, ns_create, fs_name, &ns, &ns_size);

	struct kvtree *kvtree = NULL;
	struct stat bufstat;

	/* Set stat */
	memset(&bufstat, 0, sizeof(struct stat));
	bufstat.st_mode = S_IFDIR|0777;
	bufstat.st_ino = CFS_ROOT_INODE;
	bufstat.st_nlink = 2;
	bufstat.st_uid = 0;
	bufstat.st_gid = 0;
	bufstat.st_atim.tv_sec = 0;
	bufstat.st_mtim.tv_sec = 0;
	bufstat.st_ctim.tv_sec = 0;

	RC_WRAP_LABEL(rc, delete_ns, kvtree_create, ns, (void *)&bufstat,
		      sizeof(struct stat), &kvtree);

	RC_WRAP_LABEL(rc, delete_kvtree, fs_node_init, fs_node, ns, ns_size);
	RC_WRAP_LABEL(rc, deinit_fs_node, cfs_ino_num_gen_init,
		      &fs_node->cfs_fs);

	LIST_INSERT_HEAD(&fs_list, fs_node, link);
	goto out;

deinit_fs_node:
	fs_node_deinit(fs_node);
delete_kvtree:
	kvtree_delete(kvtree);
delete_ns:
	ns_delete(ns);
free_fs_node:
	free(fs_node);
out:
	if (rc != -ENOMEM) {
		log_info("fs_name=" STR256_F " rc=%d", STR256_P(fs_name), rc);
	}
        return rc;
}

int cfs_endpoint_create(const str256_t *endpoint_name, const char *endpoint_options)
{
	int rc = 0;
	uint16_t fs_id = 0;
	struct cfs_fs_node *fs_node = NULL;
	struct cfs_fs *fs = NULL;
	struct tenant *tenant;


	/* check file system exist */
	rc = cfs_fs_lookup(endpoint_name, &fs);
	if (rc != 0) {
		log_err("Can't create endpoint for non existent fs");
		rc = -ENOENT;
		goto out;
	}

	if (fs->tenant != NULL ) {
		log_err("fs=" STR256_F " already exported",
			 STR256_P(endpoint_name));
		rc = -EEXIST;
		goto out;
	}

	/* get filesyetm ID */
	cfs_fs_get_id(fs, &fs_id);

	dassert(g_e_ops->create != NULL);
	RC_WRAP_LABEL(rc, out, g_e_ops->create, endpoint_name->s_str,
		      fs_id,endpoint_options);
	/* create tenant object */
	RC_WRAP_LABEL(rc, out, tenant_create, endpoint_name, &tenant,
		      fs_id, endpoint_options);

	/* update fs_list */
	fs_node = container_of(fs, struct cfs_fs_node, cfs_fs);
	RC_WRAP_LABEL(rc, out, tenant_copy, tenant, &fs_node->cfs_fs.tenant);
	tenant = NULL;

out:
	log_info("endpoint_name=" STR256_F " rc=%d", STR256_P(endpoint_name), rc);
	return rc;
}

int cfs_endpoint_delete(const str256_t *endpoint_name)
{
	int rc = 0;
	uint16_t ns_id = 0;
	struct cfs_fs *fs = NULL;
	struct cfs_fs_node *fs_node = NULL;

	/* check endpoint exist */
	rc = cfs_fs_lookup(endpoint_name, &fs);
	if (rc != 0) {
		log_err("Can not delete " STR256_F ". endpoint for non existent. fs",
			 STR256_P(endpoint_name));
		rc = -ENOENT;
		goto out;
	}

	if (fs->tenant == NULL) {
		log_err("Can not delete " STR256_F ". endpoint. Doesn't existent.",
			 STR256_P(endpoint_name));
		rc = -ENOENT;
		goto out;
	}

	/** TODO: check if FS is mounted.
	 * We should not remove the endpoint it is still
	 * "mounted" on one of the clients. Right now,
	 * we don't have a mechanism to check that,
	 * so that ignore this requirement.
	 */

	/* get namespace ID */
	cfs_fs_get_id(fs, &ns_id);

	dassert(g_e_ops->delete != NULL);
	RC_WRAP_LABEL(rc, out, g_e_ops->delete, ns_id);
	/* delete tenant from nsal */
	fs_node = container_of(fs, struct cfs_fs_node, cfs_fs);
	RC_WRAP_LABEL(rc, out, tenant_delete, fs_node->cfs_fs.tenant);

	/* Remove endpoint from the fs list */
	tenant_free(fs_node->cfs_fs.tenant);
	fs_node->cfs_fs.tenant= NULL;

out:
	log_info("endpoint_name=" STR256_F " rc=%d", STR256_P(endpoint_name), rc);
	return rc;
}

int cfs_fs_delete(const str256_t *fs_name)
{
	int rc = 0;
	struct cfs_fs *fs;
	struct cfs_fs_node *fs_node = NULL;

	rc = cfs_fs_lookup(fs_name, &fs);
	if (rc != 0) {
		log_err("Can not delete " STR256_F ". FS doesn't exists.",
			STR256_P(fs_name));
		goto out;
	}

	if (fs->tenant != NULL) {
		log_err("Can not delete exported " STR256_F ". Fs",
			STR256_P(fs_name));
		rc = -EINVAL;
		goto out;
	}

	rc = cfs_fs_is_empty(fs);
	if (rc != 0) {
		log_err("Can not delete FS " STR256_F ". It is not empty",
			STR256_P(fs_name));
		goto out;
	}

	/* Remove fs and its entries from the cortxfs list */
	fs_node = container_of(fs, struct cfs_fs_node, cfs_fs);
	LIST_REMOVE(fs_node, link);
	RC_WRAP_LABEL(rc, out, cfs_ino_num_gen_fini, fs);
	RC_WRAP_LABEL(rc, out, kvtree_fini, fs->kvtree);
	kvnode_fini(fs->root_node);
	RC_WRAP_LABEL(rc, out, kvtree_delete, fs->kvtree);
	RC_WRAP_LABEL(rc, out, ns_delete, fs->ns);

	tenant_free(fs->tenant);
	free(fs_node->cfs_fs.root_node);
	free(fs_node);

out:
	log_info("fs_name=" STR256_F " rc=%d", STR256_P(fs_name), rc);
	return rc;
}

void cfs_fs_get_name(const struct cfs_fs *fs, str256_t **name)
{
	dassert(fs);
	ns_get_name(fs->ns, name);
}

void cfs_fs_get_endpoint(const struct cfs_fs *fs, void **info)
{
	dassert(fs);
	if (fs->tenant == NULL) {
		*info = NULL;
	} else {
		tenant_get_info(fs->tenant, info);
	}
}

int cfs_fs_open(const char *fs_name, struct cfs_fs **ret_fs)
{
	int rc;
	struct cfs_fs *fs = NULL;
	kvs_idx_fid_t ns_fid;
	str256_t name;

	str256_from_cstr(name, fs_name, strlen(fs_name));
	rc = cfs_fs_lookup(&name, &fs);
	if (rc != 0) {
		log_err(STR256_F " FS not found rc=%d",
				STR256_P(&name), rc);
		rc = -ENOENT;
		goto error;
	}

	ns_get_fid(fs->ns, &ns_fid);
	//RC_WRAP_LABEL(rc, error, kvs_index_open, kvstor, &ns_fid, index);

	*ret_fs = fs;

error:
	log_info("cfs_fs_open done, FS: %s, rc: %d, ptr: %p",
		 fs_name, rc, fs);
	if (rc != 0) {
		log_err("Cannot open fid for fs_name=%s, rc:%d", fs_name, rc);
	}

	return rc;
}

void cfs_fs_close(struct cfs_fs *cfs_fs)
{
	// This is empty placeholder for future use
	log_warn("Unused function is being called!");
}
