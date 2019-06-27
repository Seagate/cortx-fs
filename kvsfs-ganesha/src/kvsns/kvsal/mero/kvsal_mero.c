/*
 * vim:noexpandtab:shiftwidth=8:tabstop=8:
 *
 * Copyright (C) CEA, 2016
 * Author: Philippe Deniel  philippe.deniel@cea.fr
 *
 * contributeur : Philippe DENIEL   philippe.deniel@cea.fr
 *
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 *
 * -------------
 */

/* kvsal_mero.c
 * KVS Abstraction Layer: interface for MERO
 */

#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include "../../common/mero/m0common.h"
#include <kvsns/kvsal.h>
#include <kvsns/kvsns.h>
#include "kvsns/log.h"

/* The REDIS context exists in the TLS, for MT-Safety */

int kvsal_init(struct collection_item *cfg_items)
{
	return m0init(cfg_items);
}

int kvsal_fini(void)
{
	m0fini();

	return 0;
}

int kvsal_begin_transaction(void)
{
	return 0;
}

int kvsal_end_transaction(void)
{
	return 0;
}

int kvsal_discard_transaction(void)
{
	return 0;
}

int kvsal_exists(char *k)
{
	size_t klen;
	size_t vlen = VLEN;
	char myval[VLEN];

	klen = strnlen(k, KLEN)+1;
	return m0kvs_get(k, klen, myval, &vlen);
}

int kvsal2_exists(void *ctx, char *k, size_t klen)
{
	size_t vlen = VLEN;
	char myval[VLEN];

	return m0kvs2_get(ctx, k, klen, myval, &vlen);
}

int kvsal_set_char(char *k, char *v)
{
	size_t klen;
	size_t vlen;

	klen = strnlen(k, KLEN)+1;
	vlen = strnlen(v, VLEN)+1;
	return m0kvs_set(k, klen, v, vlen);
}

int kvsal2_set_char(void *ctx, char *k, size_t klen, char *v, size_t vlen)
{
	return m0kvs2_set(ctx, k, klen, v, vlen);
}

int kvsal3_set_bin(void *ctx, void *k, const size_t klen, void *v,
		   const size_t vlen)
{
	return m0kvs3_set(ctx, k, klen, v, vlen);
}

int kvsal2_set_bin(void *ctx, const void *k, size_t klen, const void *v,
		   size_t vlen)
{
	return m0kvs2_set(ctx, k, klen, v, vlen);
}

int kvsal_get_char(char *k, char *v)
{
	size_t klen;
	size_t vlen = VLEN;

	klen = strnlen(k, KLEN)+1;
	return m0kvs_get(k, klen, v, &vlen);
}

int kvsal2_get_char(void *ctx, char *k, size_t klen, char *v, size_t vlen)
{
	return m0kvs2_get(ctx, k, klen, v, &vlen);
}

int kvsal2_get_bin(void *ctx, const void *k, size_t klen, void *v, size_t vlen)
{
	return m0kvs2_get(ctx, k, klen, v, &vlen);
}

int kvsal3_get_bin(void *ctx, void *k, const size_t klen, void **v,
		  size_t *vlen)
{
	return m0kvs3_get(ctx, k, klen, v, vlen);
}

int kvsal_set_stat(char *k, struct stat *buf)
{
	size_t klen;

	klen = strnlen(k, KLEN)+1;
	return m0kvs_set(k, klen,
			  (char *)buf, sizeof(struct stat));
}

int kvsal_get_stat(char *k, struct stat *buf)
{
	size_t klen;
	size_t vlen = sizeof(struct stat);

	klen = strnlen(k, KLEN)+1;
	return m0kvs_get(k, klen,
			  (char *)buf, &vlen);
}

int kvsal_set_binary(char *k, char *buf, size_t size)
{
	size_t klen;

	klen = strnlen(k, KLEN)+1;
	return m0kvs_set(k, klen,
			  buf, size);
}

int kvsal_get_binary(char *k, char *buf, size_t *size)
{
	size_t klen;

	klen = strnlen(k, KLEN)+1;
	return m0kvs_get(k, klen,
			  buf, size);
}

int kvsal_incr_counter(char *k, unsigned long long *v)
{
	int rc;
	char buf[VLEN];
	size_t vlen = VLEN;
	size_t klen;

	klen = strnlen(k, KLEN) + 1;

	rc = m0kvs_get(k, klen, buf, &vlen);
	if (rc != 0)
		return rc;

	sscanf(buf, "%llu", v);
	*v += 1;
	snprintf(buf, VLEN, "%llu", *v);
	vlen = strnlen(buf, VLEN)+1;

	rc = m0kvs_set(k, klen, buf, vlen);
	if (rc != 0)
		return rc;

	return 0;
}

int kvsal2_incr_counter(void *ctx, char *k, unsigned long long *v)
{
	int rc;
	char buf[VLEN];
	size_t vlen = VLEN;
	size_t klen;

	klen = strnlen(k, KLEN) + 1;

	/* @todo: Do inode fetch and put in a single transaction */
	rc = m0kvs2_get(ctx, k, klen, buf, &vlen);
	if (rc != 0)
		return rc;

	sscanf(buf, "%llu", v);

	*v += 1;
	snprintf(buf, VLEN, "%llu", *v);
	vlen = strnlen(buf, VLEN)+1;
	log_debug("inode counter=%llu", *v);
	rc = m0kvs2_set(ctx, k, klen, buf, vlen);
	if (rc != 0)
		return rc;

	return 0;
}

int kvsal_del(char *k)
{
	size_t klen;

	klen = strnlen(k, KLEN)+1;
	return m0kvs_del(k, klen);
}

int kvsal2_del(void *ctx, char *k, size_t klen)
{
	return m0kvs2_del(ctx, k, klen);
}

int kvsal2_del_bin(void *ctx, const void *key, size_t klen)
{
	return m0kvs2_del(ctx, (char *) key, klen);
}

bool get_list_cb_size(char *k, void *arg)
{
	int size;

	memcpy((char *)&size, (char *)arg, sizeof(int));
	size += 1;
	memcpy((char *)arg, (char *)&size, sizeof(int));

	return true;
}

int kvsal_init_list(kvsal_list_t *list)
{
	if (!list)
		return -EINVAL;

	list->size = 0;
	list->content = NULL;

	return 0;
}


int kvsal_get_list_size(char *pattern)
{
	char initk[KLEN];
	int size = 0;
	int rc;

	strncpy(initk, pattern, KLEN);
	initk[strnlen(pattern, KLEN)-1] = '\0';

	rc = m0_pattern_kvs(initk, pattern,
			    get_list_cb_size, &size);
	if (rc < 0)
		return rc;

	return size;
}

int kvsal2_get_list_size(void *ctx, char *pattern, size_t plen)
{
	char initk[KLEN];
	int size = 0;
	int rc;

	strcpy(initk, pattern);
	initk[plen - 2] = '\0';

	rc = m0_pattern2_kvs(ctx, initk, pattern,
			     get_list_cb_size, &size);
	if (rc < 0)
		return rc;

	return size;
}

bool populate_list(char *k, void *arg)
{
	kvsal_list_t *list;
	kvsal_item_t *item;
	kvsal_item_t *content;

	list = (kvsal_list_t *)arg;

	if (!list)
		return false;

	list->size +=1;
	content = list->content;

	list->content = realloc(content, list->size*sizeof(kvsal_item_t));
	if (list->content == NULL)
		return false;

	item = &list->content[list->size - 1];

	strncpy(item->str, k, KLEN);
	item->offset = list->size -1;

	return true;
}

int kvsal_fetch_list(char *pattern, kvsal_list_t *list)
{
	char initk[KLEN];

        if (!pattern || !list)
                return -EINVAL;


	strncpy(initk, pattern, KLEN);
	initk[strnlen(pattern, KLEN)-1] = '\0';

	return  m0_pattern_kvs(initk, pattern,
				populate_list, list);
}

int kvsal2_fetch_list(void *ctx, char *pattern, kvsal_list_t *list)
{
	char initk[KLEN];

	if (!pattern || !list)
		return -EINVAL;

	strncpy(initk, pattern, KLEN);
	initk[strnlen(pattern, KLEN) - 1] = '\0';

	return m0_pattern2_kvs(ctx, initk, pattern, populate_list, list);
}

int kvsal_dispose_list(kvsal_list_t *list)
{
        if (!list)
                return -EINVAL;

        return 0;
}

/** @todo: too many strncpy and mallocs, this should be optimized */
int kvsal_get_list(kvsal_list_t *list, int start, int *size,
                    kvsal_item_t *items)
{
	int i;

	if (list->size < (start + *size))
		*size = list->size - start;


	for (i = start; i < start + *size ; i++) {
		items[i-start].offset = i;
		strncpy(items[i-start].str,
			list->content[i].str, KLEN);
	}

	return 0;
}

/* Since the implementation should preftech the list in memory,
 * this call can become pretty expensive */
int kvsal_get_list_pattern(char *pattern, int start, int *size,
			   kvsal_item_t *items)
{
	char initk[KLEN];
	kvsal_list_t list;
	int rc;

	strncpy(initk, pattern, KLEN);
	initk[strnlen(pattern, KLEN)-1] = '\0';

	rc = kvsal_fetch_list(pattern, &list);
	if (rc < 0 )
		return rc;

	rc = kvsal_get_list(&list, start, size, items);
	if (rc < 0) {
		kvsal_dispose_list(&list); /* Try to clean up */
		return rc;
	}

	rc = kvsal_dispose_list(&list);
	if (rc < 0 )
		return rc;

	return 0;
}

int kvsal_create_fs_ctx(unsigned long fs_id, void **fs_ctx)
{
	int rc;

	rc = m0_idx_create(fs_id, (struct m0_clovis_idx **)fs_ctx);
	if (rc != 0) {
		log_err("Failed to create idx, rc=%d", rc);
		return rc;
	}
	return 0;
}

static bool kvsal_prefix_iter_has_prefix(struct kvsal_prefix_iter *iter)
{
	void *key;
	size_t key_len;

	key_len = kvsal_iter_get_key(&iter->base, &key);
	KVSNS_DASSERT(key_len >= iter->prefix_len);
	return memcmp(iter->prefix, key, iter->prefix_len) == 0;
}

bool kvsal_prefix_iter_find(struct kvsal_prefix_iter *iter)
{
	return m0_key_iter_find(&iter->base, iter->prefix, iter->prefix_len) &&
		kvsal_prefix_iter_has_prefix(iter);
}

bool kvsal_prefix_iter_next(struct kvsal_prefix_iter *iter)
{
	return m0_key_iter_next(&iter->base) &&
		kvsal_prefix_iter_has_prefix(iter);
}

void kvsal_prefix_iter_fini(struct kvsal_prefix_iter *iter)
{
	m0_key_iter_fini(&iter->base);
}

size_t kvsal_iter_get_key(struct kvsal_iter *iter, void **buf)
{
	return m0_key_iter_get_key(iter, buf);
}

size_t kvsal_iter_get_value(struct kvsal_iter *iter, void **buf)
{
	return m0_key_iter_get_value(iter, buf);
}

int kvsal_alloc(void **ptr, uint64_t size)
{
	int rc = 0;

	*ptr = m0kvs_alloc(size);
	if (ptr == NULL)
		rc = -ENOMEM;

	return rc;
}

void kvsal_free(void *ptr)
{
	return m0kvs_free(ptr);
}
