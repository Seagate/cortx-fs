/*
 * Filename: cortxfs_fh.h
 * Description: CORTXFS File Handle API.
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

/* Low-level Design
 *
 * CORTXFS File Handle Overview.
 * -----------------------------------
 *
 * CORTXFS File Handle (cortxfs_fh) is an in-memory object that represents
 * a file object such as a regular file, a directory, a symlink etc.
 *
 * File Handles are obtained from the CORTXFS API using LOOKUP, READDIR or
 * deserialize calls.
 * These calls construct a new file handle per found object. READDIR produces
 * a file handle per each iteration over a directory.
 * File Handles cannot be constructed without making calls to the underlying
 * storage (to fetch kvnode).
 *
 * Root File Handle is a special file handle that can be constructed by
 * GETROOT call without specifying (parent_inode, dentry_name) pair.
 * However, it is still a subject to access checks.
 *
 *
 * FH Properties.
 * --------------
 * A File Handle object has the following properties:
 *	- Storage-level unique key (ObjectFID, IndexFID).
 *	- FS-level unique inode number (uint64_t).
 *	- per-object read-only attributes (type).
 *	- per-object mutable attributes (mode_t, uid:gid, {a,c,m}time).
 *	- Runtime locks.
 *	- Access reference count.
 *	- File state.
 *
 *
 * FH Serialization.
 * --------------
 *
 * CORTXFS Users can serialize a FH into a buffer of a pre-defined size, store
 * it somewhere, and deseiralize later. Also, FH supports obtaining of a unique
 * in-memory key to be used in containers (maps, sets).
 * Methods:
 *	- FH.serialize(buffer) - Writes on-wire representation into buffer.
 *	- FH.key(buffer) - Writes unique in-memory key of the FH.
 *	- deserialize(buffer, FH*) - Finds a file handle with the corresponding
 *		on-wire data in the storage and creates a new FH.
 *
 * FH Memory management.
 * ---------------------
 *
 * Since FHs are opaque to the callers, they are contructed with malloc() calls.
 * However, it is not so efficient when a caller has its own File Handle
 * structure which agregates CORTXFS FH. To prevent multiple allocations of the
 * same entity, FH provides methods for placement initialization in pre-allocated
 * buffers.
 * Methods:
 *	- create(args.., FH**) - Allocates memory and constructs a new FH.
 *	- create_inplace(args.., buffer) - Constructs FH in a pre-allocated
 *		memory region.
 *	- destroy(FH*) - Destroys internal data and deallocates memory.
 *	- destroy_inplace(FH*) - Destroys internal data.
 *	- inplace(buffer) - Macro to convert a uint8_t* to FH*.
 *
 * Note: Inplace memory management is safe with enabled pointer aliasing
 * optimizations as long as the API does not allow callers to use the same
 * buffer in more than one argument:
 * @{code}
 *	struct foo {
 *		int bar;
 *		int baz;
 *	};
 *	char buffer[64];
 *	#define FOO(_buf) ((struct foo*) _buf)
 *
 *	// Bad:
 *	#define FOO_BAR(_buf) &((struct foo*) _buf)->bar
 *	void fun1(struct foo *foo, int *bar) {
 *		*bar = 20;
 *		foo->bar = 10;
 *		printf("%d, *bar); // Prints 20 instead of 10
 *	}
 *	fun1(FOO(buffer), FOO_BAR(buffer));
 *
 *	// Better:
 *	struct foo *foo = FOO(buffer);
 *	fun1(foo, foo->bar); // Compiler can deduce bar's origin.
 *
 *	// Ideal:
 *	// Prevent the possiblity of aliasing
 *	void fun1(struct foo *foo);
 * @{endcode}
 *
 * Inplace memorty menagement is not implemented yet and will a part of
 * futher optimizations when CORTXFS FH will be fully integrated in CORTXFS
 * namespace code.
 */

#ifndef CFS_FH_H_
#define CFS_FH_H_

#include "cortxfs.h"
#include "nsal.h"
struct cfs_fh;

/* The example of CORTXFS API described below allocates new file handles in
 * heap. Later we can change that to use in-place allocated structures to
 * avoid extra malloc/free calls.
 * The function looks up a directory entry `name` in the given `parent_fh`
 * directory using `cred` for checking access.
 * If name and inodes are matching with the parent fh then it will create the
 * new FH same as parent and return the reference of it.
 * It returns a new file handle object fh.
 * @param[in] cred - User credentials.
 * @param[in] parent_fh - Parent file handle.
 * @param[in] name - Dentry name.
 * @param[out] fh - Pointer to a new file handle object.
 * @return 0 or -errno.
 */
int cfs_fh_lookup(const cfs_cred_t *cred,
                  struct cfs_fh *parent_fh, const char *name,
                  struct cfs_fh **fh);

/* Get a root file handle for given filesystem.
 * The function allocate, initialize it's all members and returns a new file
 * handle which represent the root directory of `fs` using `cred` for checking
 * access.
 * @param[in] fs - Filesystem context.
 * @param[in[ cred - User credentials.
 * @param[out] pfh - Pointer to the root FH object.
 */
int cfs_fh_getroot(struct cfs_fs *fs, const cfs_cred_t *cred,
                   struct cfs_fh **pfh);

/* Note: As of now, destroying FH does not update the stats in backend because
 * those are stale, the reason for that is FH is not supplied as input param to
 * each of the cortxfs API which can modify the stats of file directly in FH.
 * TODO: Temp_FH_op - to be removed
 * Uncomment the logic to dump the stats associated with FH once FH is present
 * everywhere all the update happens to FH
 * Dump the file attributes(kvnode) associated with a given FH
 * Release all resources and deallocate the memory region allocted for this FH
 * @param[in] fh - Any initialized FH.
 */
void cfs_fh_destroy(struct cfs_fh *fh);

/* Note: This is a temporary API and shall be removed once FH is given as input
 * to all the operation in FS and all the updates(like stats of file) happens
 * to the FH
 * TODO: Temp_FH_op - to be removed
 * Dump the file attributes(kvnode) associated with a given FH
 * Release all resources and deallocate the memory region allocted for this FH
 * @param[in] fh - Any initialized FH.
 */
void cfs_fh_destroy_and_dump_stat(struct cfs_fh *fh);

/* Get a pointer to node_id of a File Handle.
 * @param[in] fh - Any initialized FH.
 * @return Pointer to internal buffer which holds node_id number.
 */
node_id_t *cfs_node_id_from_fh(struct cfs_fh *fh);

/* Get a pointer to inode numuber of a File Handle.
 * @param[in] fh - Any initialized FH.
 * @return Pointer to internal buffer which holds inode number.
 */
cfs_ino_t *cfs_fh_ino(struct cfs_fh *fh);

/* Get a pointer to file system context from a File Handle.
 * @param[in] fh - Any initialized FH.
 * @return Pointer to FS context.
 */
struct cfs_fs *cfs_fs_from_fh(const struct cfs_fh *fh);

/* Get a pointer to kvnode hold by a File Handle.
 * @param[in] fh - Any initialized FH.
 * @return Pointer to internal buffer which holds struct kvnode.
 */
struct kvnode *cfs_kvnode_from_fh(struct cfs_fh *fh);

/* Get a pointer to attributes (stat) of a File Handle.
 * @param[in] fh - Any initialized FH.
 * @return Pointer to internal buffer which holds struct stat.
 */
struct stat *cfs_fh_stat(const struct cfs_fh *fh);

/* The function allocates a new FH, intializes it's all the members and return
 * a newly allocated FH.
 * @param[in] fs - Filesystem context.
 * @param[in] ino_num - Inode number.
 * @param[out] pfh - Pointer the created FH object.
 * @return 0 or -errno.
 * @TODO: When file handle will be represented by unique 128bit FID, input to
 * this API will be that which will be used to load/form required paramete to
 * build FH
 */
int cfs_fh_from_ino(struct cfs_fs *fs, const cfs_ino_t *ino_num,
                    struct cfs_fh **fh);

/******************************************************************************/
/* Representation */

/** Writes FH id into a buffer.
 * @param[in] - FH to be serialized.
 * @param[in,out] - Buffer for the id.
 * The caller must ensure to allocate at least CFS_FH_ONWIRE_SIZE bytes,
 * or use cfs_fh_onwire_t structure as a storage.
 */
int cfs_fh_serialize(const struct cfs_fh *fh, void* buffer, size_t max_size);

/** Creates a new in-memory file handle from a serialized file handle */
int cfs_fh_deserialize(struct cfs_fs *fs_ctx, const cfs_cred_t *cred,
		       const void* buffer, size_t buffer_size,
		       struct cfs_fh** pfh);

/** Unique key for in-memory containers on the local machine side.
 * NOTE: It could be diffent from the on-wire handle.
 */
void cfs_fh_key(const struct cfs_fh *fh, void **pbuffer, size_t *psize);

/** Max size of buffer to be used for FH serialization.
 * Note: the size is fixed however it is defined by the implementation
 * to prevent cases where CORTXFS FH on-wire has been extended or reduced
 * without recompiling the callers code.
 */
size_t cfs_fh_serialized_size(void);

/* A temporary function to be used for serialization until cortxfs_fh_get_fsid
 * is implemented (or until CORTXFS start using FIDs as primary keys).
 * @param[in] fh - FH object.
 * @param[in] fsid - Unique FS number; allows to make FHs unique across multiple
 *	filesystems.
 * @param[in,out] buffer - Buffer where serialized (on-wire) FH is stored.
 * @param[in] max_size - Size of the buffer.
 * @return -errno (fail) or positive number (succ) which corresponds to the
 * amount of bytes written into the buffer.
 */
int cfs_fh_ser_with_fsid(const struct cfs_fh *fh, uint64_t fsid, void *buffer,
			 size_t max_size);

/******************************************************************************/
#endif /* CFS_FH_H_ */
