/*
 * Filename: ut_cortxfs_file_ops.c
 * Description: Implementation tests for file creation
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

#include "cortxfs_fh.h"
#include "ut_cortxfs_helper.h"

/**
 * Setup for file creation test.
 * Description: check if file exists.
 * Strategy:
 *  1. Lookup for file.
 *  2. fail if lookup is successful
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. The lookup should fail with error -ENOENT.
 */
static int create_file_setup(void **state)
{
	int rc = 0;

	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);

	char *file_name = "test_file";
	ut_cfs_obj->file_name = file_name;

	cfs_ino_t file_inode = 0LL;

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, file_name,
			&file_inode);

	ut_assert_int_equal(rc, -ENOENT);

	rc = 0;

	return rc;
}

/**
 * Test for file creation.
 * Description: create a file.
 * Strategy:
 *  1. Create a file.
 *  2. Lookup for created file.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. The lookup should verify file creation.
 */
static void create_file(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);

	cfs_ino_t file_inode = 0LL;

	rc = cfs_creat(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->current_inode, ut_cfs_obj->file_name, 0755,
			&file_inode);

	ut_assert_int_equal(rc, 0);

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, ut_cfs_obj->file_name,
			&file_inode);

	ut_assert_int_equal(rc, 0);
}

/**
 * Test for verifying file handle and it's implementation
 * Description: create a file, lookup file handle and derived file handle from
 * inode value and compare it's internal value.
 * Strategy:
 *  1. Create a file.
 *  2. Lookup for created file, which ultimately do a fH lookup and then extract
 *     inode from it and return that.
 *  3. Cross validate the inode number which we got while creating file
 *  4. As we have inode, get FH from inode
 *  5. Extract inode from FH, internally stats of file will be accessed to get
 *     inode number
 *  6. Cross validate the inode number which we got while creating file
 *  7. Destroy the file handle, stat attributes of file will be saved down to
 *     object store.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. The lookup should verify file creation and returned inode should match
 *  3. FH should prove a correctness by returning back correct inode from
 *     internal stats it hold
 */
static void verify_file_handle(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);
	struct cfs_fh *fh = NULL;

	cfs_ino_t file_inode = 0LL;
	cfs_ino_t file_inode2 = 0LL;
	cfs_ino_t *file_inode3 = NULL;

	rc = cfs_creat(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
		       &ut_cfs_obj->current_inode, ut_cfs_obj->file_name, 0755,
		       &file_inode);

	ut_assert_int_equal(rc, 0);

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, ut_cfs_obj->file_name,
			&file_inode2);

	ut_assert_int_equal(rc, 0);
	ut_assert_int_equal(file_inode, file_inode2);

	cfs_fh_from_ino(ut_cfs_obj->cfs_fs, &file_inode, &fh);
	ut_assert_not_null(fh);
	ut_assert_int_equal(rc, 0);

	file_inode3 = cfs_fh_ino(fh);
	ut_assert_not_null(file_inode3);
	ut_assert_int_equal(file_inode, *file_inode3);

	if (fh) {
		cfs_fh_destroy(fh);
	}
}

/**
 * Setup for longname file creation test.
 * Description: check if file exists.
 * Strategy:
 *  1. Lookup for file.
 *  2. Fail if lookup is successful.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. The lookup should fail with error -ENOENT.
 */
static int create_longname255_file_setup(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);

	char *file_name = "123456789012345678901234567890123456789012345678901"
			"123456789012345678901234567890123456789012345678901"
			"123456789012345678901234567890123456789012345678901"
			"123456789012345678901234567890123456789012345678901"
			"123456789012345678901234567890123456789012345678901";

	ut_cfs_obj->file_name = file_name;
	cfs_ino_t file_inode = 0LL;

	ut_assert_int_equal(255, strlen(file_name));

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, file_name, &file_inode);

	ut_assert_int_equal(rc, -ENOENT);

	rc = 0;

	return rc;
}

/**
 * Test for longname file creation.
 * Description: create a longname file.
 * Strategy:
 *  1. Create a longname file.
 *  2. Lookup for created file.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. The lookup should verify file creation.
 */
static void create_longname255_file(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);

	cfs_ino_t file_inode = 0LL;

	rc = cfs_creat(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->current_inode, ut_cfs_obj->file_name, 0755,
			&file_inode);

	ut_assert_int_equal(rc, 0);

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, ut_cfs_obj->file_name,
			&file_inode);

	ut_assert_int_equal(rc, 0);
}

/**
 * Setup for existing file creation test.
 * Description: create file.
 * Strategy:
 *  1. Lookup for file
 *  2. Fail if lookup is successful
 *  3. Create a file.
 *  4. Lookup for created file.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. First lookup should fail with error -ENOENT
 *  3. Second lookup should be successful 
 */
static int create_exist_file_setup(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);

	char * file_name = "test_existing_file";
	cfs_ino_t file_inode = 0LL;

	ut_cfs_obj->file_name = file_name;

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, file_name, &file_inode);

	ut_assert_int_equal(rc, -ENOENT);

	rc = 0;

	rc = cfs_creat(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->current_inode, file_name, 0755, &file_inode);

	ut_assert_int_equal(rc, 0);

	rc = cfs_lookup(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->parent_inode, file_name, &file_inode);

	ut_assert_int_equal(rc, 0);

	return rc;
}

/**
 * Test for existing file creation.
 * Description: create a existing file.
 * Strategy:
 *  1. Create file.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. File creation should fail with error -EEXIST.
 */
static void create_exist_file(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = ENV_FROM_STATE(state);	

	cfs_ino_t file_inode = 0LL;

	rc = cfs_creat(ut_cfs_obj->cfs_fs, &ut_cfs_obj->cred,
			&ut_cfs_obj->current_inode, ut_cfs_obj->file_name, 0755,
			&file_inode);

	ut_assert_int_equal(rc, -EEXIST);
}

/**
 * teardown for file test.
 * Description: delete file.
 * Strategy:
 *  1. Delete file.
 * Expected behavior:
 *  1. No errors from CORTXFS API.
 *  2. Delete should be successful
 */
static int file_test_teardown(void **state)
{
	int rc = 0;

	rc = ut_file_delete(state);

	ut_assert_int_equal(rc, 0);

	return rc;
}

/**
 * Setup for file_ops test group.
 */
static int file_ops_setup(void **state)
{
	int rc = 0;
	struct ut_cfs_params *ut_cfs_obj = NULL;

	ut_cfs_obj = calloc(sizeof(struct ut_cfs_params), 1);

	ut_assert_not_null(ut_cfs_obj);

	*state = ut_cfs_obj;
	rc = ut_cfs_fs_setup(state);

	ut_assert_int_equal(rc, 0);

	return rc;
}

/*
 * Teardown for file_ops test group.
 */
static int file_ops_teardown(void **state)
{
	int rc = 0;

	rc = ut_cfs_fs_teardown(state);
	ut_assert_int_equal(rc, 0);

	free(*state);

	return rc;
}

int main(void)
{
	int rc = 0;
	char *test_log = "/var/log/cortx/test/ut/ut_cortxfs.log";

	printf("File creation tests\n");

	rc = ut_load_config(CONF_FILE);
	if (rc != 0) {
		printf("ut_load_config: err = %d\n", rc);
		goto end;
	}

	test_log = ut_get_config("cortxfs", "log_path", test_log);

	rc = ut_init(test_log);
	if (rc != 0) {
		printf("ut_init failed, log path=%s, rc=%d.\n", test_log, rc);
		goto out;
	}

	struct test_case test_list[] = {
		ut_test_case(create_file, create_file_setup,
			     file_test_teardown),
		ut_test_case(create_longname255_file,
			     create_longname255_file_setup, file_test_teardown),
		ut_test_case(create_exist_file, create_exist_file_setup,
			     file_test_teardown),
		ut_test_case(verify_file_handle, create_file_setup,
			     file_test_teardown),
	};

	int test_count = sizeof(test_list)/sizeof(test_list[0]);
	int test_failed = 0;

	test_failed = ut_run(test_list, test_count, file_ops_setup,
			     file_ops_teardown);

	ut_fini();

	ut_summary(test_count, test_failed);

out:
	free(test_log);

end:
	return rc;
}
