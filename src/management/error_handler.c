/*
 * Filename: error_handler.c
 * Description: Response error handling APIs
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

#include "internal/error_handler.h"
#include <errno.h>

const char *error_resp_messages[] = {
	"Invalid error response ID",

	/* fs create api error response */
	"The filesystem name specified is not valid.",
	"The filesystem name you tried to create already exists.",

	/* fs delete api error response */
	"The specified filesystem does not exist.",
	"The filesystem you tried to delete is being exported.",
	"The filesystem you tried to delete is not empty.",

	/* Generic error responses */
	"The ETag should not be passed for a resource which is not modifiable.",
	"The HASH specified did not match what we received.",
	"The Object ETag is not sent.",
	"Invalid payload data passed.",
	"Invalid parameters passed with the API path.",

	/* Default error response */
	"Generic error message. Check cortx logs for more information."
};

const char* fs_create_errno_to_respmsg(int err_code)
{
	enum error_resp_id resp_id;

	switch (err_code) {
	case EINVAL:
		resp_id = ERR_RES_INVALID_FSNAME;
		break;
	case EEXIST:
		resp_id = ERR_RES_FS_EXIST;
		break;
	/* Since filesystem name cannot be modified. */
	case INVALID_ETAG:
		resp_id = ERR_RES_INVALID_ETAG;
		break;
	case INVALID_PAYLOAD:
		resp_id = ERR_RES_INVALID_PAYLOAD;
		break;
	default:
		resp_id = ERR_RES_DEFAULT;
	}

	return error_resp_messages[resp_id];
}

const char* fs_delete_errno_to_respmsg(int err_code)
{
	enum error_resp_id resp_id;

	switch (err_code) {
	case ENOENT:
		resp_id = ERR_RES_FS_NONEXIST;
		break;
	case EINVAL:
		resp_id = ERR_RES_FS_EXPORT_EXIST;
		break;
	case ENOTEMPTY:
		resp_id = ERR_RES_FS_NOT_EMPTY;
		break;
	case BAD_DIGEST:
		resp_id = ERR_RES_BAD_DIGEST;
		break;
	case MISSING_ETAG:
		resp_id = ERR_RES_MISSING_ETAG;
		break;
	case INVALID_PAYLOAD:
		resp_id = ERR_RES_INVALID_PAYLOAD;
		break;
	case INVALID_PATH_PARAMS:
		resp_id = ERR_RES_INVALID_PATH_PARAMS;
		break;
	default:
		resp_id = ERR_RES_DEFAULT;
	}

	return error_resp_messages[resp_id];
}
