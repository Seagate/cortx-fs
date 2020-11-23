/*
 * Filename: error_handler.h
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
#ifndef ERROR_HANDLER_H_
#define ERROR_HANDLER_H_

/* User defined error codes set greater than unix error codes.*/
enum error_code {
	INVALID_ETAG = 133,		/* Invalid ETag */
	BAD_DIGEST, 			/* Non-matching HASH */
	MISSING_ETAG,			/* Object ETag is missing */
	INVALID_PAYLOAD,		/* Payload data is invalid */
	INVALID_PATH_PARAMS		/* Invalid REST API path parameters */
};

/**
 * ############ Error response mapping structure  #####################
 */

/**
 * The error message ids for response messages.
 */
enum error_resp_id {

	/* Response IDs for fs create api */
	ERR_RES_INVALID_FSNAME = 1,
	ERR_RES_FS_EXIST,

	/* Response IDs for fs delete api */
	ERR_RES_FS_NONEXIST,
	ERR_RES_FS_EXPORT_EXIST,
	ERR_RES_FS_NOT_EMPTY,

	/* Generic IDs */
	ERR_RES_INVALID_ETAG,
	ERR_RES_BAD_DIGEST,
	ERR_RES_MISSING_ETAG,
	ERR_RES_INVALID_PAYLOAD,
	ERR_RES_INVALID_PATH_PARAMS,

	/* Default error response ID */
	ERR_RES_DEFAULT,
	ERR_RES_MAX
};

/*
 *	Mapping APIs corresponding to every REST API
 *
 *	Returns an error response message based on the error code
 */
const char* fs_create_errno_to_respmsg(int err_code);

const char* fs_delete_errno_to_respmsg(int err_code);

#endif /* ERROR_HANDLER_H_ */
