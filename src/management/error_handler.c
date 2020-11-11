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
	"The filesystem name specified is not valid.",
	"The filesystem name you tried to create already exists.",
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
	default:
		resp_id = ERR_RES_DEFAULT;
	}

	return error_resp_messages[resp_id];
}

