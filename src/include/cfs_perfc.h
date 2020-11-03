/*
 * Filename:	cfs_perfc.h
 * Description:	This module defines performance counters and helpers.
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

#ifndef __CFS_PERF_COUNTERS_H_
#define __CFS_PERF_COUNTERS_H_
/******************************************************************************/
#include "perf/tsdb.h" /* ACTION_ID_BASE */
#include "operation.h"
#include <pthread.h>
#include <string.h>
#include "debug.h"
#include "perf/perf-counters.h"

enum perfc_cfs_function_tags {
	PFT_CFS_START = PFTR_RANGE_1_START,
	PFT_CFS_READ,
	PFT_CFS_WRITE,
	PFT_CFS_GETATTR,
	PFT_CFS_SETATTR,
	PFT_CFS_ACCESS,
	PFT_CFS_MKDIR,
	PFT_CFS_RMDIR,
	PFT_CFS_READDIR,
	PFT_CFS_LOOKUP,
	PFT_CFS_END = PFTR_RANGE_1_END
};

enum perfc_cfs_entity_attrs {
	PEA_CFS_START = PEAR_RANGE_1_START,

	PEA_R_C_COUNT,
	PEA_R_C_OFFSET,
	PEA_R_C_RES_RC,

	PEA_GETATTR_RES_RC,

	PEA_SETATTR_RES_RC,

	PEA_ACCESS_FLAGS,
	PEA_ACCESS_RES_RC,
	PEA_CFS_END = PEAR_RANGE_1_END
};

enum perfc_cfs_entity_maps {
	PEM_CFS_START = PEMR_RANGE_1_START,
	PEM_CFS_TO_NFS,
	PEM_CFS_END = PEMR_RANGE_1_END
};

/******************************************************************************/
#endif /* __CFS_PERF_COUNTERS_H_ */
