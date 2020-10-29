/**
 * Filename: api_docs.c
 * Description: Open-API specification document access controller.
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
 *
 */

#include <stdio.h> /* sprintf */
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <evhtp.h>
#include <json/json.h> /* for json_object */
#include <management.h>
#include <common/log.h>
#include <str.h>
#include <debug.h>
#include "internal/controller.h"

/**
 * ##############################################################
 * #		API-DOCS API'S				#
 * ##############################################################
 */

/* api request object */
struct apidocs_get_api_req {
	/* ... */
};

/* api response object */
struct apidocs_get_api_resp {
	/* ... */
};

struct apidocs_get_api {
	struct apidocs_get_api_req  req;
	struct apidocs_get_api_resp resp;
};

static int apidocs_get_send_response(struct controller_api *apidocs_get,
				       void *args)
{
	int resp_code = 0;
	struct request *request = NULL;

	request = apidocs_get->request;

	/* @TODO Currently, the swagger HTML doc is not present to parse and send
	        it to the client. Hence sending HTTP Response 404(Page not found)
	        response code to client.
	*/
	resp_code = errno_to_http_code(ENOENT);

	request_send_response(request, resp_code);

	log_debug("http response code : %d", resp_code);
	return 0;
}

static int apidocs_get_process_request(struct controller_api *apidocs_get,
					   void *args)
{
	/* @TODO Include validation of headers and payload data */

	log_debug("Fetching apidocs_get controller api.");

	request_next_action(apidocs_get);
	return 0;
}

static controller_api_action_func default_apidocs_get_actions[] =
{
	apidocs_get_process_request,
	apidocs_get_send_response,
};

static int apidocs_get_init(struct controller *controller,
		  struct request *request,
		  struct controller_api **api)
{
	int rc = 0;
	struct controller_api *apidocs_get = NULL;

	apidocs_get = malloc(sizeof(struct controller_api));
	if (apidocs_get == NULL) {
		rc = ENOMEM;
		log_err("Internal error: No memmory.\n");
		goto error;
	}

	/* Init. */
	apidocs_get->request = request;
	apidocs_get->controller = controller;

	apidocs_get->name = "GET";
	apidocs_get->type = APIDOCS_GET_ID;
	apidocs_get->action_next = 0;
	apidocs_get->action_table = default_apidocs_get_actions;

	apidocs_get->priv = calloc(1, sizeof(struct apidocs_get_api));
	if (apidocs_get->priv == NULL) {
		rc = ENOMEM;
		log_err("Internal error: No memmory.\n");
		goto error;
	}

	/* Assign InOut Parameter value. */
	*api = apidocs_get;
	apidocs_get = NULL;

error:
	if (apidocs_get) {
		if (apidocs_get->priv) {
			free(apidocs_get->priv);
		}

		free(apidocs_get);
	}
	log_debug("api=%p, rc=%d", *api, rc);

	return rc;
}

static void apidocs_get_fini(struct controller_api *apidocs_get)
{
	if (apidocs_get) {
		if (apidocs_get->priv) {
			free(apidocs_get->priv);
		}

		free(apidocs_get);
	}
}

/**
 * ##############################################################
 * #		API-DOCS CONTROLLER API'S				#
 * ##############################################################
 */

#define APIDOCS_NAME      "apidocs"
#define APIDOCS_API_URI   "/api-docs"

static char *default_apidocs_api_list[] =
{
#define XX(uc, lc, _)	#lc,
	APIDOCS_API_MAP(XX)
#undef XX
};

static struct controller_api_table apidocs_api_table [] =
{
#define XX(uc, lc, method)	{ #lc, #method, APIDOCS_ ## uc ## _ID },
	APIDOCS_API_MAP(XX)
#undef XX
};

static int apidocs_api_name_to_id(char *api_name, enum apidocs_api_id *api_id)
{
	int rc = EINVAL;
	int idx = 0;

	for (idx = 0; idx < APIDOCS_API_COUNT; idx++) {
		if (!strcmp(apidocs_api_table[idx].method, api_name)) {
			*api_id = apidocs_api_table[idx].id;
			rc = 0;
			break;
		}
	}
	return rc;
}

static int apidocs_api_init(char *api_name,
	       struct controller *controller,
	       struct request *request,
	       struct controller_api **api)
{
	int rc = 0;
	enum apidocs_api_id api_id;
	struct controller_api *apidocs_api = NULL;

	rc = apidocs_api_name_to_id(api_name, &api_id);
	if (rc != 0) {
		log_err("Unknown apidocs api-name : %s.\n", api_name);
		goto error;
	}

	switch (api_id) {
#define XX(uc, lc, _)							      \
		case APIDOCS_ ## uc ## _ID:					      \
			rc = apidocs_ ## lc ## _init(controller, request, &apidocs_api); \
			break;
			APIDOCS_API_MAP(XX)
#undef XX
		default:
			log_err("Not supported api : %s", api_name);
	}

	/* Assign the InOut variable api value. */
	*api = apidocs_api;

	log_debug("api_name=%s, controller=%p, api_id=%d, rc=%d", api_name,
	          controller, api_id, rc);
error:
	return rc;
}

static void apidocs_api_fini(struct controller_api *apidocs_api)
{
	char *api_name = NULL;
	enum apidocs_api_id api_id;

	api_name = apidocs_api->name;
	api_id = apidocs_api->type;

	switch (api_id) {
#define XX(uc, lc, _)					\
		case APIDOCS_ ## uc ## _ID:			\
			apidocs_ ## lc ## _fini(apidocs_api);		\
			break;
			APIDOCS_API_MAP(XX)
#undef XX
		default:
			log_err("Not supported api : %s", api_name);
	}

	log_debug("api_name=%s, api_id=%d", api_name, api_id);
}

static struct controller default_apidocs_controller =
{
	.name     = APIDOCS_NAME,
	.type     = CONTROLLER_APIDOCS_ID,
	.api_uri  = APIDOCS_API_URI,
	.api_list = default_apidocs_api_list,
	.api_init = apidocs_api_init,
	.api_fini = apidocs_api_fini,
};

int ctl_apidocs_init(struct server *server, struct controller **controller)
{
	int rc = 0;

	struct controller *apidocs_controller = NULL;

	apidocs_controller = malloc(sizeof(struct controller));
	if (apidocs_controller == NULL) {
		rc = ENOMEM;
		goto error;
	}

	/* Init apidocs_controller. */
	*apidocs_controller = default_apidocs_controller;
	apidocs_controller->server = server;

	/* Assign the return valure. */
	*controller = apidocs_controller;

error:
	log_debug("server=%p, apidocs_controller=%p, rc=%d", server,
	          apidocs_controller, rc);
	return rc;
}

void ctl_apidocs_fini(struct controller *apidocs_controller)
{
	free(apidocs_controller);
}
