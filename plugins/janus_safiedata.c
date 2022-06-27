/*! \file   janus_safiedata.c
 * \author Lorenzo Miniero <lorenzo@meetecho.com>
 * \copyright GNU General Public License v3
 * \brief  Janus SafieData plugin
 * \details Check the \ref safiedata for more details.
 *
 * \ingroup plugins
 * \ref plugins
 *
 * \page safiedata SafieData plugin documentation
 * This is a plugin implementing a very simple SafieData service
 *
 * \section vmailapi SafieData API
 *
 *
 * A successful request will result in an \c starting status event:
 *
\verbatim
{
	"safiedata" : "event",
	"status": "starting"
}
\endverbatim
 *
 * which will be followed by a \c started as soon as the associated
 * PeerConnection has been made available to the plugin:
 *
\verbatim
{
	"safiedata" : "event",
	"status": "started"
}
\endverbatim
 *
 * An error instead would provide both an error code and a more verbose
 * description of the cause of the issue:
 *
\verbatim
{
	"safiedata" : "event",
	"error_code" : <numeric ID, check Macros below>,
	"error" : "<error description as a string>"
}
\endverbatim
 *
 * The \c stop request instead has to be formatted as follows:
 *
\verbatim
{
	"request" : "stop"
}
\endverbatim
 *
 */


#include "plugin.h"
#include "refcount.h"

#include <jansson.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <sys/types.h>
#include <fcntl.h>

#include <assert.h>

#include "../debug.h"
#include "../apierror.h"
#include "../config.h"
#include "../mutex.h"
#include "../utils.h"

/* Plugin information */
#define JANUS_SAFIEDATA_VERSION		7
#define JANUS_SAFIEDATA_VERSION_STRING	"0.0.7"
#define JANUS_SAFIEDATA_DESCRIPTION	    "This is a plugin implementing a very simple SafieData service for Janus."
#define JANUS_SAFIEDATA_NAME			"JANUS SafieData plugin"
#define JANUS_SAFIEDATA_AUTHOR			"longshen.yang"
#define JANUS_SAFIEDATA_PACKAGE		    "janus.plugin.safiedata"

#define ARRAY_OF(a)               (sizeof(a) / sizeof(a[0]))
#define MSEC_PER_SEC              (1000)
#define USEC_PER_MSEC             (1000)
#define USEC_PER_SEC              (USEC_PER_MSEC*MSEC_PER_SEC)

#define NO_MEDIA_TIMEOUT          (10*USEC_PER_SEC)     /* 10s */
#define LOG_ALIVE_TIMEOUT         (2*60*USEC_PER_SEC)   /* 2min */



/* Plugin methods */
janus_plugin *create(void);
int janus_safiedata_init(janus_callbacks *callback, const char *config_path);
void janus_safiedata_destroy(void);
int janus_safiedata_get_api_compatibility(void);
int janus_safiedata_get_version(void);
const char *janus_safiedata_get_version_string(void);
const char *janus_safiedata_get_description(void);
const char *janus_safiedata_get_name(void);
const char *janus_safiedata_get_author(void);
const char *janus_safiedata_get_package(void);
void janus_safiedata_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_safiedata_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_safiedata_setup_media(janus_plugin_session *handle);
void janus_safiedata_incoming_rtp(janus_plugin_session *handle, janus_plugin_rtp *packet);
void janus_safiedata_incoming_rtcp(janus_plugin_session *handle, janus_plugin_rtcp *packet);
void janus_safiedata_incoming_data(janus_plugin_session *handle, janus_plugin_data *packet);
void janus_safiedata_data_ready(janus_plugin_session *handle);
void janus_safiedata_slow_link(janus_plugin_session *handle, int uplink, int video);
void janus_safiedata_hangup_media(janus_plugin_session *handle);
void janus_safiedata_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_safiedata_query_session(janus_plugin_session *handle);

/* Plugin setup */
static janus_plugin janus_safiedata_plugin =
	JANUS_PLUGIN_INIT (
		.init = janus_safiedata_init,
		.destroy = janus_safiedata_destroy,

		.get_api_compatibility = janus_safiedata_get_api_compatibility,
		.get_version = janus_safiedata_get_version,
		.get_version_string = janus_safiedata_get_version_string,
		.get_description = janus_safiedata_get_description,
		.get_name = janus_safiedata_get_name,
		.get_author = janus_safiedata_get_author,
		.get_package = janus_safiedata_get_package,

		.create_session = janus_safiedata_create_session,
		.handle_message = janus_safiedata_handle_message,
		.setup_media = janus_safiedata_setup_media,
		.incoming_rtp = janus_safiedata_incoming_rtp,
		.incoming_rtcp = janus_safiedata_incoming_rtcp,
		.incoming_data = janus_safiedata_incoming_data,
		.data_ready = janus_safiedata_data_ready,
		.slow_link = janus_safiedata_slow_link,
		.hangup_media = janus_safiedata_hangup_media,
		.destroy_session = janus_safiedata_destroy_session,
		.query_session = janus_safiedata_query_session,
	);

/* Plugin creator */
janus_plugin *create(void) {
	JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_SAFIEDATA_NAME);
	return &janus_safiedata_plugin;
}

/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

/* Useful stuff */
static volatile gint initialized = 0, stopping = 0;
static gboolean notify_events = TRUE;
static janus_callbacks *gateway = NULL;

static void janus_safiedata_hangup_media_internal(janus_plugin_session *handle);

/* session info */
typedef struct janus_safiedata_session {
	janus_plugin_session *handle;
	gint64 sdp_sessid;
	gint64 sdp_version;
	guint64 data_id;
	gint64 start_time;

	uint32_t bitrate;
	guint16 slowlink_count;

	int seq;
	volatile gboolean started;
	volatile gboolean stopping;
	volatile gint dataready;
	volatile gint hangingup;
	volatile gint destroyed;
	janus_refcount ref;
} janus_safiedata_session;
static GHashTable *sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;

static void janus_safiedata_session_destroy(janus_safiedata_session *session) {
	if(session && g_atomic_int_compare_and_exchange(&session->destroyed, 0, 1))
		janus_refcount_decrease(&session->ref);
}

static void janus_safiedata_session_free(const janus_refcount *session_ref) {
	janus_safiedata_session *session = janus_refcount_containerof(session_ref, janus_safiedata_session, ref);
	/* Remove the reference to the core plugin session */
	janus_refcount_decrease(&session->handle->ref);
	/* This session can be destroyed, free all the resources */
	g_free(session);
}

/* Control thread */
static GThread *handler_thread;
static void *janus_safiedata_handler(void *data);

typedef struct janus_safiedata_message {
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;
} janus_safiedata_message;
static GAsyncQueue *messages = NULL;
static janus_safiedata_message exit_message;
static void janus_safiedata_message_free(janus_safiedata_message *msg) {
	if(!msg || msg == &exit_message)
		return;

	if(msg->handle && msg->handle->plugin_handle) {
		janus_safiedata_session *session = (janus_safiedata_session *)msg->handle->plugin_handle;
		janus_refcount_decrease(&session->ref);
	}
	msg->handle = NULL;

	g_free(msg->transaction);
	msg->transaction = NULL;
	if(msg->message)
		json_decref(msg->message);
	msg->message = NULL;
	if(msg->jsep)
		json_decref(msg->jsep);
	msg->jsep = NULL;

	g_free(msg);
}


/* Error codes */
#define JANUS_SAFIEDATA_ERROR_UNKNOWN_ERROR		499
#define JANUS_SAFIEDATA_ERROR_NO_MESSAGE		460
#define JANUS_SAFIEDATA_ERROR_INVALID_JSON		461
#define JANUS_SAFIEDATA_ERROR_INVALID_REQUEST	462
#define JANUS_SAFIEDATA_ERROR_MISSING_ELEMENT	463
#define JANUS_SAFIEDATA_ERROR_INVALID_ELEMENT	464
#define JANUS_SAFIEDATA_ERROR_ALREADY_RECORDING	465
#define JANUS_SAFIEDATA_ERROR_IO_ERROR			466
#define JANUS_SAFIEDATA_ERROR_LIBOGG_ERROR		467
#define JANUS_SAFIEDATA_ERROR_INVALID_STATE		468


/* Plugin implementation */
int janus_safiedata_init(janus_callbacks *callback, const char *config_path) {
	if(g_atomic_int_get(&stopping)) {
		/* Still stopping from before */
		return -1;
	}
	if(callback == NULL || config_path == NULL) {
		/* Invalid arguments */
		return -1;
	}

	/* Read configuration */
	char filename[255];
	g_snprintf(filename, 255, "%s/%s.jcfg", config_path, JANUS_SAFIEDATA_PACKAGE);
	JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);
	janus_config *config = janus_config_parse(filename);
	if(config == NULL) {
		JANUS_LOG(LOG_WARN, "Couldn't find .jcfg configuration file (%s), trying .cfg\n", JANUS_SAFIEDATA_PACKAGE);
		g_snprintf(filename, 255, "%s/%s.cfg", config_path, JANUS_SAFIEDATA_PACKAGE);
		JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);
		janus_config *config = janus_config_parse(filename);
	}
	if(config != NULL)
		janus_config_print(config);

	sessions = g_hash_table_new_full(
			NULL, NULL, NULL, (GDestroyNotify)janus_safiedata_session_destroy);
	messages = g_async_queue_new_full(
			(GDestroyNotify) janus_safiedata_message_free);

	/* This is the callback we'll need to invoke to contact the Janus core */
	gateway = callback;

	/* Parse configuration */
	if(config != NULL) {
        janus_config_category *config_general = janus_config_get_create(config, NULL, janus_config_type_category, "general");
        janus_config_item *events = janus_config_get(config, config_general, janus_config_type_item, "events");
		if(events != NULL && events->value != NULL)
			notify_events = janus_is_true(events->value);
		if(!notify_events && callback->events_is_enabled()) {
			JANUS_LOG(LOG_WARN, "Notification of events to handlers disabled for %s\n", JANUS_SAFIEDATA_NAME);
		}
		/* Done */
		janus_config_destroy(config);
		config = NULL;
	}

	g_atomic_int_set(&initialized, 1);

	/* Launch the thread that will handle incoming messages */
	GError *error = NULL;
	handler_thread = g_thread_try_new("safiedata handler", janus_safiedata_handler, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the SafieData handler thread...\n", error->code, error->message ? error->message : "??");
		g_error_free(error);
		return -1;
	}

	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SAFIEDATA_NAME);
	return 0;
}

void janus_safiedata_destroy(void) {
	if(!g_atomic_int_get(&initialized))
		return;
	g_atomic_int_set(&stopping, 1);

	g_async_queue_push(messages, &exit_message);
	if(handler_thread != NULL) {
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	/* FIXME We should destroy the sessions cleanly */
	janus_mutex_lock(&sessions_mutex);
	g_hash_table_destroy(sessions);
	sessions = NULL;
	janus_mutex_unlock(&sessions_mutex);
	g_async_queue_unref(messages);
	messages = NULL;

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);
	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SAFIEDATA_NAME);
}

int janus_safiedata_get_api_compatibility(void) {
	/* Important! This is what your plugin MUST always return: don't lie here or bad things will happen */
	return JANUS_PLUGIN_API_VERSION;
}

int janus_safiedata_get_version(void) {
	return JANUS_SAFIEDATA_VERSION;
}

const char *janus_safiedata_get_version_string(void) {
	return JANUS_SAFIEDATA_VERSION_STRING;
}

const char *janus_safiedata_get_description(void) {
	return JANUS_SAFIEDATA_DESCRIPTION;
}

const char *janus_safiedata_get_name(void) {
	return JANUS_SAFIEDATA_NAME;
}

const char *janus_safiedata_get_author(void) {
	return JANUS_SAFIEDATA_AUTHOR;
}

const char *janus_safiedata_get_package(void) {
	return JANUS_SAFIEDATA_PACKAGE;
}

static janus_safiedata_session *janus_safiedata_lookup_session(janus_plugin_session *handle) {
	janus_safiedata_session *session = NULL;
	if (g_hash_table_contains(sessions, handle)) {
		session = (janus_safiedata_session *)handle->plugin_handle;
	}
	return session;
}

void janus_safiedata_create_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		JANUS_LOG(LOG_ERR, "failded to create session as stopping!!\n");
		*error = -1;
		return;
	}
	janus_safiedata_session *session = g_malloc0(sizeof(janus_safiedata_session));
	session->handle = handle;
	session->data_id = janus_random_uint64();
	session->start_time = 0;
	session->slowlink_count = 0;

	session->seq = 0;
	g_atomic_int_set(&session->started, 0);
	g_atomic_int_set(&session->stopping, 0);
	g_atomic_int_set(&session->hangingup, 0);
	g_atomic_int_set(&session->destroyed, 0);
	janus_refcount_init(&session->ref, janus_safiedata_session_free);
	handle->plugin_handle = session;

	janus_mutex_lock(&sessions_mutex);
	g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

void janus_safiedata_destroy_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_safiedata_session *session = janus_safiedata_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No SafieData session associated with this handle...\n");
		*error = -2;
		return;
	}
	JANUS_LOG(LOG_WARN, "Removing SafieData session...\n");
	janus_safiedata_hangup_media_internal(handle);

	g_hash_table_remove(sessions, handle);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

json_t *janus_safiedata_query_session(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		return NULL;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_safiedata_session *session = janus_safiedata_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return NULL;
	}
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);
	/* In the echo test, every session is the same: we just provide some configure info */
	json_t *info = json_object();
	json_object_set_new(info, "state", json_string(session->dataready ? "recording" : "idle"));
	json_object_set_new(info, "id", json_integer(session->data_id));
	json_object_set_new(info, "start_time", json_integer(session->start_time));
	json_object_set_new(info, "hangingup", json_integer(g_atomic_int_get(&session->hangingup)));
	json_object_set_new(info, "destroyed", json_integer(g_atomic_int_get(&session->destroyed)));
	janus_refcount_decrease(&session->ref);
	return info;
}

struct janus_plugin_result *janus_safiedata_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, g_atomic_int_get(&stopping) ? "Shutting down" : "Plugin not initialized", NULL);

	janus_mutex_lock(&sessions_mutex);
	janus_safiedata_session *session = janus_safiedata_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "No session associated with this handle", NULL);
	}
	/* Increase the reference counter for this session: we'll decrease it after we handle the message */
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);


#if 1
	/* Handle request */
	if(message == NULL) {
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "No message??", NULL);
	}
	if(!json_is_object(message)) {
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, "message JSON error: not an object", NULL);
	}

	/* Get the request first */
	static int error_code = 0;
	static char error_cause[512];
	JANUS_VALIDATE_JSON_OBJECT(message, request_parameters,
		error_code, error_cause, TRUE,
		JANUS_SAFIEDATA_ERROR_MISSING_ELEMENT, JANUS_SAFIEDATA_ERROR_INVALID_ELEMENT);
	if(error_code != 0) {
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, error_cause, NULL);
	}

	json_t *request = json_object_get(message, "request");
	const char *request_text = json_string_value(request);
	if(!strcasecmp(request_text, "send_text")) {
		json_t *device_msg_json = json_object_get(message, "text");
		const char *device_msg_str = json_string_value(device_msg_json);
		json_t *event = json_object();
		if(gateway != NULL && g_atomic_int_get(&session->dataready)) {
			janus_plugin_data data = {
				.label = "device_info",
				.protocol = NULL,
				.binary = FALSE,
				.buffer = device_msg_str,
				.length = strlen(device_msg_str)
			};
			gateway->relay_data(session->handle, &data);
			json_object_set_new(event, "send_ok", json_true());
			JANUS_LOG(LOG_VERB, "[safiedata] send_device_msg ok: %s\n", device_msg_str);
		} else {
			json_object_set_new(event, "send_ok", json_false());
			JANUS_LOG(LOG_VERB, "[safiedata] send_device_msg failed: %s\n", device_msg_str);
		}
		return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, event);
	} else if(!strcasecmp(request_text, "info")) {
		/* Get info of session */
		JANUS_LOG(LOG_VERB, "[safiedata] Get info of session\n");

		json_t *event = json_object();
		json_object_set_new(event, "safiedata", json_string("info"));
		if (session->started) {
			json_object_set_new(event, "started", json_true());
			gint64 now = janus_get_monotonic_time();
			json_object_set_new(event, "time_from_start", json_integer(now - session->start_time));
		} else {
			json_object_set_new(event, "started", json_false());
			json_object_set_new(event, "time_from_start", json_integer(0));
		}
		json_object_set_new(event, "stopping", session->stopping ? json_true() : json_false());
		json_object_set_new(event, "hangingup", json_integer(g_atomic_int_get(&session->hangingup)));
		json_object_set_new(event, "destroyed", json_integer(g_atomic_int_get(&session->destroyed)));

		return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, event);
	}
#endif

	janus_safiedata_message *msg = g_malloc(sizeof(janus_safiedata_message));
	msg->handle = handle;
	msg->transaction = transaction;
	msg->message = message;
	msg->jsep = jsep;
	g_async_queue_push(messages, msg);

	/* All the requests to this plugin are handled asynchronously */
	return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
}

void janus_safiedata_setup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC data is setuped\n", JANUS_SAFIEDATA_PACKAGE, handle);

	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_mutex_lock(&sessions_mutex);
	janus_safiedata_session *session = janus_safiedata_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	if(g_atomic_int_get(&session->destroyed)) {
		janus_mutex_unlock(&sessions_mutex);
		return;
	}
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);
	g_atomic_int_set(&session->hangingup, 0);
	/* Only start recording this peer when we get this event */
	session->start_time = janus_get_monotonic_time();
	g_atomic_int_set(&session->started, 1);

	/* Prepare JSON event */
	json_t *event = json_object();
	json_object_set_new(event, "safiedata", json_string("event"));
	json_object_set_new(event, "status", json_string("started"));
	int ret = gateway->push_event(handle, &janus_safiedata_plugin, NULL, event, NULL);
	JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
	json_decref(event);
	janus_refcount_decrease(&session->ref);
}

void janus_safiedata_incoming_rtp(janus_plugin_session *handle, janus_plugin_rtp *packet) {
	/* We don't do audio/video */
}

void janus_safiedata_incoming_rtcp(janus_plugin_session *handle, janus_plugin_rtcp *packet) {
	/* We don't do audio/video */
}

void janus_safiedata_incoming_data(janus_plugin_session *handle, janus_plugin_data *packet) {
	JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC data is comming\n", JANUS_SAFIEDATA_PACKAGE, handle);

	if(handle == NULL || g_atomic_int_get(&handle->stopped) || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	/* Simple echo test */
	if(gateway) {
		janus_safiedata_session *session = (janus_safiedata_session *)handle->plugin_handle;
		if(!session) {
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			return;
		}
		if(g_atomic_int_get(&session->destroyed))
			return;
		if(packet->buffer == NULL || packet->length == 0)
			return;
		char *label = packet->label;
		char *buf = packet->buffer;
		uint16_t len = packet->length;
		if(packet->binary) {
			JANUS_LOG(LOG_WARN, "Got a binary DataChannel message (label=%s, %d bytes) to bounce back\n", label, len);

			// todo send to safie app
			/* Binary data, shoot back as it is */
			gateway->relay_data(handle, packet);
			return;
		}
		/* Text data */
		char *text = g_malloc(len+1);
		memcpy(text, buf, len);
		*(text+len) = '\0';
		JANUS_LOG(LOG_VERB, "Got a DataChannel message (label=%s, %zu bytes) to bounce back: %s\n", label, strlen(text), text);

		/* We send back the text to safie app */
		json_t *event = json_object();
		json_object_set_new(event, "safiedata", json_string("data"));
		json_object_set_new(event, "label", json_string(label));
		json_object_set_new(event, "text", json_string(text));
		int ret = gateway->push_event(handle, &janus_safiedata_plugin, NULL, event, NULL);
		JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
		json_decref(event);
		g_free(text);
	}
}

void janus_safiedata_data_ready(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC data is ready\n", JANUS_SAFIEDATA_PACKAGE, handle);

	if(handle == NULL || g_atomic_int_get(&handle->stopped) ||
			g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized) || !gateway)
		return;
	/* Data channels are writable */
	/* Data channels are writable: we shouldn't send any datachannel message before this happens */
	janus_safiedata_session *session = (janus_safiedata_session *)handle->plugin_handle;
	if(!session || g_atomic_int_get(&session->destroyed) || g_atomic_int_get(&session->hangingup))
		return;
	if(g_atomic_int_compare_and_exchange(&session->dataready, 0, 1)) {
		JANUS_LOG(LOG_WARN, "[%s-%p] Data channel available\n", JANUS_SAFIEDATA_PACKAGE, handle);
	}
}

void janus_safiedata_slow_link(janus_plugin_session *handle, int uplink, int video) {
	/* The core is informing us that our peer got or sent too many NACKs, are we pushing media too hard? */
	if(handle == NULL || g_atomic_int_get(&handle->stopped) || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_safiedata_session *session = (janus_safiedata_session *)handle->plugin_handle;
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	if(g_atomic_int_get(&session->destroyed))
		return;
	session->slowlink_count++;
	JANUS_LOG(LOG_WARN, "slowlink_count=%d, Getting a lot of NACKs (slow %s) for %s\n",
			session->slowlink_count, uplink ? "uplink" : "downlink", video ? "video" : "audio");
}

void janus_safiedata_hangup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "[%s-%p] No WebRTC media anymore\n", JANUS_SAFIEDATA_PACKAGE, handle);
	janus_mutex_lock(&sessions_mutex);
	janus_safiedata_hangup_media_internal(handle);
	janus_mutex_unlock(&sessions_mutex);
}

static void janus_safiedata_hangup_media_internal(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_safiedata_session *session = janus_safiedata_lookup_session(handle);
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	g_atomic_int_set(&session->started, 0);
	g_atomic_int_set(&session->dataready, 0);
	if(g_atomic_int_get(&session->destroyed))
		return;
	if(!g_atomic_int_compare_and_exchange(&session->hangingup, 0, 1))
		return;

	g_atomic_int_set(&session->hangingup, 0);
}

/* Thread to handle incoming messages */
static void *janus_safiedata_handler(void *data) {
	JANUS_LOG(LOG_VERB, "SafieData handler thread started\n");
	janus_safiedata_message *msg = NULL;
	int error_code = 0;
	char error_cause[512];
	json_t *root = NULL;
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		msg = g_async_queue_pop(messages);
		if(msg == &exit_message)
			break;
		if(msg->handle == NULL) {
			janus_safiedata_message_free(msg);
			continue;
		}
		janus_mutex_lock(&sessions_mutex);
		janus_safiedata_session *session = janus_safiedata_lookup_session(msg->handle);
		if(!session) {
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			janus_safiedata_message_free(msg);
			continue;
		}
		if(g_atomic_int_get(&session->destroyed)) {
			janus_mutex_unlock(&sessions_mutex);
			janus_safiedata_message_free(msg);
			continue;
		}
		janus_mutex_unlock(&sessions_mutex);
		/* Handle request */
		error_code = 0;
		root = msg->message;
		if(msg->message == NULL) {
			JANUS_LOG(LOG_ERR, "No message??\n");
			error_code = JANUS_SAFIEDATA_ERROR_NO_MESSAGE;
			g_snprintf(error_cause, 512, "%s", "No message??");
			goto error;
		}
		if(!json_is_object(root)) {
			JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
			error_code = JANUS_SAFIEDATA_ERROR_INVALID_JSON;
			g_snprintf(error_cause, 512, "JSON error: not an object");
			goto error;
		}
		/* Get the request first */
		JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
			error_code, error_cause, TRUE,
			JANUS_SAFIEDATA_ERROR_MISSING_ELEMENT, JANUS_SAFIEDATA_ERROR_INVALID_ELEMENT);
		if(error_code != 0)
			goto error;
		json_t *request = json_object_get(root, "request");
		const char *request_text = json_string_value(request);
		json_t *event = NULL;
		gboolean sdp_update = FALSE;
		if(json_object_get(msg->jsep, "update") != NULL)
			sdp_update = json_is_true(json_object_get(msg->jsep, "update"));
		if(!strcasecmp(request_text, "record")) {
			JANUS_LOG(LOG_VERB, "Starting new recording\n");
			session->seq = 0;

			/* Done: now wait for the setup_media callback to be called */
			event = json_object();
			json_object_set_new(event, "safiedata", json_string("event"));
			json_object_set_new(event, "status", json_string(g_atomic_int_get(&session->started) ? "started" : "starting"));
			/* Also notify event handlers */
			if(notify_events && gateway->events_is_enabled()) {
				json_t *info = json_object();
				json_object_set_new(info, "event", json_string("starting"));
				gateway->notify_event(&janus_safiedata_plugin, session->handle, info);
			}
		} else {
			JANUS_LOG(LOG_ERR, "Unknown request '%s'\n", request_text);
			error_code = JANUS_SAFIEDATA_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
			goto error;
		}

		/* Prepare JSON event */
		JANUS_LOG(LOG_VERB, "[safiedata]Preparing JSON event as a reply\n");
		/* Any SDP to handle? */
		const char *msg_sdp_type = json_string_value(json_object_get(msg->jsep, "type"));
		const char *msg_sdp = json_string_value(json_object_get(msg->jsep, "sdp"));
		if(!msg_sdp) {
			int ret = gateway->push_event(msg->handle, &janus_safiedata_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> %d (%s)\n", ret, janus_get_api_error(ret));
			json_decref(event);
		} else {
			JANUS_LOG(LOG_VERB, "This is involving a negotiation (%s) as well:\n%s\n", msg_sdp_type, msg_sdp);
			const char *type = NULL;
			if(!strcasecmp(msg_sdp_type, "offer"))
				type = "answer";
			if(!strcasecmp(msg_sdp_type, "answer"))
				type = "offer";
			if(sdp_update) {
				/* Renegotiation: make sure the user provided an offer, and send answer */
				JANUS_LOG(LOG_VERB, "Request to update existing connection\n");
				session->sdp_version++;		/* This needs to be increased when it changes */
			} else {
				/* New PeerConnection */
				session->sdp_version = 1;	/* This needs to be increased when it changes */
				session->sdp_sessid = janus_get_real_time();
			}
			/* Fill the SDP template and use that as our answer */
			char sdp[1024];
			g_snprintf(sdp, 1024, 
				"v=0\r\n" \
				"o=- %"SCNu64" %"SCNu64" IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
				"s=SafieData %"SCNu64"\r\n"						/* SafieData Sesion ID */ \
				"t=0 0\r\n" \
				"c=IN IP4 1.1.1.1\r\n" \
				"a=sendrecv\r\n"					/* This plugin doesn't send any frames */
				,
				session->sdp_sessid,
				session->sdp_version,
				session->data_id			/* Recording ID */
				);

#ifdef HAVE_SCTP
			gboolean has_data_channel = (strstr(msg_sdp, "DTLS/SCTP") && !strstr(msg_sdp, " 0 DTLS/SCTP") &&
				!strstr(msg_sdp, " 0 UDP/DTLS/SCTP")) ? 1 : 0;	/* FIXME This is a really hacky way of checking... */
			if(has_data_channel) {
				/* Add data line */
				gchar buffer[512];
				memset(buffer, 0, 512);
				g_snprintf(buffer, 512,
					"m=application 1 UDP/DTLS/SCTP webrtc-datachannel\r\n"
					"c=IN IP4 1.1.1.1\r\n"
					"a=sctp-port:5000\r\n");
				g_strlcat(sdp, buffer, 2048);
			}
#endif
			/* Did the peer negotiate video? */
			if(strstr(msg_sdp, "m=video") != NULL) {
				/* If so, reject it */
				g_strlcat(sdp, "m=video 0 RTP/SAVPF 0\r\n", 1024);
			}
			json_t *jsep = json_pack("{ssss}", "type", type, "sdp", sdp);
			/* How long will the Janus core take to push the event? */
			g_atomic_int_set(&session->hangingup, 0);
			gint64 start = janus_get_monotonic_time();
			int res = gateway->push_event(msg->handle, &janus_safiedata_plugin, msg->transaction, event, jsep);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (took %"SCNu64" us)\n", res, janus_get_monotonic_time()-start);
			json_decref(event);
			json_decref(jsep);
			if(res != JANUS_OK) {
				/* TODO Failed to negotiate? We should remove this participant */
			}
		}

		/* Tear down the session if we're done */
		if(g_atomic_int_get(&session->stopping))
			gateway->end_session(session->handle);
		janus_safiedata_message_free(msg);

		continue;

error:
		{
			/* Prepare JSON error event */
			json_t *event = json_object();
			json_object_set_new(event, "safiedata", json_string("event"));
			json_object_set_new(event, "error_code", json_integer(error_code));
			json_object_set_new(event, "error", json_string(error_cause));
			int ret = gateway->push_event(msg->handle, &janus_safiedata_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
			json_decref(event);
			janus_safiedata_message_free(msg);
		}
	}
	JANUS_LOG(LOG_VERB, "Leaving SafieData handler thread\n");
	return NULL;
}

