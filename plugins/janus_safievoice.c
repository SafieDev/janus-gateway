/*! \file   janus_safievoice.c
 * \author Lorenzo Miniero <lorenzo@meetecho.com>
 * \copyright GNU General Public License v3
 * \brief  Janus SafieVoice plugin
 * \details Check the \ref safievoice for more details.
 *
 * \ingroup plugins
 * \ref plugins
 *
 * \page safievoice SafieVoice plugin documentation
 * This is a plugin implementing a very simple SafieVoice service
 * for Janus, specifically recording Opus streams. This means that it replies
 * by providing in the SDP only support for Opus, and disabling video.
 * When a peer contacts the plugin, the plugin starts recording the audio
 * frames it receives and, after 10 seconds, it shuts the PeerConnection
 * down and returns an URL to the recorded file.
 *
 * Since an URL is returned, the plugin allows you to configure where the
 * recordings whould be stored (e.g., a folder in your web server, writable
 * by the plugin) and the base path to use when returning URLs (e.g.,
 * /my/recordings/ or http://www.example.com/my/recordings).
 *
 * By default the plugin saves the recordings in the \c html folder of
 * this project, meaning that it can work out of the box with the SafieVoice
 * demo we provide in the same folder.
 *
 * \section vmailapi SafieVoice API
 *
 * The SafieVoice API supports just two requests, \c record and \c stop
 * and they're both asynchronous, which means all responses (successes
 * and errors) will be delivered as events with the same transaction.
 *
 * \c record will instruct the plugin to start recording, while \c stop
 * will make the recording stop before the 10 seconds have passed.
 * Never send a JSEP offer with any of these requests: it's always the
 * SafieVoice plugin that originates a JSEP offer, in response to a
 * \c record request, which means your application will only have to
 * send a JSEP answer when that happens.
 *
 * The \c record request has to be formatted as follows:
 *
\verbatim
{
	"request" : "record"
}
\endverbatim
 *
 * A successful request will result in an \c starting status event:
 *
\verbatim
{
	"safievoice" : "event",
	"status": "starting"
}
\endverbatim
 *
 * which will be followed by a \c started as soon as the associated
 * PeerConnection has been made available to the plugin:
 *
\verbatim
{
	"safievoice" : "event",
	"status": "started"
}
\endverbatim
 *
 * An error instead would provide both an error code and a more verbose
 * description of the cause of the issue:
 *
\verbatim
{
	"safievoice" : "event",
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
 * If the plugin detects a loss of the associated PeerConnection, whether
 * as a result of a \c stop request or because the 10 seconds passed, a
 * \c done status notification is triggered to inform the application
 * the recording session is over, together with the path to the
 * recording file itself:
 *
\verbatim
{
	"safievoice" : "event",
	"status" : "done",
	"recording : "<path to the .opus file>"
}
\endverbatim
 */

//#define JANUS_USE_PLUSE_AUDIO
//#define SAFIE_WEARABLE_DEBUG_PRINT_LATENCY
//#define DUMP_RAW_PCM

#include "plugin.h"
#include "refcount.h"

#include <jansson.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <sys/types.h>
#include <fcntl.h>
#include <opus/opus.h>

#if defined(JANUS_USE_PLUSE_AUDIO)
#include <pulse/simple.h>
#include <pulse/error.h>
#else
#include <alsa/asoundlib.h>
#endif

#include "../debug.h"
#include "../apierror.h"
#include "../config.h"
#include "../mutex.h"
#include "../rtp.h"
#include "../utils.h"

/* Plugin information */
#define JANUS_SAFIEVOICE_VERSION			7
#define JANUS_SAFIEVOICE_VERSION_STRING	"0.0.7"
#define JANUS_SAFIEVOICE_DESCRIPTION		"This is a plugin implementing a very simple SafieVoice service for Janus, recording Opus streams."
#define JANUS_SAFIEVOICE_NAME			"JANUS SafieVoice plugin"
#define JANUS_SAFIEVOICE_AUTHOR			"Meetecho s.r.l."
#define JANUS_SAFIEVOICE_PACKAGE			"janus.plugin.safievoice"

#define ARRAY_OF(a)               (sizeof(a) / sizeof(a[0]))
#define MSEC_PER_SEC              (1000)
#define USEC_PER_SEC              (1000*MSEC_PER_SEC)

#define PLAYER_RESPONSE_TIMEOUT   (5*USEC_PER_SEC)      /* 5s */
#define NO_MEDIA_TIMEOUT          (10*USEC_PER_SEC)     /* 10s */
#define LOG_ALIVE_TIMEOUT         (2*60*USEC_PER_SEC)   /* 2min */

#define SAMPLE_RATE           (48000)
#define CHANNELS              (2)
#define SAMPLE_SIZE           sizeof(opus_int16)
#define BUF_SIZE_PER_SEC      (SAMPLE_RATE*CHANNELS*sizeof(opus_int16))

#define FRAME_SAMPLE_NUM         (20*SAMPLE_RATE/MSEC_PER_SEC)                      /* 20 ms/frame */
#define USEC_PER_FRAME           ((USEC_PER_SEC * FRAME_SAMPLE_NUM) / SAMPLE_RATE)
#define PCM_FRAME_BUF_SIZE       (FRAME_SAMPLE_NUM*CHANNELS*sizeof(opus_int16))

#define MIN_SAMPLE_NUM_TO_DECODE  (120*SAMPLE_RATE/1000)     /* to decode opus: maximum packet duration (120ms; 5760 for 48kHz) */
#define MIN_BUFFER_SIZE_TO_DECODE (MIN_SAMPLE_NUM_TO_DECODE*CHANNELS*sizeof(opus_int16))

#define PLAYBACK_LATENCY_IN_USEC               (500000)  /* 500 ms */
#define PLAYBACK_LATENCY_BUF_SIZE              (BUF_SIZE_PER_SEC * ((double)PLAYBACK_LATENCY_IN_USEC/USEC_PER_SEC))

#define LATENCY_CONTROL_START_THRESHOLD          (1500000) /* 1.5 s */
#define LATENCY_CONTROL_EMERGENCY_THRESHOLD      (3000000) /* 3 s */
#define LATENCY_LEVEL_NUM          (ARRAY_OF(skip_params))
#define LATENCY_PER_LEVEL             ((LATENCY_CONTROL_EMERGENCY_THRESHOLD - LATENCY_CONTROL_START_THRESHOLD) / LATENCY_LEVEL_NUM)
#define GET_LATENCY_LEVEL(latency)    ((((latency) - LATENCY_CONTROL_START_THRESHOLD) / LATENCY_PER_LEVEL) % LATENCY_LEVEL_NUM)

static struct janus_safievoice_latency_skip_param {
    const int skip_num;
    const int skip_base;
    const int clean_buffer_bum;
} skip_params[] = {
    {1, 20, 0}, /* level 0 */
    {1, 15, 0},
    {1, 10, 0},
    {1, 6,  0},
    {1, 5,  0},
    {1, 4,  0}, /* kill section */
};


#define RECORD_SAMPLE_RATE           (8000)
#define RECORD_CHANNEL_NUM           (1)
#define RECORD_MSEC_PER_FRAME        (60)
#define RECORD_TIMESTAMP_SAMPLE_NUM  (RECORD_MSEC_PER_FRAME*48000/MSEC_PER_SEC)    /* 60 ms/frame */
#define RECORD_FRAME_SAMPLE_NUM      (RECORD_MSEC_PER_FRAME*RECORD_SAMPLE_RATE/MSEC_PER_SEC)    /* 60 ms/frame */
#define RECORD_PCM_FRAME_BUF_SIZE    (RECORD_FRAME_SAMPLE_NUM*RECORD_CHANNEL_NUM*sizeof(opus_int16))
#define RECORD_OPUS_FRAME_BUF_SIZE   (1000)
#define RECORD_PCM_BUF_SIZE_PER_SEC  (RECORD_SAMPLE_RATE*RECORD_CHANNEL_NUM*sizeof(opus_int16))
#define RECORD_LATENCY_IN_USEC       (200000)  /* 200 ms */
#define RECORD_LATENCY_PCM_BUF_SIZE  (RECORD_PCM_BUF_SIZE_PER_SEC * ((double)RECORD_LATENCY_IN_USEC/USEC_PER_SEC))

/* Plugin methods */
janus_plugin *create(void);
int janus_safievoice_init(janus_callbacks *callback, const char *config_path);
void janus_safievoice_destroy(void);
int janus_safievoice_get_api_compatibility(void);
int janus_safievoice_get_version(void);
const char *janus_safievoice_get_version_string(void);
const char *janus_safievoice_get_description(void);
const char *janus_safievoice_get_name(void);
const char *janus_safievoice_get_author(void);
const char *janus_safievoice_get_package(void);
void janus_safievoice_create_session(janus_plugin_session *handle, int *error);
struct janus_plugin_result *janus_safievoice_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep);
void janus_safievoice_setup_media(janus_plugin_session *handle);
void janus_safievoice_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_safievoice_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len);
void janus_safievoice_hangup_media(janus_plugin_session *handle);
void janus_safievoice_destroy_session(janus_plugin_session *handle, int *error);
json_t *janus_safievoice_query_session(janus_plugin_session *handle);

/* Plugin setup */
static janus_plugin janus_safievoice_plugin =
	JANUS_PLUGIN_INIT (
		.init = janus_safievoice_init,
		.destroy = janus_safievoice_destroy,

		.get_api_compatibility = janus_safievoice_get_api_compatibility,
		.get_version = janus_safievoice_get_version,
		.get_version_string = janus_safievoice_get_version_string,
		.get_description = janus_safievoice_get_description,
		.get_name = janus_safievoice_get_name,
		.get_author = janus_safievoice_get_author,
		.get_package = janus_safievoice_get_package,

		.create_session = janus_safievoice_create_session,
		.handle_message = janus_safievoice_handle_message,
		.setup_media = janus_safievoice_setup_media,
		.incoming_rtp = janus_safievoice_incoming_rtp,
		.incoming_rtcp = janus_safievoice_incoming_rtcp,
		.hangup_media = janus_safievoice_hangup_media,
		.destroy_session = janus_safievoice_destroy_session,
		.query_session = janus_safievoice_query_session,
	);

/* Plugin creator */
janus_plugin *create(void) {
	JANUS_LOG(LOG_VERB, "%s created!\n", JANUS_SAFIEVOICE_NAME);
	return &janus_safievoice_plugin;
}

/* Parameter validation */
static struct janus_json_parameter request_parameters[] = {
	{"request", JSON_STRING, JANUS_JSON_PARAM_REQUIRED}
};

/* Useful stuff */
static volatile gint initialized = 0, stopping = 0;
static gboolean notify_events = TRUE;
static janus_callbacks *gateway = NULL;

static void janus_safievoice_hangup_media_internal(janus_plugin_session *handle);

/* session info */
typedef struct janus_safievoice_session {
	janus_plugin_session *handle;
	gint64 sdp_sessid;
	gint64 sdp_version;
	guint64 recording_id;
	gint64 start_time;
#if defined(DUMP_RAW_PCM)
	char *opus_filename;
	int opus_fd;
	char *pcm_filename;
	int pcm_fd;
#endif
	OpusDecoder *decoder;
	int opus_pt;
	int record_seq;
	gint64 record_start_time;
    gint64 first_rtp_time;
    gint64 last_rtp_time;
    gint64 total_rtp_cnt;
    gint64 skiped_rtp_cnt;
    gint64 timeover_incoming_rtp_cnt;
	gint64 cur_latency;
	int seq;
	gboolean started;
	gboolean stopping;
	volatile gint hangingup;
	volatile gint destroyed;
	janus_refcount ref;
} janus_safievoice_session;
static GHashTable *sessions;
static janus_mutex sessions_mutex = JANUS_MUTEX_INITIALIZER;

static void janus_safievoice_session_destroy(janus_safievoice_session *session) {
	if(session && g_atomic_int_compare_and_exchange(&session->destroyed, 0, 1))
		janus_refcount_decrease(&session->ref);
}

static void janus_safievoice_session_free(const janus_refcount *session_ref) {
	janus_safievoice_session *session = janus_refcount_containerof(session_ref, janus_safievoice_session, ref);
	/* Remove the reference to the core plugin session */
	janus_refcount_decrease(&session->handle->ref);
	/* This session can be destroyed, free all the resources */
	g_free(session);
}

/* Control thread */
static GThread *handler_thread;
static void *janus_safievoice_handler(void *data);

typedef struct janus_safievoice_message {
	janus_plugin_session *handle;
	char *transaction;
	json_t *message;
	json_t *jsep;
} janus_safievoice_message;
static GAsyncQueue *messages = NULL;
static janus_safievoice_message exit_message;
static void janus_safievoice_message_free(janus_safievoice_message *msg) {
	if(!msg || msg == &exit_message)
		return;

	if(msg->handle && msg->handle->plugin_handle) {
		janus_safievoice_session *session = (janus_safievoice_session *)msg->handle->plugin_handle;
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

/* Player thread */
static GThread *player_thread;
static void *janus_safievoice_player(void *data);

typedef struct janus_safievoice_player_request_message {
    long int latency;      // for debugging
    uint32_t hl_timestamp; // for debugging
    gint64 rtp_time;       // for debugging
    uint32_t sample_num;
    opus_int16* pcm_buf;
} janus_safievoice_player_request_message;
static GAsyncQueue *player_request_queue = NULL;
static janus_safievoice_player_request_message player_exit_message;
static janus_safievoice_player_request_message player_open_message;
static janus_safievoice_player_request_message player_close_message;
static gint32 play_buffer_alloc_cnt = 0;
static gint32 play_buffer_free_cnt = 0;
static janus_safievoice_player_request_message* janus_safievoice_player_request_message_alloc(void) {
    janus_safievoice_player_request_message* msg = g_malloc(
		sizeof(janus_safievoice_player_request_message) + MIN_BUFFER_SIZE_TO_DECODE);
    msg->pcm_buf = (opus_int16*)&msg[1];
	play_buffer_alloc_cnt ++;
    return msg;
}
static void janus_safievoice_player_request_message_free(janus_safievoice_player_request_message *msg) {
	if(!msg ||
        msg == &player_exit_message ||
        msg == &player_open_message ||
        msg == &player_close_message) {
		return;
	}

	play_buffer_free_cnt ++;
	g_free(msg);
}

typedef struct janus_safievoice_player_response_message {
    uint32_t dummy;
} janus_safievoice_player_response_message;

static GAsyncQueue *player_response_queue = NULL;
static janus_safievoice_player_response_message player_close_succeeded;
static janus_safievoice_player_response_message player_open_succeeded;
static janus_safievoice_player_response_message player_open_failed;

/* Recorder thread */
static GThread *recorder_thread;
static void *janus_safievoice_recorder(void *data);

typedef struct janus_safievoice_recorder_request_message {
	uint32_t dummy;
} janus_safievoice_recorder_request_message;
static GAsyncQueue *recorder_request_queue = NULL;
static janus_safievoice_recorder_request_message recorder_exit_message;
static janus_safievoice_recorder_request_message recorder_open_message;
static janus_safievoice_recorder_request_message recorder_close_message;
static void janus_safievoice_recorder_request_message_free(
				janus_safievoice_recorder_request_message *msg) {
	if(!msg || 
		msg == &recorder_exit_message ||
		msg == &recorder_open_message ||
		msg == &recorder_close_message) {
		return;
	}
	g_free(msg);
}


typedef struct janus_safievoice_rtp_relay_packet {
	janus_rtp_header *data;
	gint length;
} janus_safievoice_rtp_relay_packet;

typedef struct janus_safievoice_record_message {
    gint64 start_time;      // for debugging
	gint64 start_encode_time;      // for debugging
	gint64 recorded_time;   // for debugging
	gint64 encoded_time;   // for debugging
    uint32_t sample_num;
    opus_int16* pcm_buf;
	janus_safievoice_rtp_relay_packet* outpkt;
} janus_safievoice_record_message;

static gint32 record_buffer_alloc_cnt = 0;
static gint32 record_buffer_free_cnt = 0;

static janus_safievoice_record_message* janus_safievoice_record_message_alloc(void) {
	gint64 start_time = janus_get_monotonic_time();

    janus_safievoice_record_message* msg = g_malloc0(
		sizeof(janus_safievoice_record_message) 
		+ RECORD_PCM_FRAME_BUF_SIZE
		+ sizeof(janus_safievoice_rtp_relay_packet)
		+ RECORD_OPUS_FRAME_BUF_SIZE);

    msg->pcm_buf      = (opus_int16*)(((char*)msg) + sizeof(janus_safievoice_record_message));
	msg->outpkt       = (janus_safievoice_rtp_relay_packet*)(((char*)msg->pcm_buf) + RECORD_PCM_FRAME_BUF_SIZE);
	msg->outpkt->data = (janus_rtp_header*)(((char*)msg->outpkt) + sizeof(janus_safievoice_rtp_relay_packet));

	msg->start_time    = start_time;
	msg->recorded_time = 0;
	msg->start_encode_time = 0;
	msg->encoded_time  = 0;
	msg->sample_num    = RECORD_FRAME_SAMPLE_NUM;

	record_buffer_alloc_cnt ++;

    return msg;
}

static janus_safievoice_record_message encoder_exit_message;
static janus_safievoice_record_message uploader_exit_message;
static void janus_safievoice_record_message_free(janus_safievoice_record_message *msg) {
	if(!msg ||
		msg == &encoder_exit_message ||
		msg == &uploader_exit_message
		) {
		return;
	}
	record_buffer_free_cnt ++;
	g_free(msg);
}

/* encorder thread */
static GThread *encoder_thread;
static void *janus_safievoice_encoder(void *data);
static GAsyncQueue *encoder_request_queue = NULL;

/* encorder thread */
static GThread *uploader_thread;
static void *janus_safievoice_uploader(void *data);
static GAsyncQueue *uploader_request_queue = NULL;

/* SDP offer/answer template */
#define sdp_template \
		"v=0\r\n" \
		"o=- %"SCNu64" %"SCNu64" IN IP4 127.0.0.1\r\n"	/* We need current time here */ \
		"s=SafieVoice %"SCNu64"\r\n"						/* SafieVoice recording ID */ \
		"t=0 0\r\n" \
		"m=audio 1 RTP/SAVPF %d\r\n"		/* Opus payload type */ \
		"c=IN IP4 1.1.1.1\r\n" \
		"a=rtpmap:%d opus/48000/2\r\n"		/* Opus payload type */ \
		"a=sendrecv\r\n"					/* This plugin doesn't send any frames */


/* Error codes */
#define JANUS_SAFIEVOICE_ERROR_UNKNOWN_ERROR		499
#define JANUS_SAFIEVOICE_ERROR_NO_MESSAGE		460
#define JANUS_SAFIEVOICE_ERROR_INVALID_JSON		461
#define JANUS_SAFIEVOICE_ERROR_INVALID_REQUEST	462
#define JANUS_SAFIEVOICE_ERROR_MISSING_ELEMENT	463
#define JANUS_SAFIEVOICE_ERROR_INVALID_ELEMENT	464
#define JANUS_SAFIEVOICE_ERROR_ALREADY_RECORDING	465
#define JANUS_SAFIEVOICE_ERROR_IO_ERROR			466
#define JANUS_SAFIEVOICE_ERROR_LIBOGG_ERROR		467
#define JANUS_SAFIEVOICE_ERROR_INVALID_STATE		468


/* Plugin implementation */
int janus_safievoice_init(janus_callbacks *callback, const char *config_path) {
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
	g_snprintf(filename, 255, "%s/%s.jcfg", config_path, JANUS_SAFIEVOICE_PACKAGE);
	JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);
	janus_config *config = janus_config_parse(filename);
	if(config == NULL) {
		JANUS_LOG(LOG_WARN, "Couldn't find .jcfg configuration file (%s), trying .cfg\n", JANUS_SAFIEVOICE_PACKAGE);
		g_snprintf(filename, 255, "%s/%s.cfg", config_path, JANUS_SAFIEVOICE_PACKAGE);
		JANUS_LOG(LOG_VERB, "Configuration file: %s\n", filename);
		janus_config *config = janus_config_parse(filename);
	}
	if(config != NULL)
		janus_config_print(config);

	sessions = g_hash_table_new_full(
			NULL, NULL, NULL, (GDestroyNotify)janus_safievoice_session_destroy);
	messages = g_async_queue_new_full(
			(GDestroyNotify) janus_safievoice_message_free);

	player_request_queue = g_async_queue_new_full(
			(GDestroyNotify) janus_safievoice_player_request_message_free);
	player_response_queue = g_async_queue_new();

	recorder_request_queue = g_async_queue_new_full(
			(GDestroyNotify) janus_safievoice_recorder_request_message_free);

	encoder_request_queue = g_async_queue_new_full(
			(GDestroyNotify) janus_safievoice_record_message_free);

	uploader_request_queue = g_async_queue_new_full(
			(GDestroyNotify) janus_safievoice_record_message_free);

	/* This is the callback we'll need to invoke to contact the Janus core */
	gateway = callback;

	/* Parse configuration */
	if(config != NULL) {
        janus_config_category *config_general = janus_config_get_create(config, NULL, janus_config_type_category, "general");
        janus_config_item *events = janus_config_get(config, config_general, janus_config_type_item, "events");
		if(events != NULL && events->value != NULL)
			notify_events = janus_is_true(events->value);
		if(!notify_events && callback->events_is_enabled()) {
			JANUS_LOG(LOG_WARN, "Notification of events to handlers disabled for %s\n", JANUS_SAFIEVOICE_NAME);
		}
		/* Done */
		janus_config_destroy(config);
		config = NULL;
	}

	g_atomic_int_set(&initialized, 1);

	/* Launch the thread that will handle incoming messages */
	GError *error = NULL;
	handler_thread = g_thread_try_new("safievoice handler", janus_safievoice_handler, NULL, &error);
	if(error != NULL) {
		g_atomic_int_set(&initialized, 0);
		JANUS_LOG(LOG_ERR, "Got error %d (%s) trying to launch the SafieVoice handler thread...\n", error->code, error->message ? error->message : "??");
		return -1;
	}

	/* Launch the thread that will handle play back */
	player_thread = g_thread_try_new("safievoice player", 
						janus_safievoice_player, NULL, &error);
	if(error != NULL) {
        g_atomic_int_set(&initialized, 0); // @todo
        JANUS_LOG(LOG_ERR, 
			"Got error %d (%s) trying to launch the SafieVoice player thread...\n", 
			error->code, error->message ? error->message : "??");
        /* @todo:join handler threads -> will janus_safievoice_destroy() be called?  */
        return -1;
    }

	/* Launch the thread that will handle record */
	recorder_thread = g_thread_try_new("safievoice recorder",
						janus_safievoice_recorder, NULL, &error);
	if(error != NULL) {
        g_atomic_int_set(&initialized, 0); // @todo
        JANUS_LOG(LOG_ERR,
			"Got error %d (%s) trying to launch the SafieVoice recorder thread...\n", 
			error->code, error->message ? error->message : "??");
        /* @todo:join handler threads -> will janus_safievoice_destroy() be called?  */
        return -1;
    }

	/* Launch the thread that will handle encode */
	encoder_thread = g_thread_try_new("safievoice encorder",
						janus_safievoice_encoder, NULL, &error);
	if(error != NULL) {
        g_atomic_int_set(&initialized, 0); // @todo
        JANUS_LOG(LOG_ERR,
			"Got error %d (%s) trying to launch the SafieVoice encoder thread...\n", 
			error->code, error->message ? error->message : "??");
        /* @todo:join handler threads -> will janus_safievoice_destroy() be called?  */
        return -1;
    }

	/* Launch the thread that will handle upload */
	uploader_thread = g_thread_try_new("safievoice uploader",
						janus_safievoice_uploader, NULL, &error);
	if(error != NULL) {
        g_atomic_int_set(&initialized, 0); // @todo
        JANUS_LOG(LOG_ERR,
			"Got error %d (%s) trying to launch the SafieVoice uploader thread...\n", 
			error->code, error->message ? error->message : "??");
        /* @todo:join handler threads -> will janus_safievoice_destroy() be called?  */
        return -1;
    }

	JANUS_LOG(LOG_INFO, "%s initialized!\n", JANUS_SAFIEVOICE_NAME);
	return 0;
}

void janus_safievoice_destroy(void) {
	if(!g_atomic_int_get(&initialized))
		return;
	g_atomic_int_set(&stopping, 1);

	g_async_queue_push(messages, &exit_message);
	if(handler_thread != NULL) {
		g_thread_join(handler_thread);
		handler_thread = NULL;
	}

	g_async_queue_push(player_request_queue, &player_exit_message);
	if(player_thread != NULL) {
        g_thread_join(player_thread);
        player_thread = NULL;
    }

	g_async_queue_push(recorder_request_queue, &recorder_exit_message);
	if(recorder_thread != NULL) {
        g_thread_join(recorder_thread);
        recorder_thread = NULL;
    }

	g_async_queue_push(encoder_request_queue, &encoder_exit_message);
	if(encoder_thread != NULL) {
        g_thread_join(encoder_thread);
        encoder_thread = NULL;
    }

	g_async_queue_push(uploader_request_queue, &uploader_exit_message);
	if(uploader_thread != NULL) {
        g_thread_join(uploader_thread);
        uploader_thread = NULL;
    }

	/* FIXME We should destroy the sessions cleanly */
	janus_mutex_lock(&sessions_mutex);
	g_hash_table_destroy(sessions);
	sessions = NULL;
	janus_mutex_unlock(&sessions_mutex);
	g_async_queue_unref(messages);
	messages = NULL;

	g_async_queue_unref(player_request_queue);
	player_request_queue = NULL;
	g_async_queue_unref(player_response_queue);
	player_response_queue = NULL;

	g_async_queue_unref(recorder_request_queue);
	recorder_request_queue = NULL;

	g_async_queue_unref(encoder_request_queue);
	encoder_request_queue = NULL;

	g_async_queue_unref(uploader_request_queue);
	uploader_request_queue = NULL;

	g_atomic_int_set(&initialized, 0);
	g_atomic_int_set(&stopping, 0);
	JANUS_LOG(LOG_INFO, "%s destroyed!\n", JANUS_SAFIEVOICE_NAME);
}

int janus_safievoice_get_api_compatibility(void) {
	/* Important! This is what your plugin MUST always return: don't lie here or bad things will happen */
	return JANUS_PLUGIN_API_VERSION;
}

int janus_safievoice_get_version(void) {
	return JANUS_SAFIEVOICE_VERSION;
}

const char *janus_safievoice_get_version_string(void) {
	return JANUS_SAFIEVOICE_VERSION_STRING;
}

const char *janus_safievoice_get_description(void) {
	return JANUS_SAFIEVOICE_DESCRIPTION;
}

const char *janus_safievoice_get_name(void) {
	return JANUS_SAFIEVOICE_NAME;
}

const char *janus_safievoice_get_author(void) {
	return JANUS_SAFIEVOICE_AUTHOR;
}

const char *janus_safievoice_get_package(void) {
	return JANUS_SAFIEVOICE_PACKAGE;
}

static janus_safievoice_session *janus_safievoice_lookup_session(janus_plugin_session *handle) {
	janus_safievoice_session *session = NULL;
	if (g_hash_table_contains(sessions, handle)) {
		session = (janus_safievoice_session *)handle->plugin_handle;
	}
	return session;
}

void janus_safievoice_create_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		JANUS_LOG(LOG_ERR, "failded to create session as stopping!!\n");
		*error = -1;
		return;
	}
	janus_safievoice_session *session = g_malloc0(sizeof(janus_safievoice_session));
	session->handle = handle;
	session->recording_id = janus_random_uint64();
	session->start_time = 0;
    session->decoder = NULL;
#if defined(DUMP_RAW_PCM)
    char opus_f[255];
    g_snprintf(opus_f, 255, "/sdcard/janus_voice_mail_%p.opus", handle);
	session->opus_filename = g_strdup(opus_f);
	session->opus_fd = -1;

    char pcm_f[255];
    g_snprintf(pcm_f, 255, "/sdcard/janus_voice_mail_%p.wav", handle);
	session->pcm_filename = g_strdup(pcm_f);
	session->pcm_fd = -1;
#endif
	session->first_rtp_time = 0;
	session->last_rtp_time = 0;
    session->total_rtp_cnt = 0;
    session->skiped_rtp_cnt = 0;
    session->timeover_incoming_rtp_cnt = 0;
	session->cur_latency = 0;
	g_async_queue_push(player_request_queue, &player_open_message);

    /* wait for response from player */
    janus_safievoice_player_response_message *msg =
        g_async_queue_timeout_pop(player_response_queue, PLAYER_RESPONSE_TIMEOUT);
    if ((msg == NULL) || (msg != &player_open_succeeded)) {
		JANUS_LOG(LOG_ERR, "failded to create session as player can not be open!\n");
        *error = -1;
        return;
    }

	session->seq = 0;
	session->record_seq = 0;
	session->record_start_time = 0;
	session->opus_pt = 0;
	session->started = FALSE;
	session->stopping = FALSE;
	g_atomic_int_set(&session->hangingup, 0);
	g_atomic_int_set(&session->destroyed, 0);
	janus_refcount_init(&session->ref, janus_safievoice_session_free);
	handle->plugin_handle = session;

	janus_mutex_lock(&sessions_mutex);
	g_hash_table_insert(sessions, handle, session);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

void janus_safievoice_destroy_session(janus_plugin_session *handle, int *error) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		*error = -1;
		return;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_safievoice_session *session = janus_safievoice_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No SafieVoice session associated with this handle...\n");
		*error = -2;
		return;
	}
	JANUS_LOG(LOG_VERB, "Removing SafieVoice session...\n");
	janus_safievoice_hangup_media_internal(handle);

	g_async_queue_push(player_request_queue, &player_close_message);

    /* wait for response from player */
    janus_safievoice_player_response_message *msg =
        g_async_queue_timeout_pop(player_response_queue, PLAYER_RESPONSE_TIMEOUT);
    if ((msg == NULL) || (msg != &player_close_succeeded)) {
		JANUS_LOG(LOG_ERR, "Can not close player...0x%p\n", msg);
    }

	if (session->first_rtp_time != 0) {
		g_async_queue_push(recorder_request_queue, &recorder_close_message);
	}

#if defined(DUMP_RAW_PCM)
    if (session->opus_fd >= 0) {
        fsync(session->opus_fd);
        close(session->opus_fd);
        session->opus_fd = -1;
    }

    if (session->pcm_fd >= 0) {
        fsync(session->pcm_fd);
        close(session->pcm_fd);
        session->pcm_fd = -1;
    }
#endif

	if (session->decoder) {
		opus_decoder_destroy(session->decoder);
		session->decoder = NULL;
	}

	handle->plugin_handle = NULL;
	g_hash_table_remove(sessions, handle);
	janus_mutex_unlock(&sessions_mutex);

	return;
}

json_t *janus_safievoice_query_session(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized)) {
		return NULL;
	}
	janus_mutex_lock(&sessions_mutex);
	janus_safievoice_session *session = janus_safievoice_lookup_session(handle);
	if(!session) {
		janus_mutex_unlock(&sessions_mutex);
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return NULL;
	}
	janus_refcount_increase(&session->ref);
	janus_mutex_unlock(&sessions_mutex);
	/* In the echo test, every session is the same: we just provide some configure info */
	json_t *info = json_object();
	json_object_set_new(info, "state", json_string(session->decoder ? "recording" : "idle"));
	if(session->decoder) {
		json_object_set_new(info, "id", json_integer(session->recording_id));
		json_object_set_new(info, "start_time", json_integer(session->start_time));
	}
	json_object_set_new(info, "hangingup", json_integer(g_atomic_int_get(&session->hangingup)));
	json_object_set_new(info, "destroyed", json_integer(g_atomic_int_get(&session->destroyed)));
	janus_refcount_decrease(&session->ref);
	return info;
}

struct janus_plugin_result *janus_safievoice_handle_message(janus_plugin_session *handle, char *transaction, json_t *message, json_t *jsep) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, g_atomic_int_get(&stopping) ? "Shutting down" : "Plugin not initialized", NULL);

	janus_mutex_lock(&sessions_mutex);
	janus_safievoice_session *session = janus_safievoice_lookup_session(handle);
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
		JANUS_SAFIEVOICE_ERROR_MISSING_ELEMENT, JANUS_SAFIEVOICE_ERROR_INVALID_ELEMENT);
	if(error_code != 0) {
		return janus_plugin_result_new(JANUS_PLUGIN_ERROR, error_cause, NULL);
	}

	json_t *request = json_object_get(message, "request");
	const char *request_text = json_string_value(request);
	if(!strcasecmp(request_text, "info")) {
		/* Get info of session */
		JANUS_LOG(LOG_VERB, "[safievoice] Get info of session\n");
		gint64 now = janus_get_monotonic_time();
		json_t *event = json_object();
		json_object_set_new(event, "safievoice", json_string("info"));
		if (session->started) {
			json_object_set_new(event, "started", json_true());
			json_object_set_new(event, "time_from_start", json_integer(now - session->start_time));
		} else {
			json_object_set_new(event, "started", json_false());
			json_object_set_new(event, "time_from_start", json_integer(0));
		}
		json_object_set_new(event, "stopping", session->stopping ? json_true() : json_false());
		json_object_set_new(event, "hangingup", json_integer(g_atomic_int_get(&session->hangingup)));
		json_object_set_new(event, "destroyed", json_integer(g_atomic_int_get(&session->destroyed)));
		json_object_set_new(event, "total_rtp_cnt", json_integer(session->total_rtp_cnt));
		json_object_set_new(event, "skiped_rtp_cnt", json_integer(session->skiped_rtp_cnt));
		json_object_set_new(event, "timeover_incoming_rtp_cnt", json_integer(session->timeover_incoming_rtp_cnt));
		json_object_set_new(event, "cur_latency", json_integer(session->cur_latency));
		if (session->first_rtp_time == 0) {
			json_object_set_new(event, "rtp_started", json_false());
			json_object_set_new(event, "time_from_first_rtp", json_integer(0));
			json_object_set_new(event, "time_from_last_rtp", json_integer(0));
		} else {
			json_object_set_new(event, "rtp_started", json_true());
			json_object_set_new(event, "time_from_first_rtp", json_integer(now - session->first_rtp_time));
			json_object_set_new(event, "time_from_last_rtp", json_integer(now - session->last_rtp_time));
		}
		return janus_plugin_result_new(JANUS_PLUGIN_OK, NULL, event);
	}
#endif

	janus_safievoice_message *msg = g_malloc(sizeof(janus_safievoice_message));
	msg->handle = handle;
	msg->transaction = transaction;
	msg->message = message;
	msg->jsep = jsep;
	g_async_queue_push(messages, msg);

	/* All the requests to this plugin are handled asynchronously */
	return janus_plugin_result_new(JANUS_PLUGIN_OK_WAIT, NULL, NULL);
}

void janus_safievoice_setup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "[%s-%p] WebRTC media is now available\n", JANUS_SAFIEVOICE_PACKAGE, handle);
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_mutex_lock(&sessions_mutex);
	janus_safievoice_session *session = janus_safievoice_lookup_session(handle);
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
	session->started = TRUE;
	/* Prepare JSON event */
	json_t *event = json_object();
	json_object_set_new(event, "safievoice", json_string("event"));
	json_object_set_new(event, "status", json_string("started"));
	int ret = gateway->push_event(handle, &janus_safievoice_plugin, NULL, event, NULL);
	JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
	json_decref(event);
	janus_refcount_decrease(&session->ref);
}

void janus_safievoice_incoming_rtp(janus_plugin_session *handle, int video, char *buf, int len) {
	if(handle == NULL || g_atomic_int_get(&handle->stopped) || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_safievoice_session *session = (janus_safievoice_session *)handle->plugin_handle;
	if(!session || g_atomic_int_get(&session->destroyed) || session->stopping || !session->started || session->start_time == 0)
		return;

    gint64 rtp_time = janus_get_monotonic_time();
	session->last_rtp_time = rtp_time;
    if (session->first_rtp_time == 0) {
        session->first_rtp_time = rtp_time;
        long int setup_time = (session->first_rtp_time - session->start_time);
        JANUS_LOG(LOG_WARN, "time[Media setup -> 1st rtp received] = %ld us\n", setup_time);
		g_async_queue_push(recorder_request_queue, &recorder_open_message);
    }

	/* Save the frame */
	janus_rtp_header *rtp = (janus_rtp_header *)buf;
	uint16_t seq = ntohs(rtp->seq_number);
	if(session->seq == 0)
		session->seq = seq;

    session->total_rtp_cnt ++;

    gint buffering_frames_num = g_async_queue_length(player_request_queue);
    buffering_frames_num = buffering_frames_num < 0 ? 0 : buffering_frames_num;
    long int buffering_latency = buffering_frames_num * USEC_PER_FRAME;

    uint32_t hl_timestamp = ntohl(rtp->timestamp);
    long int expect_time = session->first_rtp_time + ((hl_timestamp / FRAME_SAMPLE_NUM) * USEC_PER_FRAME);
    long int incoming_latency = rtp_time - expect_time;

    long int total_latency = buffering_latency + incoming_latency + PLAYBACK_LATENCY_IN_USEC;
	session->cur_latency = total_latency;
    if (total_latency > LATENCY_CONTROL_START_THRESHOLD) {
        session->timeover_incoming_rtp_cnt ++;
        int latency_level = GET_LATENCY_LEVEL(total_latency);
        int skip_num = skip_params[latency_level].skip_num;
        int skip_base = skip_params[latency_level].skip_base;
        if ((session->timeover_incoming_rtp_cnt % skip_base) < skip_num) {
            session->skiped_rtp_cnt ++;

            /* skip decoded PCM buffer */
            janus_safievoice_player_request_message *msg = NULL;
            int clean_buffer_cnt = 0;
            int clean_buffer_num = skip_params[latency_level].clean_buffer_bum;
            while((total_latency > LATENCY_CONTROL_EMERGENCY_THRESHOLD) ||
                  (clean_buffer_cnt < clean_buffer_num)) {
                msg = g_async_queue_try_pop(player_request_queue);
                if (msg == NULL)
                    break;

                janus_safievoice_player_request_message_free(msg);
                clean_buffer_cnt ++;
            }

            JANUS_LOG(LOG_ERR, "Skip total timeover rtp->{seq=%d, skiped:total:to=%ld:%ld:%ld} as latency(%ld:%ld:%ld) > %d ms, skip(lvl%d) rate=%d/%d, cleaned buf=%d\n",
                      seq,
                      (long int)session->skiped_rtp_cnt,
                      (long int)session->total_rtp_cnt,
                      (long int)session->timeover_incoming_rtp_cnt,
                      total_latency,
                      incoming_latency,
                      buffering_latency,
                      LATENCY_CONTROL_START_THRESHOLD/1000,
                      latency_level,
                      skip_num,
                      skip_base,
                      clean_buffer_num
                );

            return;
        }
    }

	int plen = 0;
	const unsigned char *payload = (const unsigned char *)janus_rtp_payload(buf, len, &plen);
	if(!payload) {
		JANUS_LOG(LOG_ERR, "Ops! got an error accessing the RTP payload\n");
		return;
	}

	/* decode and save in file */
    janus_safievoice_player_request_message* player_msg = 
		janus_safievoice_player_request_message_alloc();

	player_msg->latency = incoming_latency;
    player_msg->rtp_time = rtp_time;
    player_msg->hl_timestamp = hl_timestamp;

    opus_int16* pcm = player_msg->pcm_buf;
	int sample_num = opus_decode(session->decoder, payload, plen, pcm, MIN_SAMPLE_NUM_TO_DECODE, 0);
	if (sample_num > 0) {
        player_msg->sample_num = sample_num;
        g_async_queue_push(player_request_queue, player_msg);
	} else if (sample_num < 0) {
		JANUS_LOG(LOG_ERR, "Decode error(%s)\n", opus_strerror(sample_num));
		janus_safievoice_player_request_message_free(player_msg);
	}

#if defined(DUMP_RAW_PCM)
	if (plen > 0 && session->opus_fd != -1) {
		/* write opus */
		size_t ws = plen;
		const char* buf = (const char*)payload;
		while (ws > 0) {
			ssize_t n = write(session->opus_fd, buf, ws);
			if (n >= 0) {
				ws -= (size_t)n;
				buf += n;
			} else {
				break;
			}
		}
	}

	if (sample_num > 0 && session->pcm_fd != -1) {
		/* write pcm */
		size_t ws = sizeof(opus_int16) * CHANNELS * (size_t)sample_num;
		const char* buf = (const char*)pcm;
		while (ws > 0) {
			ssize_t n = write(session->pcm_fd, buf, ws);
			if (n >= 0) {
				ws -= (size_t)n;
				buf += n;
			} else {
				break;
			}
		}
	}
#endif
}

void janus_safievoice_incoming_rtcp(janus_plugin_session *handle, int video, char *buf, int len) {
	if(handle == NULL || g_atomic_int_get(&handle->stopped) || g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	/* FIXME Should we care? */
}

void janus_safievoice_hangup_media(janus_plugin_session *handle) {
	JANUS_LOG(LOG_INFO, "[%s-%p] No WebRTC media anymore\n", JANUS_SAFIEVOICE_PACKAGE, handle);
	janus_mutex_lock(&sessions_mutex);
	janus_safievoice_hangup_media_internal(handle);
	janus_mutex_unlock(&sessions_mutex);
}

static void janus_safievoice_hangup_media_internal(janus_plugin_session *handle) {
	if(g_atomic_int_get(&stopping) || !g_atomic_int_get(&initialized))
		return;
	janus_safievoice_session *session = janus_safievoice_lookup_session(handle);
	if(!session) {
		JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
		return;
	}
	session->started = FALSE;
	if(g_atomic_int_get(&session->destroyed))
		return;
	if(!g_atomic_int_compare_and_exchange(&session->hangingup, 0, 1))
		return;

	g_atomic_int_set(&session->hangingup, 0);
}

/* Thread to handle incoming messages */
static void *janus_safievoice_handler(void *data) {
	JANUS_LOG(LOG_VERB, "SafieVoice handler thread started\n");
	janus_safievoice_message *msg = NULL;
	int error_code = 0;
	char error_cause[512];
	json_t *root = NULL;
	while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		msg = g_async_queue_pop(messages);
		if(msg == &exit_message)
			break;
		if(msg->handle == NULL) {
			janus_safievoice_message_free(msg);
			continue;
		}
		janus_mutex_lock(&sessions_mutex);
		janus_safievoice_session *session = janus_safievoice_lookup_session(msg->handle);
		if(!session) {
			janus_mutex_unlock(&sessions_mutex);
			JANUS_LOG(LOG_ERR, "No session associated with this handle...\n");
			janus_safievoice_message_free(msg);
			continue;
		}
		if(g_atomic_int_get(&session->destroyed)) {
			janus_mutex_unlock(&sessions_mutex);
			janus_safievoice_message_free(msg);
			continue;
		}
		janus_mutex_unlock(&sessions_mutex);
		/* Handle request */
		error_code = 0;
		root = msg->message;
		if(msg->message == NULL) {
			JANUS_LOG(LOG_ERR, "No message??\n");
			error_code = JANUS_SAFIEVOICE_ERROR_NO_MESSAGE;
			g_snprintf(error_cause, 512, "%s", "No message??");
			goto error;
		}
		if(!json_is_object(root)) {
			JANUS_LOG(LOG_ERR, "JSON error: not an object\n");
			error_code = JANUS_SAFIEVOICE_ERROR_INVALID_JSON;
			g_snprintf(error_cause, 512, "JSON error: not an object");
			goto error;
		}
		/* Get the request first */
		JANUS_VALIDATE_JSON_OBJECT(root, request_parameters,
			error_code, error_cause, TRUE,
			JANUS_SAFIEVOICE_ERROR_MISSING_ELEMENT, JANUS_SAFIEVOICE_ERROR_INVALID_ELEMENT);
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
			if(session->decoder != NULL) {
                JANUS_LOG(LOG_ERR, "Already recording (safie)\n");
				error_code = JANUS_SAFIEVOICE_ERROR_ALREADY_RECORDING;
				g_snprintf(error_cause, 512, "Already recording");
				goto error;
			}

			if (session->decoder == NULL) {
				int err = 0;
				session->decoder = opus_decoder_create(SAMPLE_RATE, CHANNELS, &err);
				if (err < 0) {
					g_snprintf(error_cause, 512, "Failed to create decoder (%s)\n", opus_strerror(err));
					error_code = JANUS_SAFIEVOICE_ERROR_IO_ERROR;
					JANUS_LOG(LOG_ERR, "%s\n", error_cause);
					goto error;
				}
#if defined(DUMP_RAW_PCM)
				session->opus_fd = open(session->opus_filename, 
										O_CREAT | O_TRUNC | O_RDWR);
				if (session->opus_fd == -1) {
					perror("opus_fd");
					JANUS_LOG(LOG_ERR, "Create(%s)=%d\n", session->opus_filename, session->opus_fd);
				}

				session->pcm_fd = open(session->pcm_filename, 
										O_CREAT | O_TRUNC | O_RDWR);
				if (session->pcm_fd == -1) {
					perror("pcm_fd");
					JANUS_LOG(LOG_ERR, "Create(%s)=%d\n", session->pcm_filename, session->pcm_fd);
				}
#endif
			}
			session->seq = 0;

			/* Done: now wait for the setup_media callback to be called */
			event = json_object();
			json_object_set_new(event, "safievoice", json_string("event"));
			json_object_set_new(event, "status", json_string(session->started ? "started" : "starting"));
			/* Also notify event handlers */
			if(notify_events && gateway->events_is_enabled()) {
				json_t *info = json_object();
				json_object_set_new(info, "event", json_string("starting"));
				gateway->notify_event(&janus_safievoice_plugin, session->handle, info);
			}
		} else if(!strcasecmp(request_text, "update")) {
			/* Only needed in case of renegotiations and ICE restarts (but with 10s messages is this worth it?) */
			JANUS_LOG(LOG_VERB, "Updating existing recording\n");
			if(session->decoder == NULL || !session->started) {
				JANUS_LOG(LOG_ERR, "Invalid state (not recording)\n");
				error_code = JANUS_SAFIEVOICE_ERROR_INVALID_STATE;
				g_snprintf(error_cause, 512, "Invalid state (not recording)");
				goto error;
			}
			sdp_update = TRUE;
			event = json_object();
			json_object_set_new(event, "safievoice", json_string("event"));
			json_object_set_new(event, "status", json_string("updating"));
		} else if(!strcasecmp(request_text, "stop")) {
			/* Stop the recording */
			session->started = FALSE;
			session->stopping = TRUE;

#if defined(DUMP_RAW_PCM)
			if (session->opus_fd >= 0) {
				fsync(session->opus_fd);
				close(session->opus_fd);
				session->opus_fd = -1;
			}

			if (session->pcm_fd >= 0) {
				fsync(session->pcm_fd);
				close(session->pcm_fd);
				session->pcm_fd = -1;
			}
#endif

			if (session->decoder) {
				opus_decoder_destroy(session->decoder);
				session->decoder = NULL;
			}
			/* Done: send the event and close the handle */
			event = json_object();
			json_object_set_new(event, "safievoice", json_string("event"));
			json_object_set_new(event, "status", json_string("done"));
			/* Also notify event handlers */
			if(notify_events && gateway->events_is_enabled()) {
				json_t *info = json_object();
				json_object_set_new(info, "event", json_string("done"));
				gateway->notify_event(&janus_safievoice_plugin, session->handle, info);
			}
		} else {
			JANUS_LOG(LOG_ERR, "Unknown request '%s'\n", request_text);
			error_code = JANUS_SAFIEVOICE_ERROR_INVALID_REQUEST;
			g_snprintf(error_cause, 512, "Unknown request '%s'", request_text);
			goto error;
		}

		/* Prepare JSON event */
		JANUS_LOG(LOG_VERB, "[safievoice]Preparing JSON event as a reply\n");
		/* Any SDP to handle? */
		const char *msg_sdp_type = json_string_value(json_object_get(msg->jsep, "type"));
		const char *msg_sdp = json_string_value(json_object_get(msg->jsep, "sdp"));
		if(!msg_sdp) {
			int ret = gateway->push_event(msg->handle, &janus_safievoice_plugin, msg->transaction, event, NULL);
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
			/* What is the Opus payload type? */
			int opus_pt = janus_get_codec_pt(msg_sdp, "opus");
			session->opus_pt = opus_pt;
			JANUS_LOG(LOG_VERB, "Opus payload type is %d\n", opus_pt);
			g_snprintf(sdp, 1024, sdp_template,
				session->sdp_sessid,
				session->sdp_version,
				session->recording_id,			/* Recording ID */
				opus_pt,						/* Opus payload type */
				opus_pt                         /* Opus payload type */);

			/* Did the peer negotiate video? */
			if(strstr(msg_sdp, "m=video") != NULL) {
				/* If so, reject it */
				g_strlcat(sdp, "m=video 0 RTP/SAVPF 0\r\n", 1024);
			}
			json_t *jsep = json_pack("{ssss}", "type", type, "sdp", sdp);
			/* How long will the Janus core take to push the event? */
			g_atomic_int_set(&session->hangingup, 0);
			gint64 start = janus_get_monotonic_time();
			int res = gateway->push_event(msg->handle, &janus_safievoice_plugin, msg->transaction, event, jsep);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (took %"SCNu64" us)\n", res, janus_get_monotonic_time()-start);
			json_decref(event);
			json_decref(jsep);
			if(res != JANUS_OK) {
				/* TODO Failed to negotiate? We should remove this participant */
			}
		}
		janus_safievoice_message_free(msg);

		if(session->stopping) {
			gateway->end_session(session->handle);
		}

		continue;

error:
		{
			/* Prepare JSON error event */
			json_t *event = json_object();
			json_object_set_new(event, "safievoice", json_string("event"));
			json_object_set_new(event, "error_code", json_integer(error_code));
			json_object_set_new(event, "error", json_string(error_cause));
			int ret = gateway->push_event(msg->handle, &janus_safievoice_plugin, msg->transaction, event, NULL);
			JANUS_LOG(LOG_VERB, "  >> Pushing event: %d (%s)\n", ret, janus_get_api_error(ret));
			json_decref(event);
			janus_safievoice_message_free(msg);
		}
	}
	JANUS_LOG(LOG_VERB, "Leaving SafieVoice handler thread\n");
	return NULL;
}

#if !defined(JANUS_USE_PLUSE_AUDIO)
static snd_pcm_t* pcm_alsa_open(snd_pcm_stream_t stream);
static int pcm_alsa_playback(snd_pcm_t *pcm_handler, opus_int16* pcm_buf, uint32_t sample_num);
static void pcm_alsa_close(snd_pcm_t *pcm_handler);

static snd_pcm_t* pcm_alsa_open(snd_pcm_stream_t stream)
{
    static gboolean is_retrying[SND_PCM_STREAM_LAST] = {FALSE, FALSE};
    unsigned int err;
    snd_pcm_t *pcm_handler = NULL;

	/* Open the PCM device in playback mode */
	err = snd_pcm_open(&pcm_handler, "default", stream, 
#if 0
						0 /* block */
#else
						SND_PCM_NONBLOCK
#endif
						);
    if (err != 0) {
        if (!is_retrying[stream]) {
            JANUS_LOG(LOG_ERR, "ERROR: Can't open PCM device[%d]. %s\n",
                      stream, snd_strerror(err));
            is_retrying[stream] = TRUE;
        }
        pcm_alsa_close(pcm_handler);
        return NULL;
    }

    /* set parameters */
	unsigned int ch = (stream == SND_PCM_STREAM_CAPTURE) ?  RECORD_CHANNEL_NUM : CHANNELS;
	unsigned int sample_rate = (stream == SND_PCM_STREAM_CAPTURE) ? RECORD_SAMPLE_RATE : SAMPLE_RATE;
	unsigned int latency = (stream == SND_PCM_STREAM_CAPTURE) ? RECORD_LATENCY_IN_USEC : PLAYBACK_LATENCY_IN_USEC;
    err = snd_pcm_set_params(pcm_handler,
                             SND_PCM_FORMAT_S16_LE,
                             SND_PCM_ACCESS_RW_INTERLEAVED,
                             ch, sample_rate, 1, latency);
    if (err != 0) {
        if (!is_retrying[stream]) {
            JANUS_LOG(LOG_ERR, "PCM device[%d] open error: %s\n", stream, snd_strerror(err));
            is_retrying[stream] = TRUE;
        }
        pcm_alsa_close(pcm_handler);
        return NULL;
    }

    is_retrying[stream] = FALSE;
    JANUS_LOG(LOG_WARN, "opened PCM device[%d].\n", stream);
    return pcm_handler;
}

static int pcm_alsa_io(snd_pcm_t *pcm_handler, opus_int16* pcm_buf, uint32_t sample_num)
{
    int pcm_ret;

    if (pcm_handler == NULL) {
        JANUS_LOG(LOG_ERR, "ERROR. pcm_handler == NULL!\n");
        return -1;
    }

	snd_pcm_stream_t stream = snd_pcm_stream(pcm_handler);
	const char* stream_type = ((stream == SND_PCM_STREAM_CAPTURE) ? "capture" : "play");
    int remain_size = sample_num;
    uint32_t retry_cnt = 0;
    while (remain_size > 0) {
		if (stream == SND_PCM_STREAM_CAPTURE) {
			uint32_t offset = (sample_num - remain_size) * RECORD_CHANNEL_NUM;
        	pcm_ret = snd_pcm_readi(pcm_handler, pcm_buf + offset, remain_size);
		} else {
			uint32_t offset = (sample_num - remain_size) * CHANNELS;
			pcm_ret = snd_pcm_writei(pcm_handler, pcm_buf + offset, remain_size);
		}
        if (pcm_ret == -EAGAIN) {
			// todo: improve performance
            if (retry_cnt > 20000) {
                JANUS_LOG(LOG_ERR, "[%s]failed, retry cnt=%d. progress=%d/%d\n", 
					stream_type, retry_cnt, sample_num - remain_size, sample_num);
                return pcm_ret;
            }
            retry_cnt++;
        } else if (pcm_ret == -EPIPE) {
            JANUS_LOG(LOG_WARN, "[%s] overrun. progress=%d/%d\n", 
				stream_type, sample_num - remain_size, sample_num);
			int err;
            if ((err = snd_pcm_recover(pcm_handler, pcm_ret, 0)) < 0) {
    			JANUS_LOG(LOG_WARN, "[%s] recover failed: %s\n", 
					stream_type, snd_strerror(err));
                return err;
            }
        } else if (pcm_ret == -EBADFD) {
            JANUS_LOG(LOG_ERR, "[%s] PCM is not in the right state. progress=%d/%d\n", 
				stream_type, sample_num - remain_size, sample_num);
			return pcm_ret;
		} else if (pcm_ret == -ESTRPIPE) {
            JANUS_LOG(LOG_ERR, "[%s]a suspend event occurred . progress=%d/%d\n",
				stream_type, sample_num - remain_size, sample_num);
			int err;
        	while ((err = snd_pcm_resume(pcm_handler)) == -EAGAIN) {
				JANUS_LOG(LOG_ERR, "[%s]waitng for resume . progress=%d/%d\n",
					stream_type, sample_num - remain_size, sample_num);
            	sleep(1);   /* wait until the suspend flag is released */
			}
        	if (err < 0) {
            	if ((err = snd_pcm_prepare(pcm_handler)) < 0) {
	                JANUS_LOG(LOG_ERR, "[%s]Can't recovery from suspend, prepare failed: %s\n", 
						stream_type, snd_strerror(err));
					return err;
				}
			}
		} else if (pcm_ret < 0) {
            JANUS_LOG(LOG_ERR, "[%s]ERROR. progress=%d/%d, pcm_ret=%d, %s\n",
						stream_type, sample_num - remain_size, sample_num,
                      	pcm_ret, snd_strerror(pcm_ret));
            return pcm_ret;
        } else if (pcm_ret > remain_size) {
            JANUS_LOG(LOG_ERR, "[%s]ERROR? progress=%d/%d, pcm_ret=%d\n",
						stream_type,
					  	sample_num - remain_size, sample_num, pcm_ret);
            return pcm_ret;
        } else {
            remain_size -= pcm_ret;
        }
    }

	return 0;
}

static int pcm_alsa_playback(snd_pcm_t *pcm_handler, opus_int16* pcm_buf, uint32_t sample_num)
{
    int pcm_ret;

    if (pcm_handler == NULL) {
        JANUS_LOG(LOG_ERR, "ERROR. pcm_handler == NULL!\n");
        return -1;
    }

    int write_size = sample_num;
    uint32_t retry_cnt = 0;
    while (write_size > 0) {
        uint32_t offset = (sample_num - write_size) * CHANNELS;
        pcm_ret = snd_pcm_writei(pcm_handler, pcm_buf + offset, write_size);
        if (pcm_ret == -EAGAIN) {
            if (retry_cnt > 10000) {
                JANUS_LOG(LOG_WARN, "pcm_alsa_playback, retry cnt=%d.\n[%lld, %lld]\n", 
					retry_cnt, 
					play_buffer_alloc_cnt - play_buffer_free_cnt,
					record_buffer_alloc_cnt - record_buffer_free_cnt
				);
                return 1;
            }
            retry_cnt++;
        } else if (pcm_ret == -EPIPE) {
            JANUS_LOG(LOG_WARN, "XRUN. write_size=%d\n", write_size);
            snd_pcm_prepare(pcm_handler);
        } else if (pcm_ret < 0) {
            JANUS_LOG(LOG_ERR, "ERROR. Can't write to PCM device[%d]. write_size=%d, pcm_ret=%d, %s\n",
                      snd_pcm_stream(pcm_handler),
                      write_size, pcm_ret, snd_strerror(pcm_ret));
            return -2;
        } else if (pcm_ret > write_size) {
            JANUS_LOG(LOG_ERR, "ERROR. Overrun PCM device[%d]. write_size=%d, pcm_ret=%d\n",
                      snd_pcm_stream(pcm_handler),
                      write_size, pcm_ret);
            return -3;
        } else {
            write_size -= pcm_ret;
        }
    }
    return 0;
}

static void pcm_alsa_close(snd_pcm_t *pcm_handler)
{
    if (pcm_handler == NULL)
        return;

    snd_pcm_drain(pcm_handler);
    snd_pcm_close(pcm_handler);
}
#else
static pa_simple* pcm_pulse_open(const char* name, pa_stream_direction_t dir, const char* stream_name, int* error);
static int pcm_pulse_playback(pa_simple *pcm_handler, opus_int16* pcm_buf, uint32_t sample_num, int* error);
static void pcm_pulse_close(pa_simple *pcm_handler);

static pa_simple* pcm_pulse_open(const char* name, pa_stream_direction_t dir, const char* stream_name, int* error)
{
	static const pa_sample_spec ss = {
		.format   = PA_SAMPLE_S16LE,
		.rate     = SAMPLE_RATE,
		.channels = CHANNELS
	};

	static const pa_buffer_attr buffer_attr = {
		.tlength   = (uint32_t) PLAYBACK_LATENCY_BUF_SIZE,
        .minreq    = (uint32_t) PCM_FRAME_BUF_SIZE,
    	.maxlength = (uint32_t) -1,
        .prebuf    = (uint32_t) PLAYBACK_LATENCY_BUF_SIZE,
        .fragsize  = (uint32_t) -1
	};

	JANUS_LOG(LOG_WARN, 
				"dir=%d, buffer_attr(tlength=%d, minreq=%d, prebuf=%d)\n",
				dir, 
				buffer_attr.tlength, buffer_attr.minreq, buffer_attr.prebuf);

	return pa_simple_new(NULL, 
					name,
					dir,
					NULL, /* device: use default */
					stream_name,
					&ss,
					NULL, /* channel map */
					&buffer_attr, 
					error);
}

static pa_simple* pcm_pulse_open_record_device(const char* name, pa_stream_direction_t dir, const char* stream_name, int* error)
{
	static const pa_sample_spec ss = {
		.format   = PA_SAMPLE_S16LE,
		.rate     = RECORD_SAMPLE_RATE,
		.channels = RECORD_CHANNEL_NUM
	};

	static const pa_buffer_attr buffer_attr = {
		.tlength   = (uint32_t) -1,
        .minreq    = (uint32_t) -1,
    	.maxlength = (uint32_t) RECORD_LATENCY_PCM_BUF_SIZE,
        .prebuf    = (uint32_t) -1,
        .fragsize  = (uint32_t) -1
	};

	JANUS_LOG(LOG_WARN, 
				"dir=%d, buffer_attr(tlength=%d, minreq=%d, prebuf=%d)\n",
				dir, 
				buffer_attr.tlength, buffer_attr.minreq, buffer_attr.prebuf);

	return pa_simple_new(NULL, 
					name,
					dir,
					NULL,
					//"bluez_source.50_1A_A5_B6_95_03.headset_head_unit", /* device: use default */
					stream_name,
					&ss,
					NULL, /* channel map */
					&buffer_attr, 
					error);
}

static int pcm_pulse_playback(pa_simple *pcm_handler, 
								opus_int16* pcm_buf, uint32_t sample_num, 
								int* error)
{
#if 0
    pa_usec_t latency;
    if ((latency = pa_simple_get_latency(pcm_handler, error)) == (pa_usec_t) -1) {
        JANUS_LOG(LOG_WARN, ": pa_simple_get_latency() failed: %s\n", pa_strerror(*error));
    } else {
    	JANUS_LOG(LOG_WARN, "%0.0f usec    \n", (float)latency);
	}
#endif
	return pa_simple_write(pcm_handler, pcm_buf, sample_num * CHANNELS * sizeof(opus_int16), error);
}

static int pcm_pulse_record(pa_simple *pcm_handler, 
								opus_int16* pcm_buf, uint32_t sample_num, 
								int* error)
{
#if 0
    pa_usec_t latency;
    if ((latency = pa_simple_get_latency(pcm_handler, error)) == (pa_usec_t) -1) {
        JANUS_LOG(LOG_WARN, ": pa_simple_get_latency() failed: %s\n", pa_strerror(*error));
    } else {
    	JANUS_LOG(LOG_WARN, "%0.0f usec    \n", (float)latency);
	}
#endif
	return pa_simple_read(pcm_handler, pcm_buf, sample_num * RECORD_CHANNEL_NUM * sizeof(opus_int16), error);
}

static void pcm_pulse_close(pa_simple *pcm_handler)
{
    if (pcm_handler == NULL)
        return;

	pa_simple_free(pcm_handler);
}

#endif

#if !defined(JANUS_USE_PLUSE_AUDIO)
static snd_pcm_t *speaker_handler = NULL;
#else
static pa_simple *speaker_handler = NULL;
#endif

static gboolean pcm_speaker_open(void)
{
	if (speaker_handler == NULL) {
#if !defined(JANUS_USE_PLUSE_AUDIO)
    	speaker_handler = pcm_alsa_open(SND_PCM_STREAM_PLAYBACK);
#else
		int error = 0;
    	speaker_handler = pcm_pulse_open(JANUS_SAFIEVOICE_PACKAGE, 
										PA_STREAM_PLAYBACK, 
										"janus.playback", 
										&error);
		if (speaker_handler == NULL) {
			JANUS_LOG(LOG_ERR, ": pcm_pulse_open() failed(%d): %s\n", error, pa_strerror(error));
 		}
#endif
	}

	return (speaker_handler != NULL);
}

static int pcm_speaker_playback(opus_int16* pcm_buf, uint32_t sample_num)
{
    int ret;
    if (speaker_handler == NULL) {
        pcm_speaker_open();
    	if (speaker_handler == NULL) {
            JANUS_LOG(LOG_WARN, "[player thread] pcm open failed before playback\n");
			return -1;
        }
    }

#if !defined(JANUS_USE_PLUSE_AUDIO)
    ret = pcm_alsa_io(speaker_handler, pcm_buf, sample_num);
#else
	int error = 0;
    ret = pcm_pulse_playback(speaker_handler, pcm_buf, sample_num, &error);
	if (ret < 0) {
		JANUS_LOG(LOG_ERR, ": pcm_pulse_playback() failed(%d): %s\n", error, pa_strerror(error));
 	}
#endif

	return ret;
}

static void pcm_speaker_close(void)
{
	if (speaker_handler != NULL) {
#if !defined(JANUS_USE_PLUSE_AUDIO)
		pcm_alsa_close(speaker_handler);
#else
		pcm_pulse_close(speaker_handler);
#endif
		speaker_handler = NULL;
	}
}

/* Thread to handle incoming messages */
static void *janus_safievoice_player(void *data) {
	janus_safievoice_player_request_message *msg = NULL;
	static gint64 timeout_cnt = 0;

    JANUS_LOG(LOG_WARN, "SafieVoice player thread started\n");
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
        msg = g_async_queue_timeout_pop(player_request_queue, NO_MEDIA_TIMEOUT);
        if(msg == NULL) {
			timeout_cnt++;
			if (timeout_cnt % (LOG_ALIVE_TIMEOUT/NO_MEDIA_TIMEOUT) == 0) {
            	JANUS_LOG(LOG_INFO, "[player thread] alive. but nothing received, timeout_cnt=%lld\n", timeout_cnt);
			}

            pcm_speaker_close();
            continue;
        } else if(msg == &player_exit_message) {
            JANUS_LOG(LOG_WARN, "[player thread] received exit msg\n");
            break;
        } else if(msg == &player_open_message) {
            JANUS_LOG(LOG_WARN, "[player thread] received open msg\n");
			gboolean ok = pcm_speaker_open();
            /* response to main thread */
            g_async_queue_push(player_response_queue,
                               ok ? &player_open_succeeded : &player_open_failed);
        } else if (msg == &player_close_message) {
            JANUS_LOG(LOG_WARN, "[player thread] received close msg\n");
            pcm_speaker_close();
            /* response to main thread */
            g_async_queue_push(player_response_queue, &player_close_succeeded);
            continue;
        } else {
#if defined(SAFIE_WEARABLE_DEBUG_PRINT_LATENCY)
            long int write_time = janus_get_monotonic_time();
#endif

            /* playback to speaker */
            int ret = pcm_speaker_playback(msg->pcm_buf, msg->sample_num);
            if (ret < 0) {
                JANUS_LOG(LOG_WARN, "[player thread] playback failed, ret=%d\n", ret);
				pcm_speaker_close();
            }

#if defined(SAFIE_WEARABLE_DEBUG_PRINT_LATENCY)
            long int done_time = janus_get_monotonic_time();
            JANUS_LOG(LOG_WARN, "[player thread] received playback msg->(addr=%p, stamp=%d, size=%d), latency=%ld, time(decode=%ld, writep=%ld)\n",
                      msg->pcm_buf,
                      msg->hl_timestamp,
                      msg->sample_num,
                      msg->latency,
                      (long int)(write_time - msg->rtp_time),
                      done_time - write_time
                );
#endif
        	/* free buffer */
            janus_safievoice_player_request_message_free(msg);
            continue;
        }
    }

    pcm_speaker_close();

    while(g_async_queue_length(player_request_queue)) {
        msg = g_async_queue_pop(player_request_queue);
        janus_safievoice_player_request_message_free(msg);
        msg = NULL;
    }

	JANUS_LOG(LOG_WARN, "Leaving SafieVoice player thread\n");
	return NULL;
}

#if !defined(JANUS_USE_PLUSE_AUDIO)
static snd_pcm_t *recorder_handler = NULL;
#else
static pa_simple *recorder_handler = NULL;
#endif

static gboolean pcm_recorder_open(void)
{
	if (recorder_handler == NULL) {
#if !defined(JANUS_USE_PLUSE_AUDIO)
    	recorder_handler = pcm_alsa_open(SND_PCM_STREAM_CAPTURE);
#else
		int error = 0;
    	recorder_handler = pcm_pulse_open_record_device(
								JANUS_SAFIEVOICE_PACKAGE, 
								PA_STREAM_RECORD, "janus.record", &error);
		if (recorder_handler == NULL) {
			JANUS_LOG(LOG_ERR, ": pcm_pulse_open_record_device() failed(%d): %s\n",
						error, pa_strerror(error));
 		}
#endif
	}

	return (recorder_handler != NULL);
}

static int pcm_recorder_record(opus_int16* pcm_buf, uint32_t sample_num)
{
    int ret;
    if (recorder_handler == NULL) {
        pcm_recorder_open();
    	if (recorder_handler == NULL) {
            JANUS_LOG(LOG_WARN, "[player thread] pcm open failed before playback\n");
			return -1;
        }
    }

#if !defined(JANUS_USE_PLUSE_AUDIO)
    ret = pcm_alsa_io(recorder_handler, pcm_buf, sample_num);
#else
	int error = 0;
    ret = pcm_pulse_record(recorder_handler, pcm_buf, sample_num, &error);
	if (ret < 0) {
		JANUS_LOG(LOG_ERR, ": pcm_pulse_playback() failed(%d): %s\n", error, pa_strerror(error));
 	}
#endif
	return ret;
}

static void pcm_recorder_close(void)
{
	if (recorder_handler != NULL) {
#if !defined(JANUS_USE_PLUSE_AUDIO)
		pcm_alsa_close(recorder_handler);
#else
		pcm_pulse_close(recorder_handler);
#endif
		recorder_handler = NULL;
	}
}

static void *janus_safievoice_recorder(void *data) {
	janus_safievoice_recorder_request_message *msg = NULL;
    JANUS_LOG(LOG_WARN, "SafieVoice recorder thread started\n");

	if (!pcm_recorder_open()) {
		JANUS_LOG(LOG_ERR, "Failed to open pulse recorder\n");
	}

	guint session_num = 0;
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
		if (session_num == 0) {
			msg = g_async_queue_timeout_pop(recorder_request_queue, NO_MEDIA_TIMEOUT);
		} else {
        	msg = g_async_queue_try_pop(recorder_request_queue);
		}
        if(msg == NULL) {
			if (session_num == 0) {
				continue;
			}

			// record
			janus_safievoice_record_message* record_job = 
				janus_safievoice_record_message_alloc();
			if (pcm_recorder_record(record_job->pcm_buf, 
									record_job->sample_num) != 0) {
				//JANUS_LOG(LOG_ERR, "[record] failed to record\n");
				janus_safievoice_record_message_free(record_job);
				continue;
			}
			record_job->recorded_time = janus_get_monotonic_time();

			// encode
			g_async_queue_push(encoder_request_queue, record_job);
        } else if(msg == &recorder_exit_message) {
            JANUS_LOG(LOG_WARN, "[recorder thread] received exit msg\n");
            break;
        } else if (msg == &recorder_open_message) {
			JANUS_LOG(LOG_WARN, "[recorder thread] received open msg\n");
			session_num ++;
		} else if (msg == &recorder_close_message) {
			JANUS_LOG(LOG_WARN, "[recorder thread] received close msg, session_num=%d\n", session_num);
			assert(session_num > 0);
			session_num --;
			// todo: clear all record
		}
    }

    pcm_recorder_close();

	JANUS_LOG(LOG_WARN, "Leaving SafieVoice recorder thread\n");
	return NULL;
}

static void *janus_safievoice_encoder(void *data) {
	janus_safievoice_record_message *msg = NULL;
    JANUS_LOG(LOG_WARN, "SafieVoice encoder thread started\n");

	static int error_code = 0;
	static char error_cause[512];
	OpusEncoder *encoder = opus_encoder_create(
									RECORD_SAMPLE_RATE, RECORD_CHANNEL_NUM, 
									OPUS_APPLICATION_VOIP, 
									&error_code);
	if (error_code < 0) {
		g_snprintf(error_cause, 512, "Failed to create encoder (%s)\n", 
					opus_strerror(error_code));
		JANUS_LOG(LOG_ERR, "%s\n", error_cause);
		return NULL;
	}

	static gint64 timeout_cnt = 0;
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
        msg = g_async_queue_timeout_pop(encoder_request_queue, NO_MEDIA_TIMEOUT);
        if(msg == NULL) {
			timeout_cnt++;
			if (timeout_cnt % (LOG_ALIVE_TIMEOUT/NO_MEDIA_TIMEOUT) == 0) {
            	JANUS_LOG(LOG_INFO, "[encoder thread] alive. but nothing received, timeout_cnt=%lld\n", timeout_cnt);
			}
            continue;
		} else if(msg == &encoder_exit_message) {
            JANUS_LOG(LOG_WARN, "[encoder thread] received exit msg\n");
            break;
        }

		// encode
		unsigned char *payload = (unsigned char *)msg->outpkt->data;
		msg->start_encode_time = janus_get_monotonic_time();
		gint length = opus_encode(encoder,
									msg->pcm_buf,
									msg->sample_num,
									payload+12, RECORD_OPUS_FRAME_BUF_SIZE-12);
		if(length < 0) {
			JANUS_LOG(
				LOG_ERR, 
				"[Opus] Ops! got an error encoding the Opus frame: %d (%s), %p, %p, %d\n", 
				length, opus_strerror(length),
				encoder, msg->pcm_buf, msg->sample_num);
			janus_safievoice_record_message_free(msg);
			continue;
		}
		msg->encoded_time = janus_get_monotonic_time();
		msg->outpkt->length = length + 12; /* Take the RTP header into consideration */
		msg->outpkt->data->version = 2;
		msg->outpkt->data->markerbit = 0;	/* FIXME Should be 1 for the first packet */
		msg->outpkt->data->ssrc = htonl(1);	/* The Janus core will fix this anyway */

		// upload
		g_async_queue_push(uploader_request_queue, msg);
    }

    while(g_async_queue_length(encoder_request_queue)) {
        msg = g_async_queue_pop(encoder_request_queue);
        janus_safievoice_record_message_free(msg);
        msg = NULL;
    }

	if (encoder) {
		opus_encoder_destroy(encoder);
		encoder = NULL;
	}

	JANUS_LOG(LOG_WARN, "Leaving SafieVoice encoder thread\n");
	return NULL;
}

static void janus_safievoice_relay_rtp_packet(
	gpointer key, gpointer value, gpointer data) {
	janus_safievoice_rtp_relay_packet *packet = (janus_safievoice_rtp_relay_packet *)data;
	if(!packet || !packet->data || packet->length < 1) {
		JANUS_LOG(LOG_ERR, "Invalid packet...\n");
		return;
	}
	janus_safievoice_session *session = (janus_safievoice_session *)value;
	if(!session || !session->handle) {
		// JANUS_LOG(LOG_ERR, "Invalid session...\n");
		return;
	}
	if(!g_atomic_int_get(&session->started)) {
		// JANUS_LOG(LOG_ERR, "Streaming not started yet for this session...\n");
		return;
	}
	/* Set the payload type */
	packet->data->type = session->opus_pt;

	packet->data->seq_number = session->record_seq;
	session->record_seq++;
	if (packet->data->seq_number == 0) {
		session->record_start_time = janus_get_monotonic_time();
	}
	packet->data->timestamp = htonl(session->record_seq*RECORD_TIMESTAMP_SAMPLE_NUM);

#if 0
	JANUS_LOG(LOG_ERR, "[record] relay rtp[%d, %d, %d, %d]\n", 
		packet->data->type,
		packet->data->seq_number,
		session->record_seq*RECORD_FRAME_SAMPLE_NUM,
		packet->data->timestamp);
#endif

	if(gateway != NULL)
		gateway->relay_rtp(session->handle, 0, (char *)packet->data, packet->length);
}

static void *janus_safievoice_uploader(void *data) {
	janus_safievoice_record_message *msg = NULL;
    JANUS_LOG(LOG_WARN, "SafieVoice uploader thread started\n");

	static gint64 timeout_cnt = 0;
    while(g_atomic_int_get(&initialized) && !g_atomic_int_get(&stopping)) {
        msg = g_async_queue_timeout_pop(uploader_request_queue, NO_MEDIA_TIMEOUT);
        if(msg == NULL) {
			timeout_cnt++;
			if (timeout_cnt % (LOG_ALIVE_TIMEOUT/NO_MEDIA_TIMEOUT) == 0) {
            	JANUS_LOG(LOG_INFO, "[uploader thread] alive. but nothing received, timeout_cnt=%lld\n", timeout_cnt);
			}
            continue;
		} else if(msg == &uploader_exit_message) {
            JANUS_LOG(LOG_WARN, "[uploader thread] received exit msg\n");
            break;
        }

		// relay to peer
		janus_mutex_lock(&sessions_mutex);
		guint session_num = g_hash_table_size(sessions);
		g_hash_table_foreach(sessions,
							janus_safievoice_relay_rtp_packet, msg->outpkt);
		janus_mutex_unlock(&sessions_mutex);

#if 0
		gint64 end_time = janus_get_monotonic_time();
    	JANUS_LOG(LOG_WARN, "[session_num=%d] upload rtp(len=%d), record=%lld us, latencyToEncode=%lld us, encode=%lld us, upload=%lld us, latency=%lld us\n",
			session_num,
			msg->outpkt->length,
			(msg->recorded_time - msg->start_time),
			(msg->start_encode_time - msg->recorded_time),
			(msg->encoded_time - msg->start_encode_time),
			(end_time - msg->encoded_time),
			end_time - msg->start_time);
#endif

		// free
		janus_safievoice_record_message_free(msg);
    }

    while(g_async_queue_length(uploader_request_queue)) {
        msg = g_async_queue_pop(uploader_request_queue);
        janus_safievoice_record_message_free(msg);
        msg = NULL;
    }

	JANUS_LOG(LOG_WARN, "Leaving SafieVoice uploader thread\n");
	return NULL;
}
