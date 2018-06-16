/*
Copyright (c) 2009-2014 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at
http://www.eclipse.org/org/documents/edl-v10.php.

Contributors:
Roger Light - initial implementation and documentation.
*/
#include <pthread.h>
#include <assert.h>
#include <stdio.h>

#include <config.h>

#include <mosquitto_broker.h>
#include <memory_mosq.h>
#include <send_mosq.h>
#include <time_mosq.h>
//#define _DUMMYPTHREAD_H_


static int max_inflight = 1000;
static int max_queued = 2000;
#ifdef WITH_SYS_TREE
extern unsigned long g_msgs_dropped;
#endif

int mqtt3_db_open(struct mqtt3_config *config, struct mosquitto_db *db)
{
	int rc = 0;
	struct _mosquitto_subhier *child;

	if (!config || !db) return MOSQ_ERR_INVAL;

	db->last_db_id = 0;

	db->contexts_by_id = NULL;
	db->contexts_by_sock = NULL;
	db->contexts_for_free = NULL;
#ifdef WITH_BRIDGE
	db->bridges = NULL;
	db->bridge_count = 0;
#endif

	// Initialize the hashtable
	db->clientid_index_hash = NULL;

	db->subs.next = NULL;
	db->subs.subs = NULL;
	db->subs.topic = "";

	child = _mosquitto_malloc(sizeof(struct _mosquitto_subhier));
	if (!child) {
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	child->parent = NULL;
	child->next = NULL;
	child->topic = _mosquitto_strdup("");
	if (!child->topic) {
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	child->subs = NULL;
	child->children = NULL;
	child->retained = NULL;
	db->subs.children = child;

	child = _mosquitto_malloc(sizeof(struct _mosquitto_subhier));
	if (!child) {
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	child->parent = NULL;
	child->next = NULL;
	child->topic = _mosquitto_strdup("$SYS");
	if (!child->topic) {
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	child->subs = NULL;
	child->children = NULL;
	child->retained = NULL;
	db->subs.children->next = child;

	db->unpwd = NULL;

#ifdef WITH_PERSISTENCE
	if (config->persistence && config->persistence_filepath) {
		if (mqtt3_db_restore(db)) return 1;
	}
#endif

	return rc;
}

static void subhier_clean(struct mosquitto_db *db, struct _mosquitto_subhier *subhier)
{
	struct _mosquitto_subhier *next;
	struct _mosquitto_subleaf *leaf, *nextleaf;

	while (subhier) {
		next = subhier->next;
		leaf = subhier->subs;
		while (leaf) {
			nextleaf = leaf->next;
			_mosquitto_free(leaf);
			leaf = nextleaf;
		}
		if (subhier->retained) {
			mosquitto__db_msg_store_deref(db, &subhier->retained);
		}
		subhier_clean(db, subhier->children);
		if (subhier->topic) _mosquitto_free(subhier->topic);

		_mosquitto_free(subhier);
		subhier = next;
	}
}

int mqtt3_db_close(struct mosquitto_db *db)
{
	subhier_clean(db, db->subs.children);
	mosquitto__db_msg_store_clean(db);

	return MOSQ_ERR_SUCCESS;
}


void mosquitto__db_msg_store_add(struct mosquitto_db *db, struct mosquitto_msg_store *store)
{
	store->next = db->msg_store;
	store->prev = NULL;
	if (db->msg_store) {
		db->msg_store->prev = store;
	}
	db->msg_store = store;
}


void mosquitto__db_msg_store_remove(struct mosquitto_db *db, struct mosquitto_msg_store *store)
{
	int i;

	if (store->prev) {
		store->prev->next = store->next;
		if (store->next) {
			store->next->prev = store->prev;
		}
	}
	else {
		db->msg_store = store->next;
		if (store->next) {
			store->next->prev = NULL;
		}
	}
	db->msg_store_count--;

	if (store->source_id) _mosquitto_free(store->source_id);
	if (store->dest_ids) {
		for (i = 0; i<store->dest_id_count; i++) {
			if (store->dest_ids[i]) _mosquitto_free(store->dest_ids[i]);
		}
		_mosquitto_free(store->dest_ids);
	}
	if (store->topic) _mosquitto_free(store->topic);
	if (store->payload) _mosquitto_free(store->payload);
	_mosquitto_free(store);
}


void mosquitto__db_msg_store_clean(struct mosquitto_db *db)
{
	struct mosquitto_msg_store *store, *next;;

	store = db->msg_store;
	while (store) {
		next = store->next;
		mosquitto__db_msg_store_remove(db, store);
		store = next;
	}
}

void mosquitto__db_msg_store_deref(struct mosquitto_db *db, struct mosquitto_msg_store **store)
{
	(*store)->ref_count--;
	if ((*store)->ref_count == 0) {
		mosquitto__db_msg_store_remove(db, *store);
		*store = NULL;
	}
}


static void _message_remove(struct mosquitto_db *db, struct mosquitto *context, struct mosquitto_client_msg **msg, struct mosquitto_client_msg *last)
{
	int i;
	struct mosquitto_client_msg *tail;

	if (!context || !msg || !(*msg)) {
		return;
	}

	if ((*msg)->store) {
		mosquitto__db_msg_store_deref(db, &(*msg)->store);
	}
	if (last) {
		last->next = (*msg)->next;
		if (!last->next) {
			context->last_msg = last;
		}
	}
	else {
		context->msgs = (*msg)->next;
		if (!context->msgs) {
			context->last_msg = NULL;
		}
	}
	context->msg_count--;
	if ((*msg)->qos > 0) {
		context->msg_count12--;
	}
	_mosquitto_free(*msg);
	if (last) {
		*msg = last->next;
	}
	else {
		*msg = context->msgs;
	}
	tail = context->msgs;
	i = 0;
	while (tail && tail->state == mosq_ms_queued && i<max_inflight) {
		if (tail->direction == mosq_md_out) {
			switch (tail->qos) {
			case 0:
				tail->state = mosq_ms_publish_qos0;
				break;
			case 1:
				tail->state = mosq_ms_publish_qos1;
				break;
			case 2:
				tail->state = mosq_ms_publish_qos2;
				break;
			}
		}
		else {
			if (tail->qos == 2) {
				tail->state = mosq_ms_send_pubrec;
			}
		}

		tail = tail->next;
	}
}

int mqtt3_db_message_delete(struct mosquitto_db *db, struct mosquitto *context, uint16_t mid, enum mosquitto_msg_direction dir)
{
	struct mosquitto_client_msg *tail, *last = NULL;
	int msg_index = 0;
	bool deleted = false;

	if (!context) return MOSQ_ERR_INVAL;

	tail = context->msgs;
	while (tail) {
		msg_index++;
		if (tail->state == mosq_ms_queued && msg_index <= max_inflight) {
			tail->timestamp = mosquitto_time();
			if (tail->direction == mosq_md_out) {
				switch (tail->qos) {
				case 0:
					tail->state = mosq_ms_publish_qos0;
					break;
				case 1:
					tail->state = mosq_ms_publish_qos1;
					break;
				case 2:
					tail->state = mosq_ms_publish_qos2;
					break;
				}
			}
			else {
				if (tail->qos == 2) {
					tail->state = mosq_ms_wait_for_pubrel;
				}
			}
		}
		if (tail->mid == mid && tail->direction == dir) {
			msg_index--;
			_message_remove(db, context, &tail, last);
			deleted = true;
		}
		else {
			last = tail;
			tail = tail->next;
		}
		if (msg_index > max_inflight && deleted) {
			return MOSQ_ERR_SUCCESS;
		}
	}

	return MOSQ_ERR_SUCCESS;
}

int mqtt3_db_message_insert(struct mosquitto_db *db, struct mosquitto *context, uint16_t mid, enum mosquitto_msg_direction dir, int qos, bool retain, struct mosquitto_msg_store *stored)
{
	struct mosquitto_client_msg *msg;
	enum mosquitto_msg_state state = mosq_ms_invalid;
	int rc = 0;
	int i;
	char **dest_ids;

	assert(stored);
	if (!context) return MOSQ_ERR_INVAL;
	if (!context->id) return MOSQ_ERR_SUCCESS; /* Protect against unlikely "client is disconnected but not entirely freed" scenario */

											   /* Check whether we've already sent this message to this client
											   * for outgoing messages only.
											   * If retain==true then this is a stale retained message and so should be
											   * sent regardless. FIXME - this does mean retained messages will received
											   * multiple times for overlapping subscriptions, although this is only the
											   * case for SUBSCRIPTION with multiple subs in so is a minor concern.
											   */
	if (db->config->allow_duplicate_messages == false
		&& dir == mosq_md_out && retain == false && stored->dest_ids) {

		for (i = 0; i<stored->dest_id_count; i++) {
			if (!strcmp(stored->dest_ids[i], context->id)) {
				/* We have already sent this message to this client. */
				return MOSQ_ERR_SUCCESS;
			}
		}
	}
	if (context->sock == INVALID_SOCKET) {
		/* Client is not connected only queue messages with QoS>0. */
		if (qos == 0 && !db->config->queue_qos0_messages) {
			if (!context->bridge) {
				return 2;
			}
			else {
				if (context->bridge->start_type != bst_lazy) {
					return 2;
				}
			}
		}
	}

	if (context->sock != INVALID_SOCKET) {
		if (qos == 0 || max_inflight == 0 || context->msg_count12 < max_inflight) {
			if (dir == mosq_md_out) {
				switch (qos) {
				case 0:
					state = mosq_ms_publish_qos0;
					break;
				case 1:
					state = mosq_ms_publish_qos1;
					break;
				case 2:
					state = mosq_ms_publish_qos2;
					break;
				}
			}
			else {
				if (qos == 2) {
					state = mosq_ms_wait_for_pubrel;
				}
				else {
					return 1;
				}
			}
		}
		else if (max_queued == 0 || context->msg_count12 - max_inflight < max_queued) {
			state = mosq_ms_queued;
			rc = 2;
		}
		else {
			/* Dropping message due to full queue. */
			if (context->is_dropping == false) {
				context->is_dropping = true;
				_mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE,
					"Outgoing messages are being dropped for client %s.",
					context->id);
			}
#ifdef WITH_SYS_TREE
			g_msgs_dropped++;
#endif
			return 2;
		}
	}
	else {
		if (max_queued > 0 && context->msg_count12 >= max_queued) {
#ifdef WITH_SYS_TREE
			g_msgs_dropped++;
#endif
			if (context->is_dropping == false) {
				context->is_dropping = true;
				_mosquitto_log_printf(NULL, MOSQ_LOG_NOTICE,
					"Outgoing messages are being dropped for client %s.",
					context->id);
			}
			return 2;
		}
		else {
			state = mosq_ms_queued;
		}
	}
	assert(state != mosq_ms_invalid);

#ifdef WITH_PERSISTENCE
	if (state == mosq_ms_queued) {
		db->persistence_changes++;
	}
#endif

	msg = _mosquitto_malloc(sizeof(struct mosquitto_client_msg));
	if (!msg) return MOSQ_ERR_NOMEM;
	msg->next = NULL;
	msg->store = stored;
	msg->store->ref_count++;
	msg->mid = mid;
	msg->timestamp = mosquitto_time();
	msg->direction = dir;
	msg->state = state;
	msg->dup = false;
	msg->qos = qos;
	msg->retain = retain;
	if (context->last_msg) {
		context->last_msg->next = msg;
		context->last_msg = msg;
	}
	else {
		context->msgs = msg;
		context->last_msg = msg;
	}
	context->msg_count++;
	if (qos > 0) {
		context->msg_count12++;
	}

	if (db->config->allow_duplicate_messages == false && dir == mosq_md_out && retain == false) {
		/* Record which client ids this message has been sent to so we can avoid duplicates.
		* Outgoing messages only.
		* If retain==true then this is a stale retained message and so should be
		* sent regardless. FIXME - this does mean retained messages will received
		* multiple times for overlapping subscriptions, although this is only the
		* case for SUBSCRIPTION with multiple subs in so is a minor concern.
		*/
		dest_ids = _mosquitto_realloc(stored->dest_ids, sizeof(char *)*(stored->dest_id_count + 1));
		if (dest_ids) {
			stored->dest_ids = dest_ids;
			stored->dest_id_count++;
			stored->dest_ids[stored->dest_id_count - 1] = _mosquitto_strdup(context->id);
			if (!stored->dest_ids[stored->dest_id_count - 1]) {
				return MOSQ_ERR_NOMEM;
			}
		}
		else {
			return MOSQ_ERR_NOMEM;
		}
	}
#ifdef WITH_BRIDGE
	if (context->bridge && context->bridge->start_type == bst_lazy
		&& context->sock == INVALID_SOCKET
		&& context->msg_count >= context->bridge->threshold) {

		context->bridge->lazy_reconnect = true;
	}
#endif

#ifdef WITH_WEBSOCKETS
	if (context->wsi && rc == 0) {
		return mqtt3_db_message_write(db, context);
	}
	else {
		return rc;
	}
#else
	return rc;
#endif
}

int mqtt3_db_message_update(struct mosquitto *context, uint16_t mid, enum mosquitto_msg_direction dir, enum mosquitto_msg_state state)
{
	struct mosquitto_client_msg *tail;

	tail = context->msgs;
	while (tail) {
		if (tail->mid == mid && tail->direction == dir) {
			tail->state = state;
			tail->timestamp = mosquitto_time();
			return MOSQ_ERR_SUCCESS;
		}
		tail = tail->next;
	}
	return MOSQ_ERR_NOT_FOUND;
}

int mqtt3_db_messages_delete(struct mosquitto_db *db, struct mosquitto *context)
{
	struct mosquitto_client_msg *tail, *next;

	if (!context) return MOSQ_ERR_INVAL;

	tail = context->msgs;
	while (tail) {
		mosquitto__db_msg_store_deref(db, &tail->store);
		next = tail->next;
		_mosquitto_free(tail);
		tail = next;
	}
	context->msgs = NULL;
	context->last_msg = NULL;
	context->msg_count = 0;
	context->msg_count12 = 0;

	return MOSQ_ERR_SUCCESS;
}

int mqtt3_db_messages_easy_queue(struct mosquitto_db *db, struct mosquitto *context, const char *topic, int qos, uint32_t payloadlen, const void *payload, int retain)
{
	struct mosquitto_msg_store *stored;
	char *source_id;

	assert(db);

	if (!topic) return MOSQ_ERR_INVAL;

	if (context && context->id) {
		source_id = context->id;
	}
	else {
		source_id = "";
	}
	
	if (mqtt3_db_message_store(db, source_id, 0, topic, qos, payloadlen, payload, retain, &stored, 0)) return 1;
	
	return mqtt3_db_messages_queue(db, source_id, topic, qos, retain, &stored);
}

int mqtt3_db_message_store(struct mosquitto_db *db, const char *source, uint16_t source_mid, const char *topic, int qos, uint32_t payloadlen, const void *payload, int retain, struct mosquitto_msg_store **stored, dbid_t store_id)
{
	struct mosquitto_msg_store *temp;

	assert(db);
	assert(stored);

	temp = _mosquitto_malloc(sizeof(struct mosquitto_msg_store));
	if (!temp) return MOSQ_ERR_NOMEM;

	temp->ref_count = 0;
	if (source) {
		temp->source_id = _mosquitto_strdup(source);
	}
	else {
		temp->source_id = _mosquitto_strdup("");
	}
	if (!temp->source_id) {
		_mosquitto_free(temp);
		_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
		return MOSQ_ERR_NOMEM;
	}
	temp->source_mid = source_mid;
	temp->mid = 0;
	temp->qos = qos;
	temp->retain = retain;
	if (topic) {
		temp->topic = _mosquitto_strdup(topic);
		if (!temp->topic) {
			_mosquitto_free(temp->source_id);
			_mosquitto_free(temp);
			_mosquitto_log_printf(NULL, MOSQ_LOG_ERR, "Error: Out of memory.");
			return MOSQ_ERR_NOMEM;
		}
	}
	else {
		temp->topic = NULL;
	}
	temp->payloadlen = payloadlen;
	if (payloadlen) {
		temp->payload = _mosquitto_malloc(sizeof(char)*payloadlen);
		if (!temp->payload) {
			if (temp->source_id) _mosquitto_free(temp->source_id);
			if (temp->topic) _mosquitto_free(temp->topic);
			if (temp->payload) _mosquitto_free(temp->payload);
			_mosquitto_free(temp);
			return MOSQ_ERR_NOMEM;
		}
		memcpy(temp->payload, payload, sizeof(char)*payloadlen);
	}
	else {
		temp->payload = NULL;
	}

	if (!temp->source_id || (payloadlen && !temp->payload)) {
		if (temp->source_id) _mosquitto_free(temp->source_id);
		if (temp->topic) _mosquitto_free(temp->topic);
		if (temp->payload) _mosquitto_free(temp->payload);
		_mosquitto_free(temp);
		return 1;
	}
	temp->dest_ids = NULL;
	temp->dest_id_count = 0;
	db->msg_store_count++;
	(*stored) = temp;

	if (!store_id) {
		temp->db_id = ++db->last_db_id;
	}
	else {
		temp->db_id = store_id;
	}

	mosquitto__db_msg_store_add(db, temp);

	return MOSQ_ERR_SUCCESS;
}

int mqtt3_db_message_store_find(struct mosquitto *context, uint16_t mid, struct mosquitto_msg_store **stored)
{
	struct mosquitto_client_msg *tail;

	if (!context) return MOSQ_ERR_INVAL;

	*stored = NULL;
	tail = context->msgs;
	while (tail) {
		if (tail->store->source_mid == mid && tail->direction == mosq_md_in) {
			*stored = tail->store;
			return MOSQ_ERR_SUCCESS;
		}
		tail = tail->next;
	}

	return 1;
}

/* Called on reconnect to set outgoing messages to a sensible state and force a
* retry, and to set incoming messages to expect an appropriate retry. */
int mqtt3_db_message_reconnect_reset(struct mosquitto_db *db, struct mosquitto *context)
{
	struct mosquitto_client_msg *msg;
	struct mosquitto_client_msg *prev = NULL;
	int count;

	msg = context->msgs;
	context->msg_count = 0;
	context->msg_count12 = 0;
	while (msg) {
		context->last_msg = msg;

		context->msg_count++;
		if (msg->qos > 0) {
			context->msg_count12++;
		}

		if (msg->direction == mosq_md_out) {
			if (msg->state != mosq_ms_queued) {
				switch (msg->qos) {
				case 0:
					msg->state = mosq_ms_publish_qos0;
					break;
				case 1:
					msg->state = mosq_ms_publish_qos1;
					break;
				case 2:
					if (msg->state == mosq_ms_wait_for_pubcomp) {
						msg->state = mosq_ms_resend_pubrel;
					}
					else {
						msg->state = mosq_ms_publish_qos2;
					}
					break;
				}
			}
		}
		else {
			if (msg->qos != 2) {
				/* Anything <QoS 2 can be completely retried by the client at
				* no harm. */
				_message_remove(db, context, &msg, prev);
			}
			else {
				/* Message state can be preserved here because it should match
				* whatever the client has got. */
			}
		}
		prev = msg;
		if (msg) msg = msg->next;
	}
	/* Messages received when the client was disconnected are put
	* in the mosq_ms_queued state. If we don't change them to the
	* appropriate "publish" state, then the queued messages won't
	* get sent until the client next receives a message - and they
	* will be sent out of order.
	*/
	if (context->msgs) {
		count = 0;
		msg = context->msgs;
		while (msg && (max_inflight == 0 || count < max_inflight)) {
			if (msg->state == mosq_ms_queued) {
				switch (msg->qos) {
				case 0:
					msg->state = mosq_ms_publish_qos0;
					break;
				case 1:
					msg->state = mosq_ms_publish_qos1;
					break;
				case 2:
					msg->state = mosq_ms_publish_qos2;
					break;
				}
			}
			msg = msg->next;
			count++;
		}
	}

	return MOSQ_ERR_SUCCESS;
}

int mqtt3_db_message_timeout_check(struct mosquitto_db *db, unsigned int timeout)
{
	time_t threshold;
	enum mosquitto_msg_state new_state;
	struct mosquitto *context, *ctxt_tmp;
	struct mosquitto_client_msg *msg;

	threshold = mosquitto_time() - timeout;

	HASH_ITER(hh_sock, db->contexts_by_sock, context, ctxt_tmp) {
		msg = context->msgs;
		while (msg) {
			new_state = mosq_ms_invalid;
			if (msg->timestamp < threshold && msg->state != mosq_ms_queued) {
				switch (msg->state) {
				case mosq_ms_wait_for_puback:
					new_state = mosq_ms_publish_qos1;
					break;
				case mosq_ms_wait_for_pubrec:
					new_state = mosq_ms_publish_qos2;
					break;
				case mosq_ms_wait_for_pubrel:
					new_state = mosq_ms_send_pubrec;
					break;
				case mosq_ms_wait_for_pubcomp:
					new_state = mosq_ms_resend_pubrel;
					break;
				default:
					break;
				}
				if (new_state != mosq_ms_invalid) {
					msg->timestamp = mosquitto_time();
					msg->state = new_state;
					msg->dup = true;
				}
			}
			msg = msg->next;
		}
	}

	return MOSQ_ERR_SUCCESS;
}

int mqtt3_db_message_release(struct mosquitto_db *db, struct mosquitto *context, uint16_t mid, enum mosquitto_msg_direction dir)
{
	struct mosquitto_client_msg *tail, *last = NULL;
	int qos;
	int retain;
	char *topic;
	char *source_id;
	int msg_index = 0;
	bool deleted = false;

	if (!context) return MOSQ_ERR_INVAL;

	tail = context->msgs;
	while (tail) {
		msg_index++;
		if (tail->state == mosq_ms_queued && msg_index <= max_inflight) {
			tail->timestamp = mosquitto_time();
			if (tail->direction == mosq_md_out) {
				switch (tail->qos) {
				case 0:
					tail->state = mosq_ms_publish_qos0;
					break;
				case 1:
					tail->state = mosq_ms_publish_qos1;
					break;
				case 2:
					tail->state = mosq_ms_publish_qos2;
					break;
				}
			}
			else {
				if (tail->qos == 2) {
					_mosquitto_send_pubrec(context, tail->mid);
					tail->state = mosq_ms_wait_for_pubrel;
				}
			}
		}
		if (tail->mid == mid && tail->direction == dir) {
			qos = tail->store->qos;
			topic = tail->store->topic;
			retain = tail->retain;
			source_id = tail->store->source_id;

			/* topic==NULL should be a QoS 2 message that was
			* denied/dropped and is being processed so the client doesn't
			* keep resending it. That means we don't send it to other
			* clients. */
			if (!topic || !mqtt3_db_messages_queue(db, source_id, topic, qos, retain, &tail->store)) {
				_message_remove(db, context, &tail, last);
				deleted = true;
			}
			else {
				return 1;
			}
		}
		else {
			last = tail;
			tail = tail->next;
		}
		if (msg_index > max_inflight && deleted) {
			return MOSQ_ERR_SUCCESS;
		}
	}
	if (deleted) {
		return MOSQ_ERR_SUCCESS;
	}
	else {
		return 1;
	}
}
char* makeBinary2(char c) {
	char binary[10];
	int index = 0;
	int max_num = 128;
	for (int i = 0; i < 9; i++) {
		if (i == 4) {
			binary[i] = ' ';
		}
		else {
			binary[i] = (c & max_num) ? '1' : '0';
			max_num /= 2;
		}
	}
	binary[9] = '\0';
	return binary;
}
void printBinary(void * payload, int payloadlen) {
	for (int i = 0; i < payloadlen; i++) {
		printf("~%c    -    %s~   %d %d  \n", ((char *)payload)[i], makeBinary2(((char *)payload)[i]), ((char *)payload)[i], i);
		//Sleep(300);
	}
}
int mqtt3_db_message_write(struct mosquitto_db *db, struct mosquitto *context)
{
	int rc;
	struct mosquitto_client_msg *tail, *last = NULL;
	uint16_t mid;
	int retries;
	int retain;
	const char *topic;
	int qos;
	int time_based_filter;
	uint32_t payloadlen;
	const void *payload;
	struct publish_data pub_data;
	int msg_count = 0;

	if (!context || context->sock == INVALID_SOCKET
		|| (context->state == mosq_cs_connected && !context->id)) {
		return MOSQ_ERR_INVAL;
	}

	if (context->state != mosq_cs_connected) {
		return MOSQ_ERR_SUCCESS;
	}

	tail = context->msgs;
	time_based_filter = context->time_based_filter;

	while (tail) {
		if (tail->direction == mosq_md_in) {
			msg_count++;
			printf("여기%d\n", msg_count);
		}
		if (tail->state != mosq_ms_queued) {
			mid = tail->mid;
			retries = tail->dup;
			retain = tail->retain;
			topic = tail->store->topic;
			qos = tail->qos;
			payloadlen = tail->store->payloadlen;
			payload = tail->store->payload;
			//printf("mqtt3_db_message_write : %d %d %s %s %d!!\n",qos, time_based_filter, topic, context->id, tail->state);
			switch (tail->state) {
			case mosq_ms_publish_qos0:
				if (context->time_based_filter_timer > 0 && context->time_based_filter_timer <= 255) {////////타임베이스 필터를 사용하는 경우						
																									  //if (!context->using_time_based_filter) {
																									  //printf("[mosquitto_connect_timet_based_filter] : [memory]%d [id]%s [time_based]%d [time_based_timer]%d\n", context, context->id, context->time_based_filter, context->time_based_filter_timer);					
					mosquitto_connect_timet_based_filter(*context, mid, topic, payloadlen, payload, qos, retain, retries);
					_message_remove(db, context, &tail, last);
					//printf("[mosquitto_connect_timet_based_filter2] : [memory]%d [id]%s [time_based]%d [time_based_timer]%d\n", context, context->id, context->time_based_filter, context->time_based_filter_timer);
					//}
				}
				else {///////////////////=추가 타임베이스 필터가 0인경우 바로 메세지 전송
					////=printf("여기들어옴\n");
					rc = _mosquitto_send_publish(context, mid, topic, payloadlen, payload, qos, retain, retries);
					if (!rc) {
						_message_remove(db, context, &tail, last);
					}
					else {
						return rc;
					}
				}
				break;
			case mosq_ms_publish_qos1:
				rc = _mosquitto_send_publish(context, mid, topic, payloadlen, payload, qos, retain, retries);
				if (!rc) {
					tail->timestamp = mosquitto_time();
					tail->dup = 1; /* Any retry attempts are a duplicate. */
					tail->state = mosq_ms_wait_for_puback;
				}
				else {
					return rc;
				}
				last = tail;
				tail = tail->next;
				break;

			case mosq_ms_publish_qos2:
				rc = _mosquitto_send_publish(context, mid, topic, payloadlen, payload, qos, retain, retries);
				if (!rc) {
					tail->timestamp = mosquitto_time();
					tail->dup = 1; /* Any retry attempts are a duplicate. */
					tail->state = mosq_ms_wait_for_pubrec;
				}
				else {
					return rc;
				}
				last = tail;
				tail = tail->next;
				break;

			case mosq_ms_send_pubrec:
				rc = _mosquitto_send_pubrec(context, mid);
				if (!rc) {
					tail->state = mosq_ms_wait_for_pubrel;
				}
				else {
					return rc;
				}
				last = tail;
				tail = tail->next;
				break;

			case mosq_ms_resend_pubrel:
				rc = _mosquitto_send_pubrel(context, mid);
				if (!rc) {
					tail->state = mosq_ms_wait_for_pubcomp;
				}
				else {
					return rc;
				}
				last = tail;
				tail = tail->next;
				break;

			case mosq_ms_resend_pubcomp:
				rc = _mosquitto_send_pubcomp(context, mid);
				if (!rc) {
					tail->state = mosq_ms_wait_for_pubrel;
				}
				else {
					return rc;
				}
				last = tail;
				tail = tail->next;
				break;

			default:
				last = tail;
				tail = tail->next;
				break;
			}
		}
		else {
			/* state == mosq_ms_queued */
			if (tail->direction == mosq_md_in && (max_inflight == 0 || msg_count < max_inflight)) {
				printf("else여기%d %d\n", msg_count, tail->qos);
				if (tail->qos == 2) {
					tail->state = mosq_ms_send_pubrec;
				}
			}
			else {
				last = tail;
				tail = tail->next;
			}
		}
	}

	return MOSQ_ERR_SUCCESS;
}

void mqtt3_db_limits_set(int inflight, int queued)
{
	max_inflight = inflight;
	max_queued = queued;
}

void mqtt3_db_vacuum(void)
{
	/* FIXME - reimplement? */
}



////////////////////////////////////////////=
void mosquitto_set_publish_data(struct publish_data *pub_data, struct mosquitto mosq, uint16_t mid, uint32_t payloadlen, int qos, bool retain, bool dup) {
	memcpy(&pub_data->context, &mosq, sizeof(mosq));

	pub_data->mid = mid;

	pub_data->retries = dup;

	pub_data->retain = retain;

	pub_data->qos = qos;

	pub_data->payloadlen = payloadlen;

	//printf("★[1]%s %d %d %d %d %d %d\n", pub_data->context.id, &pub_data->context, pub_data->mid, pub_data->payloadlen,
	//pub_data->qos, pub_data->retain, pub_data->retries);
}


static struct publish_data *context_head = NULL;
static struct publish_data *context_last = NULL;

pthread_mutex_t time_based_mutex;// = PTHREAD_MUTEX_INITIALIZER;////Pthread Mutex 초기화

void mosquitto_connect_timet_based_filter(struct mosquitto mosq, uint16_t mid, const char *topic, uint32_t payloadlen, const void *payload, int qos, bool retain, bool dup) {////값

	struct publish_data *pointer;
	struct publish_data *pub_data;
	int time_based_filter_timer_temp = 0;
	bool exist_context = 0;
	//printf("★[mosquitto_connect_timet_based_filter] : [memory]%d [id]%s [time_based]%d [time_based_timer]%d\n", &context, context->id, context->time_based_filter, context->time_based_filter_timer);

	pthread_mutex_lock(&time_based_mutex);

	//printf("★[mosq id] : %s %d연결\n", mosq.id, mosq.time_based_filter);
	if (!context_head) {//맨 처음인경우(비어있는경우)
		pub_data = _mosquitto_malloc(sizeof(struct publish_data));
		mosquitto_set_publish_data(pub_data, mosq, mid, payloadlen, qos, retain, dup);////pub_data를 채워준다.

																					  //여긴 처음인경우
		pub_data->topic = _mosquitto_malloc(strlen(topic));
		pub_data->payload = _mosquitto_malloc(payloadlen);
		memcpy(pub_data->topic, topic, strlen(topic) + 1);
		memcpy(pub_data->payload, payload, payloadlen);
		//printBinary(pub_data->payload, payloadlen);
		context_head = pub_data;
		context_head->next = NULL;
		context_head->prev = NULL;
		context_last = context_head;
		printf("빔!\n");
	}
	else {//Context가 존재하는 경우
		pointer = context_head;
		while (pointer) {
			if (!strcmp(pointer->context.id, mosq.id)) {//같은 id를 가진 Context가 존재하면
														//printf("★pointer존재 : %s %s\n",pointer->context.id, mosq.id);
				time_based_filter_timer_temp = pointer->context.time_based_filter_timer;
				mosquitto_set_publish_data(pointer, mosq, mid, payloadlen, qos, retain, dup);////pub_data를 채워준다.

				pointer->topic = realloc(pointer->topic, strlen(topic));//토픽 재할당
				pointer->payload = realloc(pointer->payload, payloadlen);//페이로드 재할당

				memcpy(pointer->payload, payload, payloadlen);//페이로드 복사
				memcpy(pointer->topic, topic, strlen(topic) + 1);//토픽 복사

				pointer->context.time_based_filter_timer = time_based_filter_timer_temp;//줄이던 타이머 재설정
				exist_context = 1;//데이터가 존재한다.
				break;
			}
			pointer = pointer->next;
		}
		if (!exist_context) {//같은 id를 가진 context가 존재하지 않으면
			pub_data = _mosquitto_malloc(sizeof(struct publish_data));
			mosquitto_set_publish_data(pub_data, mosq, mid, payloadlen, qos, retain, dup);////pub_data를 채워준다.
																						  //printf("pointer존재안함 : %s \n", pub_data->context.id);
																						  //여긴 처음인경우

																						 
			pub_data->topic = _mosquitto_malloc(strlen(topic));
			pub_data->payload = _mosquitto_malloc(payloadlen);
			memcpy(pub_data->topic, topic, strlen(topic) + 1);
			memcpy(pub_data->payload, payload, payloadlen);

			context_last->next = pub_data;
			pub_data->prev = context_last;
			pub_data->next = NULL;
			context_last = pub_data;
		}
	}
	///////=printf("★★size : %d\n", sizeof(*pub_data));
	pthread_mutex_unlock(&time_based_mutex);

}
void mosquitto_disconnect_timet_based_filter(struct publish_data *pub_data) {
	pub_data->context.time_based_filter_timer = pub_data->context.time_based_filter;
	if (context_head == pub_data) {
		printf("disconnect err 1\n");
		if (pub_data->next == NULL) {
			printf("disconnect err 2\n");
			context_head = NULL;
			return;
		}
		else {
			context_head = pub_data->next;
			context_head->prev = NULL;
			printf("disconnect err 3\n");
		}
	}
	else if (context_last == pub_data) {//지울 것이 마지막 이면
		context_last->prev->next = NULL;
		context_last = context_last->prev;
	}
	else {
		pub_data->prev->next = pub_data->next;
		pub_data->next->prev = pub_data->prev;	
	}
	_mosquitto_free(pub_data->topic);//할당한 토픽 자원 반납
	_mosquitto_free(pub_data->payload);//할당한 페이로드 자원 반납
	_mosquitto_free(pub_data);//데이터 구조체 자원 반납
}

void *mosquitto_time_based_filter(void *ptr) {
	struct publish_data *pub_data;
	int rc;
	pthread_mutex_init(&time_based_mutex, NULL);
	while (1) {
		pthread_mutex_lock(&time_based_mutex);
		pub_data = context_head;


		while (pub_data) {
			/////==printf("★%s의 time_based_filter : %d★\n", pub_data->context.id, pub_data->context.time_based_filter_timer);

			if (pub_data->context.time_based_filter_timer <= 0) {
				/*	printf("★[2]%s %d %d %s %d %d %d %d\n",pub_data->context.id, &pub_data->context, pub_data->mid, pub_data->topic, pub_data->payloadlen,
				pub_data->qos, pub_data->retain, pub_data->retries);*/

				////////////////여길 send_publish가 아니라 mqtt3_db_write를 부르는거로 짜보자~!!

				rc = _mosquitto_send_publish(&pub_data->context, pub_data->mid, pub_data->topic, pub_data->payloadlen,
					pub_data->payload, pub_data->qos, pub_data->retain, pub_data->retries);
				if (!rc) {
					//printf("★pub 성공\n");
					mosquitto_disconnect_timet_based_filter(pub_data);
				}
				else {//////////////==여기로 갈 때도 있네! 조심 고쳐보자
					//printf("★pub 실패\n");
					mosquitto_disconnect_timet_based_filter(pub_data);
					break;
				}

			}
			else {
				pub_data->context.time_based_filter_timer -= 1;//200이면 10ms마다 1씩줄임 = 2000ms = 2초 후 메세지 전송				
			}
			pub_data = pub_data->next;
		}
		pthread_mutex_unlock(&time_based_mutex);///한바퀴 도는 걸 뮤텍스락 걸어놓기?

#ifdef WIN32
		Sleep(10);
#else
		usleep(1000);
#endif
	}
}
void mosquitto_time_based_filter_start() {
	printf("---------------------time_based_filter start!---------------------\n");
	pthread_t thread;
	int mrc = 0;
	void *status = NULL;
	pthread_attr_t attr; // attributes for a thread

	pthread_attr_init(&attr); // get default attributes

	pthread_create(&thread, &attr, mosquitto_time_based_filter, NULL);

	printf("Main thread create mosquitto_time_based_filter thread\n");
	printf("------------------------------------------------------------------\n");
}
