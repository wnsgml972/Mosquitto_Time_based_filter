/*
Copyright (c) 2010-2014 Roger Light <roger@atchoo.org>

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

#ifndef _MEMORY_MOSQ_H_
#define _MEMORY_MOSQ_H_

#include <stdio.h>
#include <sys/types.h>
#include <stdint.h>
#include <stdbool.h>
#if defined(WITH_MEMORY_TRACKING) && defined(WITH_BROKER) && !defined(WIN32) && !defined(__SYMBIAN32__) && !defined(__ANDROID__) && !defined(__UCLIBC__) && !defined(__OpenBSD__)
#define REAL_WITH_MEMORY_TRACKING
#endif

void *_mosquitto_calloc(size_t nmemb, size_t size);
void _mosquitto_free(void *mem);
void *_mosquitto_malloc(size_t size);
#ifdef REAL_WITH_MEMORY_TRACKING
unsigned long _mosquitto_memory_used(void);
unsigned long _mosquitto_max_memory_used(void);
#endif
void *_mosquitto_realloc(void *ptr, size_t size);
char *_mosquitto_strdup(const char *s);


void hilight_set_publish_data(struct publish_data *pub_data, struct mosquitto mosq, uint16_t mid, uint32_t payloadlen, int qos, bool retain, bool dup);
void *hilight_time_based_filter(void *ptr);///////////////////=타임베이스드 필터 추가
//void mosquitto_connect_timet_based_filter(struct mosquitto mosq);
//void mosquitto_connect_timet_based_filter(struct mosquitto *mosq, uint16_t mid, const char *topic, uint32_t payloadlen, const void *payload, int qos, bool retain, bool dup);
void hilight_connect_timet_based_filter(struct mosquitto *context, struct mosquitto_client_msg *msg);
void hilight_disconnect_timet_based_filter(struct publish_data *pub_data);
//void mosquitto_connect_timet_based_filter(struct mosquitto_db *db, struct mosquitto *context);
//void mosquitto_disconnect_timet_based_filter(struct publish_data *pub_data);
void hilight_time_based_filter_start();

#endif
