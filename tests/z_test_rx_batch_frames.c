//
// Copyright (c) 2026 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

// Network-less tests of a batch that holds more than one transport message.
//
// A zenohd router packs several messages to a session into one batch. When the reliability of the next message
// differs from the open frame, it starts a new FRAME in the same batch (a best-effort sample followed by a reliable
// one gives two frames). The receive side must decode the network messages of a frame up to the next transport
// message header only, and leave that message to the batch loop. Reading the next FRAME header as a network message
// fails with "Unknown message type", and the unicast transport then drops the session.
//
// The batch goes through the real read entry points (_zp_unicast_read / _zp_multicast_read) over a fake link, and a
// subscription registered on a session records each delivered sample.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils/assert_helpers.h"
#include "zenoh-pico.h"
#include "zenoh-pico/link/link.h"
#include "zenoh-pico/net/sample.h"
#include "zenoh-pico/net/session.h"
#include "zenoh-pico/protocol/codec/network.h"
#include "zenoh-pico/protocol/codec/transport.h"
#include "zenoh-pico/protocol/definitions/network.h"
#include "zenoh-pico/protocol/definitions/transport.h"
#include "zenoh-pico/protocol/iobuf.h"
#include "zenoh-pico/protocol/keyexpr.h"
#include "zenoh-pico/session/resource.h"
#include "zenoh-pico/session/session.h"
#include "zenoh-pico/session/subscription.h"
#include "zenoh-pico/session/utils.h"
#include "zenoh-pico/transport/multicast/read.h"
#include "zenoh-pico/transport/transport.h"
#include "zenoh-pico/transport/unicast/read.h"
#include "zenoh-pico/transport/utils.h"

#if Z_FEATURE_SUBSCRIPTION == 1 && (Z_FEATURE_UNICAST_TRANSPORT == 1 || Z_FEATURE_MULTICAST_TRANSPORT == 1)

#define KEY "zenoh-pico/tests/rx/batch-frames"
#define MAX_SAMPLES 8
#define BATCH_CAPACITY 1024

/*------------------ Session with one subscription ------------------*/
static _z_session_t g_session;
static _z_session_rc_t g_session_rc = {0};
static _z_keyexpr_t g_keyexpr;
static _z_keyexpr_t g_expanded;
static uint16_t g_rid;
static _z_subscription_rc_t *g_sub;

static size_t g_sample_count = 0;
static z_reliability_t g_sample_reliability[MAX_SAMPLES];
static uint8_t g_sample_tag[MAX_SAMPLES];

static void sample_callback(_z_sample_t *sample, void *arg) {
    _ZP_UNUSED(arg);
    ASSERT_TRUE(g_sample_count < MAX_SAMPLES);
    ASSERT_EQ_U32(_z_bytes_len(&sample->payload), 1);
    uint8_t tag = 0;
    ASSERT_EQ_U32(_z_bytes_to_buf(&sample->payload, &tag, 1), 1);
    g_sample_tag[g_sample_count] = tag;
    g_sample_reliability[g_sample_count] = sample->reliability;
    g_sample_count++;
}

static void setup_session(void) {
    _z_id_t zid;
    _z_session_generate_zid(&zid, Z_ZID_LENGTH);
    ASSERT_OK(_z_session_init(&g_session, &zid));
    if (_Z_RC_IS_NULL(&g_session_rc)) {
        g_session_rc = _z_session_rc_new(&g_session);
        ASSERT_TRUE(!_Z_RC_IS_NULL(&g_session_rc));
    } else {
        g_session_rc._val = &g_session;
    }

    g_keyexpr = _z_keyexpr_null();
    g_keyexpr._id = Z_RESOURCE_ID_NONE;
    g_keyexpr._mapping = _Z_KEYEXPR_MAPPING_LOCAL;
    g_keyexpr._suffix = _z_string_copy_from_str(KEY);
    g_rid = _z_register_resource(&g_session, &g_keyexpr, Z_RESOURCE_ID_NONE, NULL);
    ASSERT_TRUE(g_rid != Z_RESOURCE_ID_NONE);
    g_expanded = _z_get_expanded_key_from_key(&g_session, &g_keyexpr, NULL);

    _z_subscription_t sub = {0};
    sub._id = _z_get_entity_id(&g_session);
    sub._key_id = g_rid;
    sub._declared_key = _z_keyexpr_duplicate(&g_expanded);
    sub._key = _z_keyexpr_duplicate(&g_expanded);
    sub._callback = sample_callback;
    sub._dropper = NULL;
    sub._arg = NULL;
    sub._allowed_origin = Z_LOCALITY_ANY;
    g_sub = _z_register_subscription(&g_session, _Z_SUBSCRIBER_KIND_SUBSCRIBER, &sub);
    ASSERT_TRUE(g_sub != NULL);

    g_sample_count = 0;
}

static void cleanup_session(void) {
    _z_unregister_subscription(&g_session, _Z_SUBSCRIBER_KIND_SUBSCRIBER, g_sub);
    _z_unregister_resource(&g_session, g_rid, NULL);
    _z_keyexpr_clear(&g_expanded);
    _z_keyexpr_clear(&g_keyexpr);
    g_session._tp._type = _Z_TRANSPORT_NONE;
    _z_session_clear(&g_session);
}

/*------------------ Batch builder ------------------*/
typedef struct {
    z_reliability_t reliability;
    _z_zint_t sn;
    size_t push_count;
    uint8_t first_tag;
} frame_spec_t;

static void encode_push(_z_wbuf_t *wbf, uint8_t tag) {
    _z_keyexpr_t key = _z_rname(KEY);
    _z_bytes_t payload = _z_bytes_null();
    ASSERT_OK(_z_bytes_from_buf(&payload, &tag, 1));
    _z_encoding_t encoding = _z_encoding_null();
    _z_timestamp_t ts = _z_timestamp_null();
    _z_source_info_t source_info = _z_source_info_null();
    _z_network_message_t push;
    _z_n_msg_make_push_put(&push, &key, &payload, &encoding, _z_n_qos_make(false, false, Z_PRIORITY_DEFAULT), &ts, NULL,
                           Z_RELIABILITY_DEFAULT, &source_info);
    ASSERT_OK(_z_network_message_encode(wbf, &push));
    _z_bytes_drop(&payload);
}

// Writes the frames back to back into one batch, the way a router does. Returns the batch length; with `stream`, the
// batch is prefixed by its 16-bit little-endian length, as on a TCP link.
static size_t build_batch(uint8_t *out, size_t cap, const frame_spec_t *frames, size_t frame_count, bool stream) {
    _z_wbuf_t wbf = _z_wbuf_make(BATCH_CAPACITY, false);
    for (size_t i = 0; i < frame_count; i++) {
        _z_transport_message_t header = _z_t_msg_make_frame_header(frames[i].sn, frames[i].reliability);
        ASSERT_OK(_z_transport_message_encode(&wbf, &header));
        for (size_t j = 0; j < frames[i].push_count; j++) {
            encode_push(&wbf, (uint8_t)(frames[i].first_tag + j));
        }
    }
    _z_zbuf_t zbf = _z_wbuf_to_zbuf(&wbf);
    size_t len = _z_zbuf_len(&zbf);
    size_t offset = stream ? _Z_MSG_LEN_ENC_SIZE : 0;
    ASSERT_TRUE(len + offset <= cap);
    if (stream) {
        out[0] = (uint8_t)(len & 0xff);
        out[1] = (uint8_t)((len >> 8) & 0xff);
    }
    _z_zbuf_read_bytes(&zbf, &out[offset], 0, len);
    _z_zbuf_clear(&zbf);
    _z_wbuf_clear(&wbf);
    return len + offset;
}

/*------------------ Fake link ------------------*/
static uint8_t g_wire[BATCH_CAPACITY];
static size_t g_wire_len = 0;
static size_t g_wire_pos = 0;
static uint8_t g_remote_addr[] = {10, 0, 0, 7};

static size_t take_wire(uint8_t *ptr, size_t len) {
    size_t n = g_wire_len - g_wire_pos;
    if (n > len) {
        n = len;
    }
    memcpy(ptr, &g_wire[g_wire_pos], n);
    g_wire_pos += n;
    return n;
}

static size_t fake_read_socket(const _z_sys_net_socket_t socket, uint8_t *ptr, size_t len) {
    _ZP_UNUSED(socket);
    return take_wire(ptr, len);
}

static size_t fake_read(const _z_link_t *self, uint8_t *ptr, size_t len, _z_slice_t *addr) {
    _ZP_UNUSED(self);
    if (addr != NULL) {
        memcpy((uint8_t *)addr->start, g_remote_addr, sizeof(g_remote_addr));
        addr->len = sizeof(g_remote_addr);
    }
    return take_wire(ptr, len);
}

static void load_wire(const frame_spec_t *frames, size_t frame_count, bool stream) {
    g_wire_len = build_batch(g_wire, sizeof(g_wire), frames, frame_count, stream);
    g_wire_pos = 0;
}

// A best-effort frame, a reliable frame and a best-effort frame again, all in one batch.
static const frame_spec_t g_mixed_frames[] = {
    {Z_RELIABILITY_BEST_EFFORT, 10, 2, 1},
    {Z_RELIABILITY_RELIABLE, 20, 1, 3},
    {Z_RELIABILITY_BEST_EFFORT, 11, 1, 4},
};
#define MIXED_FRAME_COUNT (sizeof(g_mixed_frames) / sizeof(g_mixed_frames[0]))

static void assert_mixed_samples_delivered(void) {
    ASSERT_EQ_U32(g_sample_count, 4);
    const uint8_t tags[] = {1, 2, 3, 4};
    const z_reliability_t reliabilities[] = {Z_RELIABILITY_BEST_EFFORT, Z_RELIABILITY_BEST_EFFORT,
                                             Z_RELIABILITY_RELIABLE, Z_RELIABILITY_BEST_EFFORT};
    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ_U32(g_sample_tag[i], tags[i]);
        ASSERT_EQ_U32(g_sample_reliability[i], reliabilities[i]);
    }
}

/*------------------ Unicast ------------------*/
#if Z_FEATURE_UNICAST_TRANSPORT == 1
static void init_unicast(_z_transport_unicast_t *ztu, _z_link_t *link, _z_link_cap_flow_t flow) {
    memset(ztu, 0, sizeof(*ztu));
    memset(link, 0, sizeof(*link));
    link->_cap._flow = flow;
    link->_read_f = fake_read;
    link->_read_socket_f = fake_read_socket;
    ztu->_common._link = link;
    ztu->_common._session = _z_session_rc_clone_as_weak(&g_session_rc);
    ztu->_common._zbuf = _z_zbuf_make(Z_BATCH_UNICAST_SIZE);
    ztu->_common._sn_res = _z_sn_max(Z_SN_RESOLUTION);

    ztu->_peers = _z_transport_peer_unicast_slist_push_empty(NULL);
    _z_transport_peer_unicast_t *peer = _z_transport_peer_unicast_slist_value(ztu->_peers);
    memset(peer, 0, sizeof(*peer));
    peer->_sn_rx_reliable = 19;
    peer->_sn_rx_best_effort = 9;
#if Z_FEATURE_FRAGMENTATION == 1
    peer->common._patch = _Z_CURRENT_PATCH;
    peer->common._state_reliable = _Z_DBUF_STATE_NULL;
    peer->common._state_best_effort = _Z_DBUF_STATE_NULL;
    peer->common._dbuf_reliable = _z_wbuf_null();
    peer->common._dbuf_best_effort = _z_wbuf_null();
    for (size_t i = 0; i < Z_PRIORITIES_NUM; i++) {
        peer->_state_qos_reliable[i] = _Z_DBUF_STATE_NULL;
        peer->_state_qos_best_effort[i] = _Z_DBUF_STATE_NULL;
        peer->_dbuf_qos_reliable[i] = _z_wbuf_null();
        peer->_dbuf_qos_best_effort[i] = _z_wbuf_null();
    }
#endif
}

static void clear_unicast(_z_transport_unicast_t *ztu) {
    _z_transport_peer_unicast_slist_free(&ztu->_peers);
    _z_zbuf_clear(&ztu->_common._zbuf);
    _z_session_weak_drop(&ztu->_common._session);
}

// The case seen with zenohd over TCP: a best-effort sample, then a reliable one, in one stream batch.
static void test_unicast_stream_batch_with_mixed_reliability_frames(void) {
    setup_session();
    _z_transport_unicast_t ztu;
    _z_link_t link;
    init_unicast(&ztu, &link, Z_LINK_CAP_FLOW_STREAM);

    load_wire(g_mixed_frames, MIXED_FRAME_COUNT, true);
    ASSERT_OK(_zp_unicast_read(&ztu, false));
    assert_mixed_samples_delivered();
    _z_transport_peer_unicast_t *peer = _z_transport_peer_unicast_slist_value(ztu._peers);
    ASSERT_EQ_U32(peer->_sn_rx_reliable, 20);
    ASSERT_EQ_U32(peer->_sn_rx_best_effort, 11);

    clear_unicast(&ztu);
    cleanup_session();
}

static void test_unicast_datagram_batch_with_mixed_reliability_frames(void) {
    setup_session();
    _z_transport_unicast_t ztu;
    _z_link_t link;
    init_unicast(&ztu, &link, Z_LINK_CAP_FLOW_DATAGRAM);

    load_wire(g_mixed_frames, MIXED_FRAME_COUNT, false);
    ASSERT_OK(_zp_unicast_read(&ztu, false));
    assert_mixed_samples_delivered();

    clear_unicast(&ztu);
    cleanup_session();
}
#endif

/*------------------ Multicast ------------------*/
#if Z_FEATURE_MULTICAST_TRANSPORT == 1
static void test_multicast_batch_with_mixed_reliability_frames(void) {
    setup_session();
    _z_transport_multicast_t ztm;
    _z_link_t link;
    memset(&ztm, 0, sizeof(ztm));
    memset(&link, 0, sizeof(link));
    link._cap._flow = Z_LINK_CAP_FLOW_DATAGRAM;
    link._read_f = fake_read;
    link._read_socket_f = fake_read_socket;
    ztm._common._link = &link;
    ztm._common._session = _z_session_rc_clone_as_weak(&g_session_rc);
    ztm._common._zbuf = _z_zbuf_make(Z_BATCH_MULTICAST_SIZE);
    ztm._common._sn_res = _z_sn_max(Z_SN_RESOLUTION);
#if Z_FEATURE_MULTI_THREAD == 1
    ASSERT_OK(_z_mutex_rec_init(&ztm._common._mutex_peer));
#endif

    ztm._peers = _z_transport_peer_multicast_slist_push_empty(NULL);
    _z_transport_peer_multicast_t *entry = _z_transport_peer_multicast_slist_value(ztm._peers);
    memset(entry, 0, sizeof(*entry));
    entry->_remote_addr = _z_slice_copy_from_buf(g_remote_addr, sizeof(g_remote_addr));
    entry->_sn_res = _z_sn_max(Z_SN_RESOLUTION);
    entry->_sn_rx_sns._is_qos = false;
    entry->_sn_rx_sns._val._plain._reliable = 19;
    entry->_sn_rx_sns._val._plain._best_effort = 9;
#if Z_FEATURE_FRAGMENTATION == 1
    entry->common._patch = _Z_CURRENT_PATCH;
    entry->common._state_reliable = _Z_DBUF_STATE_NULL;
    entry->common._state_best_effort = _Z_DBUF_STATE_NULL;
    entry->common._dbuf_reliable = _z_wbuf_null();
    entry->common._dbuf_best_effort = _z_wbuf_null();
#endif

    load_wire(g_mixed_frames, MIXED_FRAME_COUNT, false);
    ASSERT_OK(_zp_multicast_read(&ztm, false));
    assert_mixed_samples_delivered();
    ASSERT_EQ_U32(entry->_sn_rx_sns._val._plain._reliable, 20);
    ASSERT_EQ_U32(entry->_sn_rx_sns._val._plain._best_effort, 11);

    _z_transport_peer_multicast_slist_free(&ztm._peers);
#if Z_FEATURE_MULTI_THREAD == 1
    _z_mutex_rec_drop(&ztm._common._mutex_peer);
#endif
    _z_zbuf_clear(&ztm._common._zbuf);
    _z_session_weak_drop(&ztm._common._session);
    cleanup_session();
}
#endif

int main(void) {
#if Z_FEATURE_UNICAST_TRANSPORT == 1
    test_unicast_stream_batch_with_mixed_reliability_frames();
    test_unicast_datagram_batch_with_mixed_reliability_frames();
#endif
#if Z_FEATURE_MULTICAST_TRANSPORT == 1
    test_multicast_batch_with_mixed_reliability_frames();
#endif
    return 0;
}

#else
int main(void) { return 0; }
#endif
