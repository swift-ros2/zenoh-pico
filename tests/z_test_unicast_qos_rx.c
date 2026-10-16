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

// Network-less tests of the unicast QoS negotiation and of the receive path that depends on it.
//
// A zenohd (1.x) router that has no QoS on a session keeps a single transmission queue for it. When a droppable
// message (best effort, or CongestionControl::Drop) cannot get a batch in time, the router marks that queue
// congested under Priority::Data, but clears the flag under Priority::Control when a batch is refilled. The flag then
// never clears, and the router silently discards every later droppable message to the session: one dropped large
// message stalls all following fragmented messages for good. Announcing QoS gives the session one queue per
// priority, where the flag is set and cleared under the same priority. With QoS, every priority has its own SN space,
// so the receive side must track SNs and defragmentation buffers per priority.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils/assert_helpers.h"
#include "zenoh-pico.h"
#include "zenoh-pico/protocol/codec/transport.h"
#include "zenoh-pico/protocol/definitions/transport.h"
#include "zenoh-pico/protocol/ext.h"
#include "zenoh-pico/protocol/iobuf.h"
#include "zenoh-pico/transport/transport.h"
#include "zenoh-pico/transport/unicast/rx.h"
#include "zenoh-pico/transport/utils.h"

#if Z_FEATURE_UNICAST_TRANSPORT == 1 && Z_FEATURE_FRAGMENTATION == 1

#define PRIO_CONTROL 0
#define PRIO_REAL_TIME 1
#define PRIO_DATA 5
#define PAYLOAD_LEN 16

static uint8_t g_payload[PAYLOAD_LEN] = {0xAA};

// Decodes bytes as they come off the wire. The message may alias `zbf`: clear the message before `zbf`.
static _z_transport_message_t decode_bytes(_z_zbuf_t *zbf, const uint8_t *bytes, size_t len) {
    *zbf = _z_zbuf_make(len);
    memcpy(_z_zbuf_get_wptr(zbf), bytes, len);
    _z_zbuf_set_wpos(zbf, len);
    _z_transport_message_t msg;
    ASSERT_OK(_z_transport_message_decode(&msg, zbf));
    return msg;
}

// The InitSyn announces QoS, and a router's InitAck that carries it is understood.
static void test_init_qos_roundtrip(void) {
    _z_id_t zid = _z_id_empty();
    zid.id[0] = 1;
    _z_transport_message_t syn = _z_t_msg_make_init_syn(Z_WHATAMI_CLIENT, zid);
    ASSERT_TRUE(syn._body._init._qos);

    _z_wbuf_t wbf = _z_wbuf_make(128, false);
    ASSERT_OK(_z_transport_message_encode(&wbf, &syn));
    _z_zbuf_t zbf = _z_wbuf_to_zbuf(&wbf);
    _z_transport_message_t decoded;
    ASSERT_OK(_z_transport_message_decode(&decoded, &zbf));
    ASSERT_TRUE(decoded._body._init._qos);
    ASSERT_EQ_U32(decoded._body._init._patch, _Z_CURRENT_PATCH);
    _z_t_msg_clear(&decoded);
    _z_zbuf_clear(&zbf);
    _z_wbuf_clear(&wbf);
    _z_t_msg_clear(&syn);

    // An InitAck only carries QoS when the listener accepted it.
    _z_transport_message_t ack = _z_t_msg_make_init_ack(Z_WHATAMI_PEER, zid, _z_slice_null());
    ASSERT_FALSE(ack._body._init._qos);
    wbf = _z_wbuf_make(128, false);
    ASSERT_OK(_z_transport_message_encode(&wbf, &ack));
    zbf = _z_wbuf_to_zbuf(&wbf);
    ASSERT_OK(_z_transport_message_decode(&decoded, &zbf));
    ASSERT_FALSE(decoded._body._init._qos);
    _z_t_msg_clear(&decoded);
    _z_zbuf_clear(&zbf);
    _z_wbuf_clear(&wbf);
    _z_t_msg_clear(&ack);
}

// A router sends the priority of a Frame or Fragment as a mandatory QoS extension once QoS is on. Decoding it must
// not fail as an unknown mandatory extension.
static void test_frame_and_fragment_qos_extension(void) {
    uint8_t frame[] = {
        (uint8_t)(_Z_MID_T_FRAME | _Z_FLAG_T_FRAME_R | _Z_FLAG_T_Z),
        0x07,                     // SN
        _Z_MSG_EXT_ID_FRAME_QOS,  // QoS extension, last one
        PRIO_CONTROL,
    };
    _z_zbuf_t zbf;
    _z_transport_message_t msg = decode_bytes(&zbf, frame, sizeof(frame));
    ASSERT_EQ_U32(msg._body._frame._sn, 7);
    ASSERT_EQ_U32(msg._body._frame._priority, PRIO_CONTROL);
    _z_t_msg_clear(&msg);
    _z_zbuf_clear(&zbf);

    uint8_t plain_frame[] = {(uint8_t)(_Z_MID_T_FRAME | _Z_FLAG_T_FRAME_R), 0x08};
    msg = decode_bytes(&zbf, plain_frame, sizeof(plain_frame));
    ASSERT_EQ_U32(msg._body._frame._priority, PRIO_DATA);
    _z_t_msg_clear(&msg);
    _z_zbuf_clear(&zbf);

    uint8_t fragment[] = {
        (uint8_t)(_Z_MID_T_FRAGMENT | _Z_FLAG_T_FRAGMENT_R | _Z_FLAG_T_FRAGMENT_M | _Z_FLAG_T_Z),
        0x09,                                                       // SN
        (uint8_t)(_Z_MSG_EXT_ID_FRAGMENT_QOS | _Z_MSG_EXT_FLAG_Z),  // QoS extension, more follow
        PRIO_REAL_TIME,
        _Z_MSG_EXT_ID_FRAGMENT_FIRST,  // first marker, last one
        0x01,
        0x02,
        0x03,
    };
    msg = decode_bytes(&zbf, fragment, sizeof(fragment));
    ASSERT_EQ_U32(msg._body._fragment._sn, 9);
    ASSERT_EQ_U32(msg._body._fragment._priority, PRIO_REAL_TIME);
    ASSERT_TRUE(msg._body._fragment.first);
    ASSERT_EQ_U32(msg._body._fragment._payload.len, 3);
    _z_t_msg_clear(&msg);
    _z_zbuf_clear(&zbf);
}

static void init_transport(_z_transport_unicast_t *ztu, _z_transport_peer_unicast_t *peer, bool is_qos,
                           _z_zint_t initial_sn) {
    memset(ztu, 0, sizeof(*ztu));
    memset(peer, 0, sizeof(*peer));
    ztu->_common._sn_res = _z_sn_max(Z_SN_RESOLUTION);
    _z_zint_t sn = _z_sn_decrement(ztu->_common._sn_res, initial_sn);
    peer->_sn_rx_reliable = sn;
    peer->_sn_rx_best_effort = sn;
    peer->_is_qos = is_qos;
    for (size_t i = 0; i < Z_PRIORITIES_NUM; i++) {
        peer->_sn_rx_qos_reliable[i] = sn;
        peer->_sn_rx_qos_best_effort[i] = sn;
        peer->_state_qos_reliable[i] = _Z_DBUF_STATE_NULL;
        peer->_state_qos_best_effort[i] = _Z_DBUF_STATE_NULL;
        peer->_dbuf_qos_reliable[i] = _z_wbuf_null();
        peer->_dbuf_qos_best_effort[i] = _z_wbuf_null();
    }
    peer->common._patch = _Z_CURRENT_PATCH;
    peer->common._state_reliable = _Z_DBUF_STATE_NULL;
    peer->common._state_best_effort = _Z_DBUF_STATE_NULL;
    peer->common._dbuf_reliable = _z_wbuf_null();
    peer->common._dbuf_best_effort = _z_wbuf_null();
}

// Feeds one non-final fragment (it never completes a message, so no session is needed).
static void feed_fragment(_z_transport_unicast_t *ztu, _z_transport_peer_unicast_t *peer, z_reliability_t reliability,
                          uint8_t priority, _z_zint_t sn, bool first) {
    _z_transport_message_t msg = _z_t_msg_make_fragment(sn, _z_slice_alias_buf(g_payload, PAYLOAD_LEN), reliability,
                                                        false, first, false);
    msg._body._fragment._priority = priority;
    ASSERT_OK(_z_unicast_handle_transport_message(ztu, &msg, peer));
}

// Feeds one empty frame: it only moves the SN of its channel.
static void feed_empty_frame(_z_transport_unicast_t *ztu, _z_transport_peer_unicast_t *peer,
                             z_reliability_t reliability, uint8_t priority, _z_zint_t sn) {
    _z_zbuf_t empty = _z_zbuf_make(1);
    _z_transport_message_t msg = _z_t_msg_make_frame(sn, &empty, reliability);
    msg._body._frame._priority = priority;
    ASSERT_OK(_z_unicast_handle_transport_message(ztu, &msg, peer));
    _z_zbuf_clear(&empty);
}

// With QoS, a Control frame carries an SN of its own space. It must not be taken as an out-of-order message of the
// Data channel (which would drop the Data message being reassembled), and fragments of another priority must not
// mix into the Data defragmentation buffer.
static void test_qos_priorities_keep_their_own_channel(void) {
    _z_transport_unicast_t ztu;
    _z_transport_peer_unicast_t peer;
    init_transport(&ztu, &peer, true, 100);

    feed_fragment(&ztu, &peer, Z_RELIABILITY_RELIABLE, PRIO_DATA, 100, true);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_RELIABLE, PRIO_DATA, 101, false);
    feed_empty_frame(&ztu, &peer, Z_RELIABILITY_RELIABLE, PRIO_CONTROL, 100);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_RELIABLE, PRIO_REAL_TIME, 100, true);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_RELIABLE, PRIO_DATA, 102, false);

    ASSERT_EQ_U32(_z_wbuf_len(&peer.common._dbuf_reliable), 3 * PAYLOAD_LEN);
    ASSERT_EQ_U32(_z_wbuf_len(&peer._dbuf_qos_reliable[PRIO_REAL_TIME]), PAYLOAD_LEN);
    ASSERT_EQ_U32(peer._sn_rx_reliable, 102);
    ASSERT_EQ_U32(peer._sn_rx_qos_reliable[PRIO_CONTROL], 100);
    ASSERT_EQ_U32(peer._sn_rx_qos_reliable[PRIO_REAL_TIME], 100);

    _z_transport_peer_unicast_clear(&peer);
}

// A lost fragment (an SN gap) loses the message being reassembled, but the fragment after the gap that starts a new
// message must be kept, so the next message still arrives.
static void test_gap_keeps_the_next_first_fragment(void) {
    _z_transport_unicast_t ztu;
    _z_transport_peer_unicast_t peer;
    init_transport(&ztu, &peer, false, 10);

    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 10, true);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 11, false);
    // SN 12 is lost; SN 13 starts the next message.
    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 13, true);
    ASSERT_EQ_U32(_z_wbuf_len(&peer.common._dbuf_best_effort), PAYLOAD_LEN);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 14, false);
    ASSERT_EQ_U32(_z_wbuf_len(&peer.common._dbuf_best_effort), 2 * PAYLOAD_LEN);

    // A gap followed by a fragment that does not start a message drops everything until the next start.
    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 20, false);
    ASSERT_EQ_U32(_z_wbuf_len(&peer.common._dbuf_best_effort), 0);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 21, false);
    ASSERT_EQ_U32(_z_wbuf_len(&peer.common._dbuf_best_effort), 0);
    feed_fragment(&ztu, &peer, Z_RELIABILITY_BEST_EFFORT, PRIO_DATA, 22, true);
    ASSERT_EQ_U32(_z_wbuf_len(&peer.common._dbuf_best_effort), PAYLOAD_LEN);

    _z_transport_peer_unicast_clear(&peer);
}

int main(void) {
    test_init_qos_roundtrip();
    test_frame_and_fragment_qos_extension();
    test_qos_priorities_keep_their_own_channel();
    test_gap_keeps_the_next_first_fragment();
    return 0;
}

#else
int main(void) { return 0; }
#endif
