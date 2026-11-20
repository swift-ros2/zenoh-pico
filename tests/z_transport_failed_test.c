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

// Network-less tests of a unicast transport that fails while application threads send on it.
//
// When the lease expires or the link breaks, the failure path sends a CLOSE and then clears the transport: it destroys
// the tx mutex, frees the buffers and the link, and only then sets the type to "none". A send that is blocked in the
// link (a peer that no longer reads) keeps the tx mutex, so the CLOSE waits for it, and every sender that arrives in
// the meantime queues on a mutex that is about to be destroyed. The transport must therefore be marked as failed
// before the CLOSE is sent, senders must be turned away once it is marked, and the clear must wait for the senders
// that are already inside.
//
// The session uses a fake link whose write blocks until the test opens a gate.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils/assert_helpers.h"
#include "zenoh-pico.h"
#include "zenoh-pico/collections/atomic.h"
#include "zenoh-pico/link/link.h"
#include "zenoh-pico/net/session.h"
#include "zenoh-pico/protocol/definitions/core.h"
#include "zenoh-pico/protocol/definitions/network.h"
#include "zenoh-pico/protocol/definitions/transport.h"
#include "zenoh-pico/runtime/executor.h"
#include "zenoh-pico/session/session.h"
#include "zenoh-pico/session/utils.h"
#include "zenoh-pico/system/platform.h"
#include "zenoh-pico/transport/common/tx.h"
#include "zenoh-pico/transport/transport.h"
#include "zenoh-pico/transport/unicast/lease.h"
#include "zenoh-pico/transport/unicast/transport.h"
#include "zenoh-pico/transport/utils.h"

#if Z_FEATURE_UNICAST_TRANSPORT == 1 && Z_FEATURE_MULTI_THREAD == 1 && Z_FEATURE_PUBLICATION == 1

#define KEY "zenoh-pico/tests/transport/failed"
#define WAIT_LIMIT_MS 10000

/*------------------ Fake link: a write blocks while the gate is closed ------------------*/
static _z_mutex_t g_gate_mutex;
static _z_condvar_t g_gate_cond;
static bool g_gate_open = false;
static size_t g_writers_inside = 0;
static size_t g_write_count = 0;
static uint8_t g_last_write[8];
static size_t g_last_write_len = 0;

static size_t fake_write(const _z_link_t *self, const uint8_t *ptr, size_t len, _z_sys_net_socket_t *socket) {
    _ZP_UNUSED(self);
    _ZP_UNUSED(socket);
    _z_mutex_lock(&g_gate_mutex);
    g_writers_inside++;
    while (!g_gate_open) {
        _z_condvar_wait(&g_gate_cond, &g_gate_mutex);
    }
    g_writers_inside--;
    g_write_count++;
    g_last_write_len = len < sizeof(g_last_write) ? len : sizeof(g_last_write);
    memcpy(g_last_write, ptr, g_last_write_len);
    _z_mutex_unlock(&g_gate_mutex);
    return len;
}

static void set_gate(bool open) {
    _z_mutex_lock(&g_gate_mutex);
    g_gate_open = open;
    _z_condvar_signal_all(&g_gate_cond);
    _z_mutex_unlock(&g_gate_mutex);
}

static size_t writers_inside(void) {
    _z_mutex_lock(&g_gate_mutex);
    size_t n = g_writers_inside;
    _z_mutex_unlock(&g_gate_mutex);
    return n;
}

/*------------------ Session with a unicast transport over the fake link ------------------*/
static _z_session_t g_session;
static _z_session_rc_t g_session_rc = {0};

static _z_transport_common_t *transport_common(void) { return &g_session._tp._transport._unicast._common; }

static size_t tx_users(void) { return _z_atomic_size_load(&transport_common()->_tx_users, _z_memory_order_seq_cst); }

static void setup_session(void) {
    _z_id_t zid;
    _z_session_generate_zid(&zid, Z_ZID_LENGTH);
    ASSERT_OK(_z_session_init(&g_session, &zid));
    ASSERT_TRUE(g_session._mode == Z_WHATAMI_CLIENT);
    if (_Z_RC_IS_NULL(&g_session_rc)) {
        g_session_rc = _z_session_rc_new(&g_session);
        ASSERT_TRUE(!_Z_RC_IS_NULL(&g_session_rc));
    } else {
        g_session_rc._val = &g_session;
    }

    // The transport owns the link and frees it when it is cleared.
    _z_link_t *link = (_z_link_t *)z_malloc(sizeof(_z_link_t));
    ASSERT_TRUE(link != NULL);
    memset(link, 0, sizeof(_z_link_t));
    link->_cap._flow = Z_LINK_CAP_FLOW_STREAM;
    link->_cap._is_reliable = true;
    link->_mtu = 65535;
    link->_write_f = fake_write;

    _z_transport_unicast_establish_param_t param;
    memset(&param, 0, sizeof(param));
    param._batch_size = 2048;
    param._seq_num_res = Z_SN_RESOLUTION;
    param._lease = Z_TRANSPORT_LEASE;
    param._peer_lease = Z_TRANSPORT_LEASE;
    ASSERT_OK(_z_unicast_transport_create(&g_session._tp, link, &param));
    transport_common()->_session = _z_session_rc_clone_as_weak(&g_session_rc);
    transport_common()->_state = _Z_TRANSPORT_STATE_OPEN;
    // The router. It does not own a socket, so clearing the transport closes nothing.
    _z_sys_net_socket_t no_socket;
    memset(&no_socket, 0, sizeof(no_socket));
    _z_transport_peer_unicast_t *router = NULL;
    ASSERT_OK(_z_transport_peer_unicast_add(&g_session._tp._transport._unicast, &param, no_socket, false, &router));
    ASSERT_EQ_U32(_z_transport_get_peers_count(&g_session._tp), 1);

    g_writers_inside = 0;
    g_write_count = 0;
    g_last_write_len = 0;
    set_gate(false);
}

static void cleanup_session(void) {
    // Every test ends with the transport cleared.
    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_NONE);
    _z_session_clear(&g_session);
}

/*------------------ Threads ------------------*/
static z_result_t send_put(z_congestion_control_t cong_ctrl) {
    // As a client sends an undeclared key: no id, the whole key in the suffix.
    _z_wireexpr_t key = _z_wireexpr_null();
    key._suffix = _z_string_alias_str(KEY);
    uint8_t byte = 42;
    _z_bytes_t payload = _z_bytes_null();
    ASSERT_OK(_z_bytes_from_buf(&payload, &byte, 1));
    _z_encoding_t encoding = _z_encoding_null();
    _z_timestamp_t ts = _z_timestamp_null();
    _z_source_info_t source_info = _z_source_info_null();
    _z_network_message_t push;
    _z_n_msg_make_push_put(&push, &key, &payload, &encoding, _z_n_qos_make(false, false, Z_PRIORITY_DEFAULT), &ts, NULL,
                           Z_RELIABILITY_RELIABLE, &source_info);
    z_result_t ret = _z_send_n_msg(&g_session, &push, Z_RELIABILITY_RELIABLE, cong_ctrl, NULL);
    _z_bytes_drop(&payload);
    return ret;
}

typedef struct {
    _z_task_t task;
    volatile bool done;
    z_result_t result;
} worker_t;

static void *sender_main(void *arg) {
    worker_t *w = (worker_t *)arg;
    w->result = send_put(Z_CONGESTION_CONTROL_BLOCK);
    w->done = true;
    return NULL;
}

static _z_executor_t g_executor;

// What the lease task and the read task do when the transport fails.
static void *failure_main(void *arg) {
    worker_t *w = (worker_t *)arg;
    (void)_zp_unicast_failed_result(&g_session._tp._transport._unicast, &g_executor);
    w->result = _Z_RES_OK;
    w->done = true;
    return NULL;
}

// What closing a session does to its transport.
static void *clear_main(void *arg) {
    worker_t *w = (worker_t *)arg;
    _z_session_transport_mutex_lock(&g_session);
    _z_transport_clear(&g_session._tp);
    _z_session_transport_mutex_unlock(&g_session);
    w->result = _Z_RES_OK;
    w->done = true;
    return NULL;
}

static void start(worker_t *w, void *(*fun)(void *)) {
    w->done = false;
    w->result = _Z_RES_OK;
    ASSERT_OK(_z_task_init(&w->task, NULL, fun, w));
}

#define WAIT_UNTIL(cond)                                                  \
    do {                                                                  \
        z_clock_t wait_start = z_clock_now();                             \
        while (!(cond)) {                                                 \
            ASSERT_TRUE(z_clock_elapsed_ms(&wait_start) < WAIT_LIMIT_MS); \
            z_sleep_ms(1);                                                \
        }                                                                 \
    } while (0)

/*------------------ Tests ------------------*/
// A sender is blocked in the link and a second one waits for the tx mutex when the transport fails.
static void test_failure_turns_senders_away_before_the_transport_is_cleared(void) {
    setup_session();
    _z_executor_init(&g_executor);

    worker_t in_link;
    start(&in_link, sender_main);
    WAIT_UNTIL(writers_inside() == 1);
    worker_t on_mutex;
    start(&on_mutex, sender_main);
    WAIT_UNTIL(tx_users() == 2);

    ASSERT_FALSE(_z_transport_is_lost(&g_session._tp));
    worker_t failure;
    start(&failure, failure_main);
    // The failure is visible at once, although the CLOSE cannot be sent yet and the transport is still there.
    WAIT_UNTIL(_z_transport_is_lost(&g_session._tp));
    z_sleep_ms(50);
    ASSERT_FALSE(failure.done);
    ASSERT_FALSE(in_link.done);
    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_UNICAST_TYPE);
    // The second sender waits for the tx mutex. (It counts as inside from its first step into the gate: if the
    // failure was marked before it read the mark, it was turned away there instead.)
    size_t inside = tx_users();
    ASSERT_TRUE((inside == 2 && !on_mutex.done) || (inside == 1 && on_mutex.done));

    // A new sender is turned away without waiting for the mutex.
    ASSERT_EQ_I32(send_put(Z_CONGESTION_CONTROL_BLOCK), _Z_ERR_TRANSPORT_NOT_AVAILABLE);
    ASSERT_EQ_I32(send_put(Z_CONGESTION_CONTROL_DROP), _Z_ERR_TRANSPORT_NOT_AVAILABLE);
#if Z_FEATURE_BATCHING == 1
    ASSERT_EQ_I32(_z_send_n_batch(&g_session, Z_CONGESTION_CONTROL_BLOCK), _Z_ERR_TRANSPORT_NOT_AVAILABLE);
#endif
    _z_transport_message_t ka = _z_t_msg_make_keep_alive();
    ASSERT_EQ_I32(_z_send_t_msg(&g_session._tp, &ka), _Z_ERR_TRANSPORT_NOT_AVAILABLE);
    // A query counts the peers to know how many finals to expect: none on a transport that is on its way out.
    ASSERT_EQ_U32(_z_transport_get_peers_count(&g_session._tp), 0);
    ASSERT_EQ_U32(tx_users(), inside);

    set_gate(true);
    ASSERT_OK(_z_task_join(&in_link.task));
    ASSERT_OK(_z_task_join(&on_mutex.task));
    ASSERT_OK(_z_task_join(&failure.task));
    // The send that was in the link completed; the one that waited for the mutex did not touch the transport.
    ASSERT_OK(in_link.result);
    ASSERT_EQ_I32(on_mutex.result, _Z_ERR_TRANSPORT_NOT_AVAILABLE);
    // The CLOSE still went out, as the last message on the link (a stream link: two length bytes, then the header).
    ASSERT_EQ_U32(g_write_count, 2);
    ASSERT_TRUE(g_last_write_len >= 3);
    ASSERT_EQ_U32(_Z_MID(g_last_write[2]), _Z_MID_T_CLOSE);

    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_NONE);
    ASSERT_TRUE(_z_transport_is_lost(&g_session._tp));
    ASSERT_EQ_I32(send_put(Z_CONGESTION_CONTROL_BLOCK), _Z_ERR_TRANSPORT_NOT_AVAILABLE);

    // With auto-reconnection the failure path left a reopen task: drop it.
    _z_executor_destroy(&g_executor);
    cleanup_session();
}

// Clearing a transport (a session that closes) waits for the sender that is inside it.
static void test_clear_waits_for_a_sender_in_flight(void) {
    setup_session();

    worker_t in_link;
    start(&in_link, sender_main);
    WAIT_UNTIL(writers_inside() == 1);

    worker_t clear;
    start(&clear, clear_main);
    WAIT_UNTIL(_z_transport_is_lost(&g_session._tp));
    z_sleep_ms(100);
    ASSERT_FALSE(clear.done);
    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_UNICAST_TYPE);
    ASSERT_EQ_I32(send_put(Z_CONGESTION_CONTROL_BLOCK), _Z_ERR_TRANSPORT_NOT_AVAILABLE);

    set_gate(true);
    ASSERT_OK(_z_task_join(&in_link.task));
    ASSERT_OK(_z_task_join(&clear.task));
    ASSERT_OK(in_link.result);
    ASSERT_EQ_U32(g_write_count, 1);
    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_NONE);

    cleanup_session();
}

// A message dropped by congestion control leaves the send section, so that it does not hold up a later clear.
static void test_dropped_message_does_not_hold_the_transport(void) {
    setup_session();

    worker_t in_link;
    start(&in_link, sender_main);
    WAIT_UNTIL(writers_inside() == 1);
    ASSERT_EQ_U32(tx_users(), 1);

    ASSERT_TRUE(send_put(Z_CONGESTION_CONTROL_DROP) != _Z_RES_OK);
    ASSERT_EQ_U32(tx_users(), 1);

    set_gate(true);
    ASSERT_OK(_z_task_join(&in_link.task));
    ASSERT_OK(in_link.result);
    ASSERT_EQ_U32(tx_users(), 0);

    // An open gate: these go straight through.
    ASSERT_OK(send_put(Z_CONGESTION_CONTROL_BLOCK));
    ASSERT_OK(send_put(Z_CONGESTION_CONTROL_DROP));
    ASSERT_EQ_U32(tx_users(), 0);
    ASSERT_EQ_U32(g_write_count, 3);

    _z_session_transport_mutex_lock(&g_session);
    _z_transport_clear(&g_session._tp);
    _z_session_transport_mutex_unlock(&g_session);
    cleanup_session();
}

// Counting the peers takes the peer mutex, which a clear destroys: the clear waits for a count in progress, as it
// waits for a sender.
static void *count_main(void *arg) {
    worker_t *w = (worker_t *)arg;
    w->result = (z_result_t)_z_transport_get_peers_count(&g_session._tp);
    w->done = true;
    return NULL;
}

static void test_clear_waits_for_a_peer_count_in_progress(void) {
    setup_session();

    // Hold the count on the peer mutex
    _z_transport_peer_mutex_lock(transport_common());
    worker_t count;
    start(&count, count_main);
    WAIT_UNTIL(tx_users() == 1);

    worker_t clear;
    start(&clear, clear_main);
    WAIT_UNTIL(_z_transport_is_lost(&g_session._tp));
    z_sleep_ms(100);
    ASSERT_FALSE(clear.done);
    ASSERT_FALSE(count.done);
    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_UNICAST_TYPE);
    // A count that starts now does not touch the transport
    ASSERT_EQ_U32(_z_transport_get_peers_count(&g_session._tp), 0);

    _z_transport_peer_mutex_unlock(transport_common());
    ASSERT_OK(_z_task_join(&count.task));
    ASSERT_OK(_z_task_join(&clear.task));
    // The count that was inside saw the peer
    ASSERT_EQ_I32(count.result, 1);
    ASSERT_TRUE(g_session._tp._type == _Z_TRANSPORT_NONE);
    ASSERT_EQ_U32(_z_transport_get_peers_count(&g_session._tp), 0);

    cleanup_session();
}

int main(void) {
    ASSERT_OK(_z_mutex_init(&g_gate_mutex));
    ASSERT_OK(_z_condvar_init(&g_gate_cond));
    test_failure_turns_senders_away_before_the_transport_is_cleared();
    test_clear_waits_for_a_sender_in_flight();
    test_clear_waits_for_a_peer_count_in_progress();
    test_dropped_message_does_not_hold_the_transport();
    _z_condvar_drop(&g_gate_cond);
    _z_mutex_drop(&g_gate_mutex);
    return 0;
}

#else
int main(void) { return 0; }
#endif
