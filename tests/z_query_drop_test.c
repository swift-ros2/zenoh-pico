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

// Network-less tests of what the drop function of a query may do.
//
// The drop function of a get tells the application that the query is over. It is called when the last final arrives,
// when the query times out, when it is cancelled and when its querier is undeclared. It was called with the session
// mutex held, which is not recursive: a drop function that declared or undeclared an entity of the same session
// waited for a mutex that its own thread held. On the first two paths that thread is the session's executor thread,
// so the session never handled another message.
//
// The drop function of these tests undeclares one subscriber and declares another one.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils/assert_helpers.h"
#include "zenoh-pico.h"
#include "zenoh-pico/collections/atomic.h"
#include "zenoh-pico/net/primitives.h"
#include "zenoh-pico/net/subscribe.h"
#include "zenoh-pico/protocol/definitions/network.h"
#include "zenoh-pico/runtime/runtime.h"
#include "zenoh-pico/session/keyexpr.h"
#include "zenoh-pico/session/loopback.h"
#include "zenoh-pico/session/query.h"
#include "zenoh-pico/session/session.h"
#include "zenoh-pico/session/utils.h"
#include "zenoh-pico/system/platform.h"
#include "zenoh-pico/transport/transport.h"

#if Z_FEATURE_MULTI_THREAD == 1 && Z_FEATURE_SUBSCRIPTION == 1 && Z_FEATURE_QUERY == 1 && defined(Z_TEST_HOOKS)

#define KEY "zenoh-pico/tests/query-drop"
#define QUERIER_ID 7
#define WAIT_LIMIT_MS 10000

static _z_session_t g_session;
static _z_session_rc_t g_session_rc = {0};
static _z_transport_common_t g_fake_transport = {0};
static bool g_transport_ready = false;
static _z_link_t g_dummy_link = {0};
static _z_transport_peer_common_t g_peer = {0};

static _z_transport_common_t *transport_override(_z_session_t *zn) {
    if (g_transport_ready && zn == &g_session) {
        return &g_fake_transport;
    }
    return NULL;
}

// There is no network: every message the session sends is swallowed.
static z_result_t send_override(_z_session_t *zn, const _z_network_message_t *n_msg, z_reliability_t reliability,
                                z_congestion_control_t cong_ctrl, void *peer, bool *handled) {
    _ZP_UNUSED(zn);
    _ZP_UNUSED(n_msg);
    _ZP_UNUSED(reliability);
    _ZP_UNUSED(cong_ctrl);
    _ZP_UNUSED(peer);
    if (handled != NULL) {
        *handled = true;
    }
    return _Z_RES_OK;
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

    g_dummy_link = (_z_link_t){0};
    g_fake_transport = (_z_transport_common_t){0};
    g_fake_transport._session = _z_session_rc_clone_as_weak(&g_session_rc);
    g_fake_transport._link = &g_dummy_link;
    g_peer = (_z_transport_peer_common_t){0};
    g_transport_ready = true;
    _z_session_set_transport_common_override(transport_override);
    _z_transport_set_send_n_msg_override(send_override);
    g_session._tp._type = _Z_TRANSPORT_UNICAST_TYPE;
    ASSERT_OK(_z_runtime_start(&g_session._runtime, NULL));
}

static void cleanup_session(void) {
    g_session._tp._type = _Z_TRANSPORT_NONE;
    _z_session_clear(&g_session);
    g_transport_ready = false;
    _z_session_set_transport_common_override(NULL);
    _z_transport_set_send_n_msg_override(NULL);
}

static bool wait_for(_z_atomic_bool_t *flag) {
    for (unsigned i = 0; i < WAIT_LIMIT_MS; i++) {
        if (_z_atomic_bool_load(flag, _z_memory_order_seq_cst)) {
            return true;
        }
        z_sleep_ms(1);
    }
    return _z_atomic_bool_load(flag, _z_memory_order_seq_cst);
}

/*------------------ A query whose drop function uses the session ------------------*/
typedef struct {
    _z_subscriber_t undeclared;  // declared by the test, undeclared by the drop function
    _z_subscriber_t declared;    // declared by the drop function, undeclared by the test
    z_result_t undeclare_result;
    z_result_t declare_result;
    _z_atomic_size_t drops;
    _z_zint_t qid;
} query_t;

static void sample_callback(_z_sample_t *sample, void *arg) {
    _ZP_UNUSED(sample);
    _ZP_UNUSED(arg);
}

static void reply_callback(_z_reply_t *reply, void *arg) {
    _ZP_UNUSED(reply);
    _ZP_UNUSED(arg);
}

static void query_dropped(void *arg) {
    query_t *q = (query_t *)arg;
    q->undeclare_result = _z_undeclare_subscriber(&q->undeclared);
    _z_subscriber_clear(&q->undeclared);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    q->declare_result =
        _z_declare_subscriber(&q->declared, &g_session_rc, &ke, sample_callback, NULL, NULL, z_locality_default());
    _z_atomic_size_fetch_add(&q->drops, 1, _z_memory_order_seq_cst);
}

// Registers a pending query as _z_query does for a query that waits for one final.
static void query_start(query_t *q, uint64_t timeout_ms) {
    memset(q, 0, sizeof(*q));
    q->undeclare_result = _Z_ERR_GENERIC;
    q->declare_result = _Z_ERR_GENERIC;
    _z_atomic_size_init(&q->drops, 0);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    ASSERT_OK(
        _z_declare_subscriber(&q->undeclared, &g_session_rc, &ke, sample_callback, NULL, NULL, z_locality_default()));

    _z_keyexpr_t query_key;
    ASSERT_OK(_z_keyexpr_copy(&query_key, &ke._inner));
    ASSERT_OK(_z_session_mutex_lock_if_open(&g_session));
    _z_pending_query_t *pq = _z_unsafe_register_pending_query(&g_session);
    ASSERT_TRUE(pq != NULL);
    q->qid = pq->_id;
    memset(pq, 0, sizeof(*pq));
    pq->_id = q->qid;
    pq->_querier_id = _z_optional_id_make_some(QUERIER_ID);
    pq->_key = query_key;
    pq->_target = Z_QUERY_TARGET_BEST_MATCHING;
    pq->_consolidation = Z_CONSOLIDATION_MODE_NONE;
    pq->_callback = reply_callback;
    pq->_dropper = query_dropped;
    pq->_arg = q;
    pq->_allowed_destination = z_locality_default();
    pq->_timeout = timeout_ms;
    pq->_start_time = z_clock_now();
    pq->_remaining_finals = 1;
    _z_session_mutex_unlock(&g_session);
}

static size_t pending_queries(void) {
    _z_session_mutex_lock(&g_session);
    size_t len = _z_pending_query_slist_len(g_session._pending_queries);
    _z_session_mutex_unlock(&g_session);
    return len;
}

// The drop function ran once, and both of its calls on the session went through.
static void query_check_dropped(query_t *q) {
    ASSERT_TRUE(_z_atomic_size_load(&q->drops, _z_memory_order_seq_cst) == 1);
    ASSERT_OK(q->undeclare_result);
    ASSERT_OK(q->declare_result);
    ASSERT_TRUE(pending_queries() == 0);
    ASSERT_OK(_z_undeclare_subscriber(&q->declared));
    _z_subscriber_clear(&q->declared);
}

/*------------------ The four ways a query ends ------------------*/
typedef enum { END_FINAL, END_TIMEOUT, END_UNREGISTER, END_QUERIER } end_kind_t;

typedef struct {
    end_kind_t kind;
    _z_zint_t qid;
    z_result_t result;
    _z_atomic_bool_t done;
} end_t;

static void end_query(end_t *end) {
    end->result = _Z_RES_OK;
    switch (end->kind) {
        case END_FINAL: {
            // The handler the transport calls for each decoded network message
            _z_network_message_t msg = {0};
            _z_n_msg_make_response_final(&msg, end->qid);
            end->result = _z_handle_network_message(&g_fake_transport, &msg, &g_peer);
            _z_n_msg_clear(&msg);
            break;
        }
        case END_TIMEOUT:
            // What the session's periodic task calls
            _z_pending_query_process_timeout(&g_session);
            break;
        case END_UNREGISTER:
            // A cancelled query, or one whose message could not be sent
            _z_unregister_pending_query(&g_session, end->qid);
            break;
        case END_QUERIER:
        default:
            // The querier of the query is undeclared
            _z_unregister_pending_queries_from_querier(&g_session, QUERIER_ID);
            break;
    }
    _z_atomic_bool_store(&end->done, true, _z_memory_order_seq_cst);
}

static _z_fut_fn_result_t end_on_session_thread(void *arg, _z_executor_t *executor) {
    _ZP_UNUSED(executor);
    end_query((end_t *)arg);
    return _z_fut_fn_result_ready();
}

static void *end_on_another_thread(void *arg) {
    end_query((end_t *)arg);
    return NULL;
}

// Ends the query on the thread that does it in a session: the session's own for a final and for a timeout, an
// application thread for the other two. With a drop function that waits for its own thread, the end never returns.
static void check_query_end(end_kind_t kind, uint64_t timeout_ms) {
    setup_session();
    query_t q;
    query_start(&q, timeout_ms);
    ASSERT_TRUE(pending_queries() == 1);
    if (kind == END_TIMEOUT) {
        z_sleep_ms((size_t)timeout_ms + 20);
    }

    end_t end;
    end.kind = kind;
    end.qid = q.qid;
    end.result = _Z_ERR_GENERIC;
    _z_atomic_bool_init(&end.done, false);
    if (kind == END_FINAL || kind == END_TIMEOUT) {
        _z_fut_t fut = _z_fut_new(&end, end_on_session_thread, NULL);
        ASSERT_TRUE(!_z_fut_handle_is_null(_z_runtime_spawn(&g_session._runtime, &fut)));
        ASSERT_TRUE(wait_for(&end.done));
    } else {
        _z_task_t task;
        ASSERT_OK(_z_task_init(&task, NULL, end_on_another_thread, &end));
        ASSERT_TRUE(wait_for(&end.done));
        ASSERT_OK(_z_task_join(&task));
    }
    ASSERT_OK(end.result);
    query_check_dropped(&q);

    cleanup_session();
}

static void test_drop_function_of_a_finished_query_uses_the_session(void) { check_query_end(END_FINAL, 60000); }

static void test_drop_function_of_a_timed_out_query_uses_the_session(void) { check_query_end(END_TIMEOUT, 10); }

static void test_drop_function_of_an_unregistered_query_uses_the_session(void) {
    check_query_end(END_UNREGISTER, 60000);
}

static void test_drop_function_of_a_query_of_an_undeclared_querier_uses_the_session(void) {
    check_query_end(END_QUERIER, 60000);
}

int main(void) {
    test_drop_function_of_a_finished_query_uses_the_session();
    test_drop_function_of_a_timed_out_query_uses_the_session();
    test_drop_function_of_an_unregistered_query_uses_the_session();
    test_drop_function_of_a_query_of_an_undeclared_querier_uses_the_session();
    return 0;
}

#else

int main(void) { return 0; }

#endif
