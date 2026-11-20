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

// Network-less tests of undeclaring a subscriber or a queryable from one of the session's callbacks.
//
// An undeclare waits until the callbacks of the entity are dropped, so that the caller may free what they use. The
// session's executor thread runs every callback, so an undeclare made inside a callback is made on that thread: there
// it must not wait, or the session waits for itself and never handles another message. Its entity is not called
// again, not even by the dispatch in progress (two entities on one key, the first callback undeclares the second).
// An undeclare from any other thread still waits for a callback that is running.
//
// The messages are fed through a task of the session's runtime, as the read task does.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils/assert_helpers.h"
#include "zenoh-pico.h"
#include "zenoh-pico/collections/atomic.h"
#include "zenoh-pico/net/liveliness.h"
#include "zenoh-pico/net/primitives.h"
#include "zenoh-pico/net/query.h"
#include "zenoh-pico/net/subscribe.h"
#include "zenoh-pico/protocol/definitions/declarations.h"
#include "zenoh-pico/protocol/definitions/network.h"
#include "zenoh-pico/runtime/runtime.h"
#include "zenoh-pico/session/keyexpr.h"
#include "zenoh-pico/session/loopback.h"
#include "zenoh-pico/session/session.h"
#include "zenoh-pico/session/utils.h"
#include "zenoh-pico/system/platform.h"
#include "zenoh-pico/transport/transport.h"

#if Z_FEATURE_MULTI_THREAD == 1 && Z_FEATURE_SUBSCRIPTION == 1 && Z_FEATURE_QUERYABLE == 1 && \
    Z_FEATURE_LIVELINESS == 1 && defined(Z_TEST_HOOKS)

#define KEY "zenoh-pico/tests/undeclare-in-callback"
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

/*------------------ Messages, handled on the session's own thread ------------------*/
typedef enum { FEED_PUT, FEED_QUERY, FEED_TOKEN } feed_kind_t;

typedef struct {
    feed_kind_t kind;
    z_result_t result;
    _z_atomic_bool_t done;
} feed_t;

// A wire expression as a router sends it for an undeclared key: no id, the whole key in the suffix.
static _z_wireexpr_t wire_key(void) {
    _z_wireexpr_t key = _z_wireexpr_null();
    key._suffix = _z_string_alias_str(KEY);
    return key;
}

static _z_fut_fn_result_t feed_task(void *arg, _z_executor_t *executor) {
    _ZP_UNUSED(executor);
    feed_t *feed = (feed_t *)arg;
    _z_wireexpr_t key = wire_key();
    _z_network_message_t msg = {0};
    switch (feed->kind) {
        case FEED_PUT: {
            _z_n_msg_make_push_put(&msg, &key, NULL, NULL, _z_n_qos_make(false, false, Z_PRIORITY_DEFAULT), NULL, NULL,
                                   Z_RELIABILITY_RELIABLE, NULL);
            break;
        }
        case FEED_QUERY: {
            _z_slice_t parameters = _z_slice_null();
            _z_n_msg_make_query(&msg, &key, &parameters, 1, Z_RELIABILITY_RELIABLE, Z_CONSOLIDATION_MODE_NONE, NULL,
                                NULL, 0, NULL, _z_n_qos_make(false, false, Z_PRIORITY_DEFAULT), NULL, false);
            break;
        }
        case FEED_TOKEN:
        default: {
            _z_n_msg_make_declare(&msg, _z_make_decl_token(&key, 1), _z_optional_id_make_none());
            break;
        }
    }
    // The handler the transport calls for each decoded network message
    feed->result = _z_handle_network_message(&g_fake_transport, &msg, &g_peer);
    _z_n_msg_clear(&msg);
    _z_atomic_bool_store(&feed->done, true, _z_memory_order_seq_cst);
    return _z_fut_fn_result_ready();
}

static void feed_start(feed_t *feed, feed_kind_t kind) {
    feed->kind = kind;
    feed->result = _Z_ERR_GENERIC;
    _z_atomic_bool_init(&feed->done, false);
    _z_fut_t fut = _z_fut_new(feed, feed_task, NULL);
    ASSERT_TRUE(!_z_fut_handle_is_null(_z_runtime_spawn(&g_session._runtime, &fut)));
}

// Feeds one message and waits until the session handled it: with an undeclare that waits for itself, it never does.
static void feed(feed_kind_t kind) {
    feed_t f;
    feed_start(&f, kind);
    ASSERT_TRUE(wait_for(&f.done));
    ASSERT_OK(f.result);
}

/*------------------ Entities ------------------*/
typedef struct entity_t {
    bool is_queryable;
    _z_subscriber_t sub;
    _z_queryable_t qle;
    struct entity_t *undeclare_in_callback;  // the entity this one's callback undeclares, if it is still declared
    _z_atomic_bool_t declared;
    _z_atomic_size_t calls;
    _z_atomic_size_t drops;
    z_result_t undeclare_result;
    // A callback that waits until the test lets it go
    bool blocks;
    _z_atomic_bool_t entered;
    _z_atomic_bool_t release;
} entity_t;

static size_t calls(entity_t *e) { return _z_atomic_size_load(&e->calls, _z_memory_order_seq_cst); }
static size_t drops(entity_t *e) { return _z_atomic_size_load(&e->drops, _z_memory_order_seq_cst); }

// What an application does: undeclare, then give up everything the callback uses.
static void entity_undeclare(entity_t *e) {
    bool expected = true;
    if (!_z_atomic_bool_compare_exchange_strong(&e->declared, &expected, false, _z_memory_order_seq_cst,
                                                _z_memory_order_seq_cst)) {
        return;
    }
    if (e->is_queryable) {
        e->undeclare_result = _z_undeclare_queryable(&e->qle);
        _z_queryable_clear(&e->qle);
    } else {
        e->undeclare_result = _z_undeclare_subscriber(&e->sub);
        _z_subscriber_clear(&e->sub);
    }
}

static void entity_called(entity_t *e) {
    _z_atomic_size_fetch_add(&e->calls, 1, _z_memory_order_seq_cst);
    if (e->blocks) {
        _z_atomic_bool_store(&e->entered, true, _z_memory_order_seq_cst);
        ASSERT_TRUE(wait_for(&e->release));
    }
    if (e->undeclare_in_callback != NULL) {
        entity_undeclare(e->undeclare_in_callback);
    }
}

static void sample_callback(_z_sample_t *sample, void *arg) {
    _ZP_UNUSED(sample);
    entity_called((entity_t *)arg);
}

static void query_callback(_z_query_rc_t *query, void *arg) {
    _ZP_UNUSED(query);
    entity_called((entity_t *)arg);
}

static void entity_dropped(void *arg) {
    entity_t *e = (entity_t *)arg;
    _z_atomic_size_fetch_add(&e->drops, 1, _z_memory_order_seq_cst);
}

static void entity_init(entity_t *e, bool is_queryable) {
    memset(e, 0, sizeof(*e));
    e->is_queryable = is_queryable;
    e->undeclare_result = _Z_ERR_GENERIC;
    _z_atomic_bool_init(&e->declared, false);
    _z_atomic_size_init(&e->calls, 0);
    _z_atomic_size_init(&e->drops, 0);
    _z_atomic_bool_init(&e->entered, false);
    _z_atomic_bool_init(&e->release, false);
}

static void declare_subscriber(entity_t *e) {
    entity_init(e, false);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    ASSERT_OK(
        _z_declare_subscriber(&e->sub, &g_session_rc, &ke, sample_callback, entity_dropped, e, z_locality_default()));
    _z_atomic_bool_store(&e->declared, true, _z_memory_order_seq_cst);
}

static void declare_liveliness_subscriber(entity_t *e) {
    entity_init(e, false);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    ASSERT_OK(_z_declare_liveliness_subscriber(&e->sub, &g_session_rc, &ke, sample_callback, entity_dropped, false, e));
    _z_atomic_bool_store(&e->declared, true, _z_memory_order_seq_cst);
}

static void declare_queryable(entity_t *e) {
    entity_init(e, true);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    ASSERT_OK(_z_declare_queryable(&e->qle, &g_session_rc, &ke, true, query_callback, entity_dropped, e,
                                   z_locality_default()));
    _z_atomic_bool_store(&e->declared, true, _z_memory_order_seq_cst);
}

/*------------------ Tests ------------------*/
// The reported case: a callback undeclares its own entity. The message must still be handled to the end, and the
// session must go on to the next one.
static void check_entity_undeclared_in_its_own_callback(void (*declare)(entity_t *), feed_kind_t kind) {
    setup_session();
    entity_t e;
    declare(&e);
    e.undeclare_in_callback = &e;

    feed(kind);
    ASSERT_TRUE(calls(&e) == 1);
    ASSERT_OK(e.undeclare_result);
    // The dispatch that called it was the last one to hold it
    ASSERT_TRUE(drops(&e) == 1);

    feed(kind);
    ASSERT_TRUE(calls(&e) == 1);
    ASSERT_TRUE(drops(&e) == 1);

    cleanup_session();
}

static void test_subscriber_undeclared_in_its_own_callback(void) {
    check_entity_undeclared_in_its_own_callback(declare_subscriber, FEED_PUT);
}

static void test_liveliness_subscriber_undeclared_in_its_own_callback(void) {
    check_entity_undeclared_in_its_own_callback(declare_liveliness_subscriber, FEED_TOKEN);
}

static void test_queryable_undeclared_in_its_own_callback(void) {
    check_entity_undeclared_in_its_own_callback(declare_queryable, FEED_QUERY);
}

// Two entities match one message, and the callback that runs first undeclares the other one. The undeclare returned,
// so the application may have freed what the other callback uses: that callback must not run in this dispatch.
static void check_entity_undeclared_by_a_sibling_callback(void (*declare)(entity_t *), feed_kind_t kind) {
    setup_session();
    entity_t a;
    entity_t b;
    declare(&a);
    declare(&b);
    a.undeclare_in_callback = &b;
    b.undeclare_in_callback = &a;

    feed(kind);
    ASSERT_TRUE(calls(&a) + calls(&b) == 1);
    entity_t *first = calls(&a) == 1 ? &a : &b;
    entity_t *second = calls(&a) == 1 ? &b : &a;
    ASSERT_OK(second->undeclare_result);
    ASSERT_TRUE(drops(second) == 1);
    ASSERT_TRUE(drops(first) == 0);

    feed(kind);
    ASSERT_TRUE(calls(first) == 2);
    ASSERT_TRUE(calls(second) == 0);

    // From this thread, with no callback running
    entity_undeclare(first);
    ASSERT_OK(first->undeclare_result);
    ASSERT_TRUE(drops(first) == 1);

    cleanup_session();
}

static void test_subscriber_undeclared_by_a_sibling_callback(void) {
    check_entity_undeclared_by_a_sibling_callback(declare_subscriber, FEED_PUT);
}

static void test_queryable_undeclared_by_a_sibling_callback(void) {
    check_entity_undeclared_by_a_sibling_callback(declare_queryable, FEED_QUERY);
}

// An undeclare from another thread keeps the guarantee of 1.10: it returns only once the running callback is over.
static void *undeclare_main(void *arg) {
    entity_undeclare((entity_t *)arg);
    return NULL;
}

static void check_undeclare_from_another_thread_waits(void (*declare)(entity_t *), feed_kind_t kind) {
    setup_session();
    entity_t e;
    declare(&e);
    e.blocks = true;

    feed_t f;
    feed_start(&f, kind);
    ASSERT_TRUE(wait_for(&e.entered));

    _z_task_t task;
    ASSERT_OK(_z_task_init(&task, NULL, undeclare_main, &e));
    // The entity is unregistered at once, but the call stays in the wait while the callback runs
    for (unsigned i = 0; i < WAIT_LIMIT_MS && _z_atomic_bool_load(&e.declared, _z_memory_order_seq_cst); i++) {
        z_sleep_ms(1);
    }
    ASSERT_TRUE(!_z_atomic_bool_load(&e.declared, _z_memory_order_seq_cst));
    z_sleep_ms(200);
    ASSERT_TRUE(drops(&e) == 0);
    ASSERT_TRUE(e.undeclare_result == _Z_ERR_GENERIC);

    _z_atomic_bool_store(&e.release, true, _z_memory_order_seq_cst);
    ASSERT_OK(_z_task_join(&task));
    ASSERT_OK(e.undeclare_result);
    ASSERT_TRUE(drops(&e) == 1);
    ASSERT_TRUE(wait_for(&f.done));
    ASSERT_OK(f.result);
    ASSERT_TRUE(calls(&e) == 1);

    cleanup_session();
}

static void test_subscriber_undeclare_from_another_thread_waits(void) {
    check_undeclare_from_another_thread_waits(declare_subscriber, FEED_PUT);
}

static void test_queryable_undeclare_from_another_thread_waits(void) {
    check_undeclare_from_another_thread_waits(declare_queryable, FEED_QUERY);
}

int main(void) {
    test_subscriber_undeclared_in_its_own_callback();
    test_liveliness_subscriber_undeclared_in_its_own_callback();
    test_queryable_undeclared_in_its_own_callback();
    test_subscriber_undeclared_by_a_sibling_callback();
    test_queryable_undeclared_by_a_sibling_callback();
    test_subscriber_undeclare_from_another_thread_waits();
    test_queryable_undeclare_from_another_thread_waits();
    return 0;
}

#else

int main(void) { return 0; }

#endif
