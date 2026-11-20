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

// Network-less tests of a message that names a key expression which this session has undeclared.
//
// A subscriber or a queryable declares its key as a resource and tells the router the id; from then on the router
// names the key by that id in what it sends to the session. The resource is undeclared together with the last entity
// that uses it, and whatever the router sent before it processed the undeclaration still carries the id. Such a
// message has no receiver left and must be dropped. Reporting it as an error makes the transport treat it as a
// protocol violation and close, so undeclaring an entity under traffic cost the whole session.
//
// An id that the peer never declared in its own mapping stays an error: the peer declares before it uses.
//
// The messages are fed through a task of the session's runtime, as the read task does.

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#include "utils/assert_helpers.h"
#include "zenoh-pico.h"
#include "zenoh-pico/collections/atomic.h"
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

#if Z_FEATURE_MULTI_THREAD == 1 && Z_FEATURE_SUBSCRIPTION == 1 && Z_FEATURE_QUERYABLE == 1 && Z_FEATURE_QUERY == 1 && \
    Z_FEATURE_LIVELINESS == 1 && Z_FEATURE_INTEREST == 1 && defined(Z_TEST_HOOKS)

#define KEY "zenoh-pico/tests/stale-keyexpr"
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

// There is no network: every message the session sends is swallowed. The finals are counted by request id.
#define FINALS_MAX 16
static _z_atomic_size_t g_finals[FINALS_MAX];

static z_result_t send_override(_z_session_t *zn, const _z_network_message_t *n_msg, z_reliability_t reliability,
                                z_congestion_control_t cong_ctrl, void *peer, bool *handled) {
    _ZP_UNUSED(zn);
    _ZP_UNUSED(reliability);
    _ZP_UNUSED(cong_ctrl);
    _ZP_UNUSED(peer);
    if (n_msg->_tag == _Z_N_RESPONSE_FINAL && n_msg->_body._response_final._request_id < FINALS_MAX) {
        _z_atomic_size_fetch_add(&g_finals[n_msg->_body._response_final._request_id], 1, _z_memory_order_seq_cst);
    }
    if (handled != NULL) {
        *handled = true;
    }
    return _Z_RES_OK;
}

static size_t finals(_z_zint_t rid) { return _z_atomic_size_load(&g_finals[rid], _z_memory_order_seq_cst); }

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
    for (size_t i = 0; i < FINALS_MAX; i++) {
        _z_atomic_size_init(&g_finals[i], 0);
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

/*------------------ Key expressions as a router sends them ------------------*/
// The whole key in the suffix, no id
static _z_wireexpr_t key_by_name(void) {
    _z_wireexpr_t key = _z_wireexpr_null();
    key._suffix = _z_string_alias_str(KEY);
    return key;
}

// An id that the receiving session declared
static _z_wireexpr_t key_by_own_id(uint16_t id) {
    _z_wireexpr_t key = _z_wireexpr_null();
    key._id = id;
    key._mapping = _Z_KEYEXPR_MAPPING_LOCAL;
    return key;
}

// An id of the sender's own mapping
static _z_wireexpr_t key_by_peer_id(uint16_t id) {
    _z_wireexpr_t key = _z_wireexpr_null();
    key._id = id;
    key._mapping = (uintptr_t)&g_peer;
    return key;
}

// The only resource the session has declared
static uint16_t declared_resource_id(void) {
    _z_session_mutex_lock(&g_session);
    ASSERT_TRUE(_z_resource_slist_len(g_session._local_resources) == 1);
    uint16_t id = _z_resource_slist_value(g_session._local_resources)->_id;
    _z_session_mutex_unlock(&g_session);
    ASSERT_TRUE(id != Z_RESOURCE_ID_NONE);
    return id;
}

static size_t declared_resources(void) {
    _z_session_mutex_lock(&g_session);
    size_t len = _z_resource_slist_len(g_session._local_resources);
    _z_session_mutex_unlock(&g_session);
    return len;
}

/*------------------ Messages, handled on the session's own thread ------------------*/
typedef enum {
    FEED_PUT,
    FEED_QUERY,
    FEED_REPLY,
    FEED_DECL_SUBSCRIBER,
    FEED_DECL_QUERYABLE,
    FEED_DECL_TOKEN
} feed_kind_t;

typedef struct {
    feed_kind_t kind;
    _z_wireexpr_t key;
    _z_zint_t rid;
    z_result_t result;
    _z_atomic_bool_t done;
} feed_t;

static _z_fut_fn_result_t feed_task(void *arg, _z_executor_t *executor) {
    _ZP_UNUSED(executor);
    feed_t *feed = (feed_t *)arg;
    _z_network_message_t msg = {0};
    _z_n_qos_t qos = _z_n_qos_make(false, false, Z_PRIORITY_DEFAULT);
    switch (feed->kind) {
        case FEED_PUT: {
            _z_n_msg_make_push_put(&msg, &feed->key, NULL, NULL, qos, NULL, NULL, Z_RELIABILITY_RELIABLE, NULL);
            break;
        }
        case FEED_QUERY: {
            _z_slice_t parameters = _z_slice_null();
            _z_n_msg_make_query(&msg, &feed->key, &parameters, feed->rid, Z_RELIABILITY_RELIABLE,
                                Z_CONSOLIDATION_MODE_NONE, NULL, NULL, 0, NULL, qos, NULL, false);
            break;
        }
        case FEED_REPLY: {
            _z_id_t zid = _z_id_empty();
            _z_n_msg_make_reply_ok_put(&msg, &zid, feed->rid, &feed->key, Z_RELIABILITY_RELIABLE,
                                       Z_CONSOLIDATION_MODE_NONE, qos, NULL, NULL, NULL, NULL, NULL);
            break;
        }
        case FEED_DECL_SUBSCRIBER: {
            _z_n_msg_make_declare(&msg, _z_make_decl_subscriber(&feed->key, 1), _z_optional_id_make_none());
            break;
        }
        case FEED_DECL_QUERYABLE: {
            _z_n_msg_make_declare(&msg, _z_make_decl_queryable(&feed->key, 1, true, 0), _z_optional_id_make_none());
            break;
        }
        case FEED_DECL_TOKEN:
        default: {
            _z_n_msg_make_declare(&msg, _z_make_decl_token(&feed->key, 1), _z_optional_id_make_none());
            break;
        }
    }
    // The handler the transport calls for each decoded network message. An error from it closes the transport.
    feed->result = _z_handle_network_message(&g_fake_transport, &msg, &g_peer);
    _z_n_msg_clear(&msg);
    _z_atomic_bool_store(&feed->done, true, _z_memory_order_seq_cst);
    return _z_fut_fn_result_ready();
}

// Feeds one message, waits until the session handled it and returns what the handler reported.
static z_result_t feed(feed_kind_t kind, _z_wireexpr_t key, _z_zint_t rid) {
    feed_t f;
    f.kind = kind;
    f.key = key;
    f.rid = rid;
    f.result = _Z_ERR_GENERIC;
    _z_atomic_bool_init(&f.done, false);
    _z_fut_t fut = _z_fut_new(&f, feed_task, NULL);
    ASSERT_TRUE(!_z_fut_handle_is_null(_z_runtime_spawn(&g_session._runtime, &fut)));
    ASSERT_TRUE(wait_for(&f.done));
    return f.result;
}

/*------------------ Entities ------------------*/
static void sample_callback(_z_sample_t *sample, void *arg) {
    _ZP_UNUSED(sample);
    _z_atomic_size_fetch_add((_z_atomic_size_t *)arg, 1, _z_memory_order_seq_cst);
}

static void query_callback(_z_query_rc_t *query, void *arg) {
    _ZP_UNUSED(query);
    _z_atomic_size_fetch_add((_z_atomic_size_t *)arg, 1, _z_memory_order_seq_cst);
}

static size_t count(_z_atomic_size_t *calls) { return _z_atomic_size_load(calls, _z_memory_order_seq_cst); }

static void declare_subscriber(_z_subscriber_t *sub, _z_atomic_size_t *calls) {
    _z_atomic_size_init(calls, 0);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    ASSERT_OK(_z_declare_subscriber(sub, &g_session_rc, &ke, sample_callback, NULL, calls, z_locality_default()));
}

static void undeclare_subscriber(_z_subscriber_t *sub) {
    ASSERT_OK(_z_undeclare_subscriber(sub));
    _z_subscriber_clear(sub);
}

/*------------------ Tests ------------------*/
// Samples that name the subscriber's key by its id, before and after the key is undeclared.
static void test_sample_for_an_undeclared_key_is_dropped(void) {
    setup_session();
    _z_subscriber_t a;
    _z_subscriber_t b;
    _z_atomic_size_t calls_a;
    _z_atomic_size_t calls_b;
    declare_subscriber(&a, &calls_a);
    declare_subscriber(&b, &calls_b);
    uint16_t id = declared_resource_id();

    ASSERT_OK(feed(FEED_PUT, key_by_own_id(id), 0));
    ASSERT_TRUE(count(&calls_a) == 1);
    ASSERT_TRUE(count(&calls_b) == 1);

    // One subscriber is left: the key stays declared and the id still resolves
    undeclare_subscriber(&a);
    ASSERT_TRUE(declared_resources() == 1);
    ASSERT_OK(feed(FEED_PUT, key_by_own_id(id), 0));
    ASSERT_TRUE(count(&calls_a) == 1);
    ASSERT_TRUE(count(&calls_b) == 2);

    // The last one undeclares the key with it. A sample the router sent before it knew is not an error.
    undeclare_subscriber(&b);
    ASSERT_TRUE(declared_resources() == 0);
    ASSERT_OK(feed(FEED_PUT, key_by_own_id(id), 0));
    ASSERT_OK(feed(FEED_PUT, key_by_name(), 0));
    ASSERT_TRUE(count(&calls_a) == 1);
    ASSERT_TRUE(count(&calls_b) == 2);

    // The peer never declared this id in its own mapping: that is a protocol error
    ASSERT_EQ_I32(feed(FEED_PUT, key_by_peer_id(id), 0), _Z_ERR_KEYEXPR_UNKNOWN);

    cleanup_session();
}

// A query for a queryable that is gone: it is dropped, and the other side gets the final it waits for.
static void test_query_for_an_undeclared_key_gets_its_final(void) {
    setup_session();
    _z_queryable_t qle;
    _z_atomic_size_t calls;
    _z_atomic_size_init(&calls, 0);
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(KEY);
    ASSERT_OK(_z_declare_queryable(&qle, &g_session_rc, &ke, true, query_callback, NULL, &calls, z_locality_default()));
    uint16_t id = declared_resource_id();

    ASSERT_OK(feed(FEED_QUERY, key_by_own_id(id), 1));
    ASSERT_TRUE(count(&calls) == 1);

    ASSERT_OK(_z_undeclare_queryable(&qle));
    _z_queryable_clear(&qle);
    ASSERT_TRUE(declared_resources() == 0);

    ASSERT_TRUE(finals(2) == 0);
    ASSERT_OK(feed(FEED_QUERY, key_by_own_id(id), 2));
    ASSERT_TRUE(count(&calls) == 1);
    ASSERT_TRUE(finals(2) == 1);

    // As for a query that no queryable matches
    ASSERT_OK(feed(FEED_QUERY, key_by_name(), 3));
    ASSERT_TRUE(finals(3) == 1);

    // A protocol error of the peer is reported, and nothing is answered
    ASSERT_EQ_I32(feed(FEED_QUERY, key_by_peer_id(id), 4), _Z_ERR_KEYEXPR_UNKNOWN);
    ASSERT_TRUE(finals(4) == 0);
    ASSERT_TRUE(count(&calls) == 1);

    cleanup_session();
}

// The other messages whose key the session resolves: a reply and the declarations a router sends for an interest.
static void test_reply_and_declarations_for_an_undeclared_key_are_dropped(void) {
    setup_session();
    _z_subscriber_t sub;
    _z_atomic_size_t calls;
    declare_subscriber(&sub, &calls);
    uint16_t id = declared_resource_id();
    undeclare_subscriber(&sub);
    ASSERT_TRUE(declared_resources() == 0);

    feed_kind_t kinds[] = {FEED_REPLY, FEED_DECL_SUBSCRIBER, FEED_DECL_QUERYABLE, FEED_DECL_TOKEN};
    for (size_t i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        ASSERT_OK(feed(kinds[i], key_by_own_id(id), 5));
        ASSERT_EQ_I32(feed(kinds[i], key_by_peer_id(id), 5), _Z_ERR_KEYEXPR_UNKNOWN);
    }
    // Only a request is answered
    ASSERT_TRUE(finals(5) == 0);
    ASSERT_TRUE(count(&calls) == 0);

    cleanup_session();
}

int main(void) {
    test_sample_for_an_undeclared_key_is_dropped();
    test_query_for_an_undeclared_key_gets_its_final();
    test_reply_and_declarations_for_an_undeclared_key_are_dropped();
    return 0;
}

#else

int main(void) { return 0; }

#endif
