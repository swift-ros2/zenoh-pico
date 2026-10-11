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

// Network-less test of how a remote declaration is routed to the session's interests when the router tags it with an
// interest id. Some routers tag a future declaration with the id of an unrelated interest (typically the session's
// latest one). The declaration must still reach every interest whose key matches, otherwise a publisher's write
// filter never opens and its matching status stays false.

#undef NDEBUG
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "zenoh-pico/api/constants.h"
#include "zenoh-pico/api/primitives.h"
#include "zenoh-pico/api/types.h"
#include "zenoh-pico/net/filtering.h"
#include "zenoh-pico/net/primitives.h"
#include "zenoh-pico/protocol/core.h"
#include "zenoh-pico/protocol/definitions/declarations.h"
#include "zenoh-pico/protocol/definitions/network.h"
#include "zenoh-pico/protocol/keyexpr.h"
#include "zenoh-pico/session/interest.h"
#include "zenoh-pico/session/loopback.h"
#include "zenoh-pico/session/session.h"
#include "zenoh-pico/session/utils.h"
#include "zenoh-pico/system/platform.h"
#include "zenoh-pico/transport/transport.h"
#include "zenoh-pico/utils/result.h"

#if Z_FEATURE_INTEREST == 1

#define KEY_A "zenoh-pico/interest-fallback/a"
#define KEY_B "zenoh-pico/interest-fallback/b"
#define UNKNOWN_INTEREST_ID 9999u

static _z_session_t g_session;
static _z_session_rc_t g_session_rc = {0};
static _z_transport_common_t g_fake_transport = {0};
static bool g_transport_ready = false;
static _z_link_t g_dummy_link = {0};

static _z_transport_common_t *transport_override(_z_session_t *zn) {
    if (g_transport_ready && zn == &g_session) {
        return &g_fake_transport;
    }
    return NULL;
}

// The interests are sent to a router in client mode: swallow them, there is no network.
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
    assert(_z_session_init(&g_session, &zid) == _Z_RES_OK);
    assert(g_session._mode == Z_WHATAMI_CLIENT);
    if (_Z_RC_IS_NULL(&g_session_rc)) {
        g_session_rc = _z_session_rc_new(&g_session);
        assert(!_Z_RC_IS_NULL(&g_session_rc));
    } else {
        g_session_rc._val = &g_session;
    }

    g_dummy_link = (_z_link_t){0};
    g_fake_transport = (_z_transport_common_t){0};
    g_fake_transport._session = _z_session_rc_clone_as_weak(&g_session_rc);
    g_fake_transport._link = &g_dummy_link;
    g_transport_ready = true;
    _z_session_set_transport_common_override(transport_override);
    _z_transport_set_send_n_msg_override(send_override);
    g_session._tp._type = _Z_TRANSPORT_UNICAST_TYPE;
}

static void cleanup_session(void) {
    // The session is torn down as "closed" (no transport to close), which skips its entity cleanup: flush the
    // interests and the remote declarations the tests registered.
    _z_flush_interest(&g_session);
    g_transport_ready = false;
    g_session._tp._type = _Z_TRANSPORT_NONE;
    _z_session_clear(&g_session);
    _z_session_set_transport_common_override(NULL);
    _z_transport_set_send_n_msg_override(NULL);
}

/*------------------ Remote declarations ------------------*/
static void feed_decl_subscriber(const char *key, uint32_t decl_id, _z_optional_id_t interest_id) {
    _z_keyexpr_t ke = _z_rname(key);
    _z_n_msg_declare_t msg = {0};
    msg._decl = _z_make_decl_subscriber(&ke, decl_id);
    msg._interest_id = interest_id;
    assert(_z_interest_process_declares(&g_session, &msg, NULL) == _Z_RES_OK);
    _z_n_msg_declare_clear(&msg);
}

static void feed_decl_queryable(const char *key, uint32_t decl_id, _z_optional_id_t interest_id) {
    _z_keyexpr_t ke = _z_rname(key);
    _z_n_msg_declare_t msg = {0};
    msg._decl = _z_make_decl_queryable(&ke, decl_id, false, 0);
    msg._interest_id = interest_id;
    assert(_z_interest_process_declares(&g_session, &msg, NULL) == _Z_RES_OK);
    _z_n_msg_declare_clear(&msg);
}

// Undeclares carry no key and no interest id on the wire (the session finds the key from the earlier declaration).
static void feed_undecl_subscriber(uint32_t decl_id) {
    _z_declaration_t decl = _z_make_undecl_subscriber(decl_id, NULL);
    assert(_z_interest_process_undeclares(&g_session, &decl, NULL) == _Z_RES_OK);
    _z_declaration_clear(&decl);
}

/*------------------ Counting interests ------------------*/
typedef struct {
    unsigned decl_subscriber;
    unsigned undecl_subscriber;
    unsigned other;
    uint32_t last_decl_id;
} counter_t;

static void counting_handler(const _z_interest_msg_t *msg, _z_transport_peer_common_t *peer, void *arg) {
    _ZP_UNUSED(peer);
    counter_t *c = (counter_t *)arg;
    switch (msg->type) {
        case _Z_INTEREST_MSG_TYPE_DECL_SUBSCRIBER:
            c->decl_subscriber++;
            c->last_decl_id = msg->id;
            break;
        case _Z_INTEREST_MSG_TYPE_UNDECL_SUBSCRIBER:
            c->undecl_subscriber++;
            break;
        default:
            c->other++;
            break;
    }
}

static void noop_deleter(void *val) { _ZP_UNUSED(val); }

// Registers an interest whose counter lives on the heap: the interest owns it (dropping the interest frees it), so the
// counter stays valid until the session is flushed.
static uint32_t add_counting_interest(const char *key, uint8_t flags, counter_t **counter) {
    *counter = (counter_t *)z_malloc(sizeof(counter_t));
    assert(*counter != NULL);
    **counter = (counter_t){0};
    _z_void_rc_t arg = _z_void_rc_rc_new(*counter, noop_deleter);
    assert(!_Z_RC_IS_NULL(&arg));
    uint32_t id = _z_add_interest(&g_session, _z_rname(key), counting_handler, flags, &arg);
    assert(id != 0);
    return id;
}

#define SUBSCRIBER_INTEREST_FLAGS (_Z_INTEREST_FLAG_SUBSCRIBERS | _Z_INTEREST_FLAG_CURRENT | _Z_INTEREST_FLAG_FUTURE)

/*------------------ Tests ------------------*/
// The reported bug: the declaration for A is tagged with the latest interest, which belongs to B.
static void test_write_filter_opens_when_the_tag_belongs_to_another_interest(void) {
    setup_session();

    _z_write_filter_t filter_a = {0};
    _z_write_filter_t filter_b = {0};
    assert(_z_write_filter_create(&g_session_rc, &filter_a, _z_rname(KEY_A), _Z_INTEREST_FLAG_SUBSCRIBERS, false,
                                  Z_LOCALITY_ANY) == _Z_RES_OK);
    assert(_z_write_filter_create(&g_session_rc, &filter_b, _z_rname(KEY_B), _Z_INTEREST_FLAG_SUBSCRIBERS, false,
                                  Z_LOCALITY_ANY) == _Z_RES_OK);
    assert(filter_b._interest_id > filter_a._interest_id);
    assert(_z_write_filter_active(&filter_a));
    assert(_z_write_filter_active(&filter_b));

    feed_decl_subscriber(KEY_A, 100, _z_optional_id_make_some(filter_b._interest_id));
    assert(!_z_write_filter_active(&filter_a));  // A has a subscriber: its filter is open
    assert(_z_write_filter_active(&filter_b));   // B has none

    // The undeclare carries no interest id: it closes the filter again.
    feed_undecl_subscriber(100);
    assert(_z_write_filter_active(&filter_a));
    assert(_z_write_filter_active(&filter_b));

    _z_write_filter_clear(&filter_a);
    _z_write_filter_clear(&filter_b);
    cleanup_session();
}

// The tag names an interest that does not exist (any more).
static void test_write_filter_opens_when_the_tag_is_unknown(void) {
    setup_session();

    _z_write_filter_t filter_a = {0};
    assert(_z_write_filter_create(&g_session_rc, &filter_a, _z_rname(KEY_A), _Z_INTEREST_FLAG_SUBSCRIBERS, false,
                                  Z_LOCALITY_ANY) == _Z_RES_OK);
    assert(_z_write_filter_active(&filter_a));

    feed_decl_subscriber(KEY_A, 101, _z_optional_id_make_some(UNKNOWN_INTEREST_ID));
    assert(!_z_write_filter_active(&filter_a));

    feed_undecl_subscriber(101);
    assert(_z_write_filter_active(&filter_a));

    _z_write_filter_clear(&filter_a);
    cleanup_session();
}

// Normal flows keep working: the declaration tagged with the right interest, or not tagged at all.
static void test_write_filter_opens_when_the_tag_is_correct_or_absent(void) {
    setup_session();

    _z_write_filter_t filter_a = {0};
    assert(_z_write_filter_create(&g_session_rc, &filter_a, _z_rname(KEY_A), _Z_INTEREST_FLAG_SUBSCRIBERS, false,
                                  Z_LOCALITY_ANY) == _Z_RES_OK);

    feed_decl_subscriber(KEY_A, 102, _z_optional_id_make_some(filter_a._interest_id));
    assert(!_z_write_filter_active(&filter_a));
    feed_undecl_subscriber(102);
    assert(_z_write_filter_active(&filter_a));

    feed_decl_subscriber(KEY_A, 103, _z_optional_id_make_none());
    assert(!_z_write_filter_active(&filter_a));
    feed_undecl_subscriber(103);
    assert(_z_write_filter_active(&filter_a));

    _z_write_filter_clear(&filter_a);
    cleanup_session();
}

// A tag that matches an interest keeps the declaration restricted to it: the fallback must not also hand it to the
// other interests with the same key (a CURRENT reply is per interest).
static void test_matching_tag_is_not_double_processed(void) {
    setup_session();

    counter_t *first = NULL;
    counter_t *second = NULL;
    uint32_t first_id = add_counting_interest(KEY_A, SUBSCRIBER_INTEREST_FLAGS, &first);
    uint32_t second_id = add_counting_interest(KEY_A, SUBSCRIBER_INTEREST_FLAGS, &second);
    assert(first_id != second_id);

    feed_decl_subscriber(KEY_A, 110, _z_optional_id_make_some(first_id));
    assert(first->decl_subscriber == 1);
    assert(second->decl_subscriber == 0);

    feed_decl_subscriber(KEY_A, 111, _z_optional_id_make_some(second_id));
    assert(first->decl_subscriber == 1);
    assert(second->decl_subscriber == 1);

    // No tag, and the tag 0 used by peers for declaration propagation: every matching interest, once.
    feed_decl_subscriber(KEY_A, 112, _z_optional_id_make_none());
    assert(first->decl_subscriber == 2);
    assert(second->decl_subscriber == 2);
    feed_decl_subscriber(KEY_A, 113, _z_optional_id_make_some(0));
    assert(first->decl_subscriber == 3);
    assert(second->decl_subscriber == 3);

    cleanup_session();
}

// A tag that matches no interest by key: each interest with a matching key sees the declaration exactly once.
static void test_fallback_delivers_once_per_matching_interest(void) {
    setup_session();

    counter_t *a1 = NULL;
    counter_t *a2 = NULL;
    counter_t *b = NULL;
    uint32_t a1_id = add_counting_interest(KEY_A, SUBSCRIBER_INTEREST_FLAGS, &a1);
    uint32_t a2_id = add_counting_interest(KEY_A, SUBSCRIBER_INTEREST_FLAGS, &a2);
    uint32_t b_id = add_counting_interest(KEY_B, SUBSCRIBER_INTEREST_FLAGS, &b);
    assert(a1_id < a2_id && a2_id < b_id);

    // Tagged with B's id, but about A: B's interest exists and does not match the key.
    feed_decl_subscriber(KEY_A, 120, _z_optional_id_make_some(b_id));
    assert(a1->decl_subscriber == 1 && a1->last_decl_id == 120);
    assert(a2->decl_subscriber == 1 && a2->last_decl_id == 120);
    assert(b->decl_subscriber == 0);

    // Tagged with an unknown id.
    feed_decl_subscriber(KEY_B, 121, _z_optional_id_make_some(UNKNOWN_INTEREST_ID));
    assert(a1->decl_subscriber == 1);
    assert(a2->decl_subscriber == 1);
    assert(b->decl_subscriber == 1 && b->last_decl_id == 121);

    // The undeclares go to the interests that match the declaration's key, once each.
    feed_undecl_subscriber(120);
    assert(a1->undecl_subscriber == 1 && a2->undecl_subscriber == 1 && b->undecl_subscriber == 0);
    feed_undecl_subscriber(121);
    assert(a1->undecl_subscriber == 1 && a2->undecl_subscriber == 1 && b->undecl_subscriber == 1);

    assert(a1->other == 0 && a2->other == 0 && b->other == 0);
    cleanup_session();
}

// The fallback still honors the interest flags: a subscriber declaration is not for a queryable interest, and
// a queryable declaration is not for a subscriber interest.
static void test_fallback_honors_interest_flags(void) {
    setup_session();

    counter_t *queryable_interest = NULL;
    counter_t *subscriber_interest = NULL;
    uint32_t queryable_id = add_counting_interest(
        KEY_A, _Z_INTEREST_FLAG_QUERYABLES | _Z_INTEREST_FLAG_CURRENT | _Z_INTEREST_FLAG_FUTURE, &queryable_interest);
    uint32_t subscriber_id = add_counting_interest(KEY_B, SUBSCRIBER_INTEREST_FLAGS, &subscriber_interest);
    assert(queryable_id < subscriber_id);

    feed_decl_subscriber(KEY_A, 130, _z_optional_id_make_some(subscriber_id));
    assert(queryable_interest->decl_subscriber == 0);
    assert(queryable_interest->other == 0);
    assert(subscriber_interest->decl_subscriber == 0);

    feed_decl_queryable(KEY_B, 131, _z_optional_id_make_some(queryable_id));
    assert(queryable_interest->other == 0);
    assert(subscriber_interest->decl_subscriber == 0);
    assert(subscriber_interest->other == 0);

    cleanup_session();
}

int main(void) {
    test_write_filter_opens_when_the_tag_belongs_to_another_interest();
    test_write_filter_opens_when_the_tag_is_unknown();
    test_write_filter_opens_when_the_tag_is_correct_or_absent();
    test_matching_tag_is_not_double_processed();
    test_fallback_delivers_once_per_matching_interest();
    test_fallback_honors_interest_flags();
    printf("z_interest_fallback_test: all tests passed\n");
    return 0;
}

#else

int main(void) {
    printf("z_interest_fallback_test: skipped (needs Z_FEATURE_INTEREST)\n");
    return 0;
}

#endif
