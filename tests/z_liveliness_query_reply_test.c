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

// Network-less test of the replies to a liveliness query. A router answers the query (an interest in the current
// tokens only) with one token declaration per matching token, and gives every one of them the id 0, because a token
// reported this way is never undeclared. Each declaration must be handled without an error: an error from the network
// message handler closes the transport, so a query that matches two tokens would cost the session. The end of another
// interest of the session must not end the query either.

#undef NDEBUG
#include <assert.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "zenoh-pico/api/constants.h"
#include "zenoh-pico/api/primitives.h"
#include "zenoh-pico/api/types.h"
#include "zenoh-pico/net/liveliness.h"
#include "zenoh-pico/net/reply.h"
#include "zenoh-pico/protocol/core.h"
#include "zenoh-pico/protocol/definitions/declarations.h"
#include "zenoh-pico/protocol/definitions/network.h"
#include "zenoh-pico/session/keyexpr.h"
#include "zenoh-pico/session/liveliness.h"
#include "zenoh-pico/session/loopback.h"
#include "zenoh-pico/session/resource.h"
#include "zenoh-pico/session/session.h"
#include "zenoh-pico/session/utils.h"
#include "zenoh-pico/system/platform.h"
#include "zenoh-pico/transport/transport.h"
#include "zenoh-pico/utils/result.h"

#if Z_FEATURE_LIVELINESS == 1 && Z_FEATURE_QUERY == 1 && Z_FEATURE_SUBSCRIPTION == 1 && Z_FEATURE_INTEREST == 1

#define QUERY_KEY "zenoh-pico/liveliness-query/**"
#define TOKEN_A "zenoh-pico/liveliness-query/a"
#define TOKEN_B "zenoh-pico/liveliness-query/b"
#define TOKEN_C "zenoh-pico/liveliness-query/c"
#define MAX_REPLIES 8
#define MAX_KEY_LEN 64

static _z_session_t g_session;
static _z_session_rc_t g_session_rc = {0};
static _z_transport_common_t g_fake_transport = {0};
static bool g_transport_ready = false;
static _z_link_t g_dummy_link = {0};
static uint32_t g_last_interest_id = 0;

static _z_transport_common_t *transport_override(_z_session_t *zn) {
    if (g_transport_ready && zn == &g_session) {
        return &g_fake_transport;
    }
    return NULL;
}

// The query goes to a router as an interest: keep its id, there is no network.
static z_result_t send_override(_z_session_t *zn, const _z_network_message_t *n_msg, z_reliability_t reliability,
                                z_congestion_control_t cong_ctrl, void *peer, bool *handled) {
    _ZP_UNUSED(zn);
    _ZP_UNUSED(reliability);
    _ZP_UNUSED(cong_ctrl);
    _ZP_UNUSED(peer);
    if (n_msg->_tag == _Z_N_INTEREST) {
        g_last_interest_id = n_msg->_body._interest._interest._id;
    }
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
    g_last_interest_id = 0;
    _z_session_set_transport_common_override(transport_override);
    _z_transport_set_send_n_msg_override(send_override);
    g_session._tp._type = _Z_TRANSPORT_UNICAST_TYPE;
}

static void cleanup_session(void) {
    g_transport_ready = false;
    g_session._tp._type = _Z_TRANSPORT_NONE;
    _z_session_clear(&g_session);
    _z_session_set_transport_common_override(NULL);
    _z_transport_set_send_n_msg_override(NULL);
}

/*------------------ Query ------------------*/
typedef struct {
    unsigned replies;
    unsigned drops;
    char keys[MAX_REPLIES][MAX_KEY_LEN];
} query_result_t;

static void reply_handler(_z_reply_t *reply, void *arg) {
    query_result_t *res = (query_result_t *)arg;
    assert(res->replies < MAX_REPLIES);
    assert(reply->data._tag == _Z_REPLY_TAG_DATA);
    const _z_string_t *key = &reply->data._result.sample.keyexpr._inner._keyexpr;
    size_t len = _z_string_len(key);
    assert(len < MAX_KEY_LEN);
    memcpy(res->keys[res->replies], _z_string_data(key), len);
    res->keys[res->replies][len] = '\0';
    res->replies++;
}

static void drop_handler(void *arg) {
    query_result_t *res = (query_result_t *)arg;
    res->drops++;
}

static uint32_t start_query(const char *key, query_result_t *res) {
    *res = (query_result_t){0};
    g_last_interest_id = 0;
    _z_declared_keyexpr_t ke = _z_declared_keyexpr_alias_from_str(key);
    assert(_z_liveliness_query(&g_session_rc, &ke, reply_handler, drop_handler, res, 1000, NULL) == _Z_RES_OK);
    assert(g_last_interest_id != 0);
    return g_last_interest_id;
}

/*------------------ Router messages ------------------*/
// A wire expression as a router sends it for an undeclared key: no id, the whole key in the suffix.
static _z_wireexpr_t wireexpr_from_str(const char *key) {
    _z_wireexpr_t expr = _z_wireexpr_null();
    expr._suffix = _z_string_alias_str(key);
    return expr;
}

// Feeds the message through the handler the transport calls for each decoded network message: a result other than
// _Z_RES_OK there is what closes the transport.
static z_result_t feed_token(const char *key, uint32_t token_id, _z_optional_id_t interest_id) {
    _z_wireexpr_t ke = wireexpr_from_str(key);
    _z_network_message_t msg = {0};
    _z_n_msg_make_declare(&msg, _z_make_decl_token(&ke, token_id), interest_id);
    z_result_t ret = _z_handle_network_message(&g_fake_transport, &msg, NULL);
    _z_n_msg_clear(&msg);
    return ret;
}

static z_result_t feed_final(uint32_t interest_id) {
    _z_network_message_t msg = {0};
    _z_n_msg_make_declare(&msg, _z_make_decl_final(), _z_optional_id_make_some(interest_id));
    z_result_t ret = _z_handle_network_message(&g_fake_transport, &msg, NULL);
    _z_n_msg_clear(&msg);
    return ret;
}

/*------------------ Tests ------------------*/
// The reported bug: the second reply has the same token id (0) as the first one and another key.
static void test_query_with_several_tokens_gets_every_reply(void) {
    setup_session();

    query_result_t res;
    uint32_t query_id = start_query(QUERY_KEY, &res);

    assert(feed_token(TOKEN_A, 0, _z_optional_id_make_some(query_id)) == _Z_RES_OK);
    assert(feed_token(TOKEN_B, 0, _z_optional_id_make_some(query_id)) == _Z_RES_OK);
    assert(feed_token(TOKEN_C, 0, _z_optional_id_make_some(query_id)) == _Z_RES_OK);
    assert(res.replies == 3);
    assert(strcmp(res.keys[0], TOKEN_A) == 0);
    assert(strcmp(res.keys[1], TOKEN_B) == 0);
    assert(strcmp(res.keys[2], TOKEN_C) == 0);
    assert(res.drops == 0);

    assert(feed_final(query_id) == _Z_RES_OK);
    assert(res.replies == 3);
    assert(res.drops == 1);

    cleanup_session();
}

// A second query on the same session gets its replies too: the token id 0 left by the first query is not in the way.
static void test_second_query_gets_its_replies(void) {
    setup_session();

    query_result_t first;
    uint32_t first_id = start_query(QUERY_KEY, &first);
    assert(feed_token(TOKEN_A, 0, _z_optional_id_make_some(first_id)) == _Z_RES_OK);
    assert(feed_token(TOKEN_B, 0, _z_optional_id_make_some(first_id)) == _Z_RES_OK);
    assert(feed_final(first_id) == _Z_RES_OK);
    assert(first.replies == 2 && first.drops == 1);

    query_result_t second;
    uint32_t second_id = start_query(QUERY_KEY, &second);
    assert(second_id != first_id);
    assert(feed_token(TOKEN_B, 0, _z_optional_id_make_some(second_id)) == _Z_RES_OK);
    assert(feed_token(TOKEN_A, 0, _z_optional_id_make_some(second_id)) == _Z_RES_OK);
    assert(feed_final(second_id) == _Z_RES_OK);
    assert(second.replies == 2 && second.drops == 1);
    assert(strcmp(second.keys[0], TOKEN_B) == 0);
    assert(strcmp(second.keys[1], TOKEN_A) == 0);
    assert(first.replies == 2 && first.drops == 1);

    cleanup_session();
}

// A token declared twice with the same id and the same key (a repeated declaration) stays accepted.
static void test_repeated_token_declaration_is_accepted(void) {
    setup_session();

    assert(feed_token(TOKEN_A, 7, _z_optional_id_make_none()) == _Z_RES_OK);
    assert(feed_token(TOKEN_A, 7, _z_optional_id_make_none()) == _Z_RES_OK);

    cleanup_session();
}

// A router ends every interest with a DeclareFinal that carries the interest id: the write filter of a publisher, a
// liveliness subscriber, a liveliness query. The final of another interest must not end a pending query.
static void test_final_of_another_interest_does_not_end_the_query(void) {
    setup_session();

    // The id another interest of this session takes, declared just before the query.
    uint32_t other_interest_id = _z_get_entity_id(&g_session);

    query_result_t res;
    uint32_t query_id = start_query(QUERY_KEY, &res);
    assert(query_id != other_interest_id);

    assert(feed_final(other_interest_id) == _Z_RES_OK);
    assert(res.drops == 0);

    assert(feed_token(TOKEN_A, 0, _z_optional_id_make_some(query_id)) == _Z_RES_OK);
    assert(feed_token(TOKEN_B, 0, _z_optional_id_make_some(query_id)) == _Z_RES_OK);
    assert(res.replies == 2);
    assert(res.drops == 0);

    assert(feed_final(query_id) == _Z_RES_OK);
    assert(res.replies == 2);
    assert(res.drops == 1);

    cleanup_session();
}

int main(void) {
    test_query_with_several_tokens_gets_every_reply();
    test_second_query_gets_its_replies();
    test_repeated_token_declaration_is_accepted();
    test_final_of_another_interest_does_not_end_the_query();
    printf("z_liveliness_query_reply_test: all tests passed\n");
    return 0;
}

#else

int main(void) {
    printf("z_liveliness_query_reply_test: skipped (needs liveliness, queries, subscriptions and interests)\n");
    return 0;
}

#endif
