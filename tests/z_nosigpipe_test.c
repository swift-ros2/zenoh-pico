// A send() on a TCP socket whose peer already closed the connection must fail
// with an error instead of raising SIGPIPE and killing the process.
//
// Linux passes MSG_NOSIGNAL on every send(); macOS / iOS / *BSD have no such
// flag and rely on SO_NOSIGPIPE being set on the socket in _z_open_tcp. When
// it is not, a late KEEP_ALIVE written after the router dropped the session
// terminates the whole client process (youtalk/swift-ros2#116).
#undef NDEBUG
#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zenoh-pico/system/link/tcp.h"
#include "zenoh-pico/system/platform.h"

#define SEND_ATTEMPTS 20

int main(void) {
    // Make sure SIGPIPE has its default (fatal) disposition, even if the
    // test runner ignores it.
    signal(SIGPIPE, SIG_DFL);

    // Local listener on an ephemeral port.
    int lfd = socket(AF_INET, SOCK_STREAM, 0);
    assert(lfd >= 0);
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    int rc = bind(lfd, (struct sockaddr *)&addr, sizeof(addr));
    assert(rc == 0);
    rc = listen(lfd, 1);
    assert(rc == 0);
    socklen_t addr_len = sizeof(addr);
    rc = getsockname(lfd, (struct sockaddr *)&addr, &addr_len);
    assert(rc == 0);
    char port[8];
    snprintf(port, sizeof(port), "%u", (unsigned)ntohs(addr.sin_port));

    // Connect with zenoh-pico's own TCP open path.
    _z_sys_net_endpoint_t ep;
    z_result_t res = _z_create_endpoint_tcp(&ep, "127.0.0.1", port);
    assert(res == _Z_RES_OK);
    _z_sys_net_socket_t sock;
    res = _z_open_tcp(&sock, ep, 1000);
    assert(res == _Z_RES_OK);

    // The peer accepts and immediately goes away.
    int pfd = accept(lfd, NULL, NULL);
    assert(pfd >= 0);
    close(pfd);
    close(lfd);

    // The first send after the peer's FIN may still succeed (the peer answers
    // it with RST); the following ones hit EPIPE. None of them may kill us.
    uint8_t payload[64];
    memset(payload, 0xAB, sizeof(payload));
    int failures = 0;
    for (int i = 0; i < SEND_ATTEMPTS; i++) {
        size_t sb = _z_send_tcp(sock, payload, sizeof(payload));
        if (sb == SIZE_MAX) {
            failures++;
        }
        z_sleep_ms(20);
    }
    printf("sends failed with an error: %d/%d, process still alive\n", failures, SEND_ATTEMPTS);
    assert(failures > 0);

#if defined(SO_NOSIGPIPE)
    int nosigpipe = 0;
    socklen_t opt_len = sizeof(nosigpipe);
    rc = getsockopt(sock._fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, &opt_len);
    assert(rc == 0);
    assert(nosigpipe != 0);
#endif

    _z_close_tcp(&sock);
    _z_free_endpoint_tcp(&ep);
    return 0;
}
