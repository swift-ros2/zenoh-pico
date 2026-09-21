// z_sleep_ms must not tick at 1 kHz: iOS throttles processes that sustain
// more than ~150 wakeups/s, which delays the lease task's KEEP_ALIVE past the
// session lease on subscriber-only sessions (youtalk/swift-ros2#116).
#include <assert.h>
#include <stdio.h>
#include <sys/resource.h>

#include "zenoh-pico/system/platform.h"

int main(void) {
    struct rusage before, after;
    getrusage(RUSAGE_SELF, &before);
    z_time_t start = z_time_now();

    z_result_t ret = z_sleep_ms(500);

    unsigned long elapsed = z_time_elapsed_ms(&start);
    getrusage(RUSAGE_SELF, &after);
    long switches = after.ru_nvcsw - before.ru_nvcsw;
    printf("elapsed=%lums voluntary_ctx_switches=%ld\n", elapsed, switches);

    assert(ret == _Z_RES_OK);
    assert(elapsed >= 500);  // "sleep at least the requested duration" still holds
    assert(switches < 50);   // the 1 kHz loop yields ~400+ here
    return 0;
}
