// z_sleep_ms must not tick at 1 kHz: iOS throttles processes that sustain
// more than ~150 wakeups/s, which delays the lease task's KEEP_ALIVE past the
// session lease on subscriber-only sessions (youtalk/swift-ros2#116).
//
// Wakeup counting is platform-specific: Linux's getrusage(RUSAGE_SELF)
// correctly attributes each timed-sleep wakeup to ru_nvcsw. Darwin's
// ru_nvcsw does not (it stays flat regardless of sleep pattern), so on
// __APPLE__ this counts via Mach task_info(TASK_EVENTS_INFO)'s csw field
// instead, which does discriminate a many-wakeups sleep loop from a single
// sleep.
#include <assert.h>
#include <stdio.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#else
#include <sys/resource.h>
#endif

#include "zenoh-pico/system/platform.h"

int main(void) {
#if defined(__APPLE__)
    task_events_info_data_t before, after;
    mach_msg_type_number_t info_count = TASK_EVENTS_INFO_COUNT;
    task_info(mach_task_self(), TASK_EVENTS_INFO, (task_info_t)&before, &info_count);
#else
    struct rusage before, after;
    getrusage(RUSAGE_SELF, &before);
#endif
    z_time_t start = z_time_now();

    z_result_t ret = z_sleep_ms(500);

    unsigned long elapsed = z_time_elapsed_ms(&start);
#if defined(__APPLE__)
    info_count = TASK_EVENTS_INFO_COUNT;
    task_info(mach_task_self(), TASK_EVENTS_INFO, (task_info_t)&after, &info_count);
    long wakeups = after.csw - before.csw;
    printf("elapsed=%lums context_switches(mach)=%ld\n", elapsed, wakeups);
#else
    getrusage(RUSAGE_SELF, &after);
    long wakeups = after.ru_nvcsw - before.ru_nvcsw;
    printf("elapsed=%lums voluntary_ctx_switches=%ld\n", elapsed, wakeups);
#endif

    assert(ret == _Z_RES_OK);
    assert(elapsed >= 500);  // "sleep at least the requested duration" still holds
    assert(wakeups < 50);    // the 1 kHz loop yields ~400+ here
    return 0;
}
