/*
 * Copyright (c) 2024, Apusic
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *        http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

#ifndef __LATENCY_H
#define __LATENCY_H

#define LATENCY_TS_LEN 160 /* History length for every monitored event. */

/* Representation of a latency sample: the sampling time and the latency
 * observed in milliseconds. */
struct latencySample {
    int32_t time; /* We don't use time_t to force 4 bytes usage everywhere. */
    uint32_t latency; /* Latency in milliseconds. */
};

/* The latency time series for a given event. */
struct latencyTimeSeries {
    int idx; /* Index of the next sample to store. */
    uint32_t max; /* Max latency observed for this event. */
    struct latencySample samples[LATENCY_TS_LEN]; /* Latest history. */
};

/* Latency statistics structure. */
struct latencyStats {
    uint32_t all_time_high; /* Absolute max observed since latest reset. */
    uint32_t avg;           /* Average of current samples. */
    uint32_t min;           /* Min of current samples. */
    uint32_t max;           /* Max of current samples. */
    uint32_t mad;           /* Mean absolute deviation. */
    uint32_t samples;       /* Number of non-zero samples. */
    time_t period;          /* Number of seconds since first event and now. */
};

void latencyMonitorInit(void);
void latencyAddSample(const char *event, mstime_t latency);
int THPIsEnabled(void);
int THPDisable(void);

/* Latency monitoring macros. */

/* Start monitoring an event. We just set the current time. */
#define latencyStartMonitor(var) if (server.latency_monitor_threshold) { \
    var = mstime(); \
} else { \
    var = 0; \
}

/* End monitoring an event, compute the difference with the current time
 * to check the amount of time elapsed. */
#define latencyEndMonitor(var) if (server.latency_monitor_threshold) { \
    var = mstime() - var; \
}

/* Add the sample only if the elapsed time is >= to the configured threshold. */
#define latencyAddSampleIfNeeded(event,var) \
    if (server.latency_monitor_threshold && \
        (var) >= server.latency_monitor_threshold) \
          latencyAddSample((event),(var));

/* Remove time from a nested event. */
#define latencyRemoveNestedEvent(event_var,nested_var) \
    event_var += nested_var;

#endif /* __LATENCY_H */
