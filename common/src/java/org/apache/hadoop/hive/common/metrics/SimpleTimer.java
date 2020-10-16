package org.apache.hadoop.hive.common.metrics;

import java.util.HashMap;

public abstract class SimpleTimer {
    private static final ThreadLocal<HashMap<String, Long>> threadLocalStartTimes =
            new ThreadLocal<HashMap<String, Long>>() {
                @Override
                protected HashMap<String, Long> initialValue() {
                    return new HashMap<>();
                }
            };

    public static void start(String name) {
        threadLocalStartTimes.get().put(name, System.currentTimeMillis());
    }

    public static Long stop(String name) {
        Long elapsedTimeMs = null;
        if (threadLocalStartTimes.get().containsKey(name)) {
            elapsedTimeMs = System.currentTimeMillis() - threadLocalStartTimes.get().remove(name);
        }
        return elapsedTimeMs;
    }
}
