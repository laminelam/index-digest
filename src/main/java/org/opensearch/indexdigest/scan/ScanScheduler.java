/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.indexdigest.IndexDigestSettings;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * v1 scheduler: fixed-delay loop (cron-like).
 * Later you can swap to JobScheduler SPI or event-driven hooks.
 */
public final class ScanScheduler implements Closeable {
    private static final Logger logger = LogManager.getLogger(ScanScheduler.class);

    private final ThreadPool threadPool;
    private final ClusterSettings clusterSettings;
    private final ShardCopyResolver resolver;
    private final ShardScanner scanner;

    private final AtomicBoolean running = new AtomicBoolean(false);
    /**
     * Monotonic token identifying the current self-rescheduling chain. Every
     * (re)schedule and every cancel bumps it; a fired one-shot whose token is
     * stale exits without running or rescheduling. This is what prevents a
     * settings update landing mid-tick from spawning a second permanent chain.
     */
    private final java.util.concurrent.atomic.AtomicLong scheduleGeneration = new java.util.concurrent.atomic.AtomicLong();
    private volatile org.opensearch.threadpool.Scheduler.Cancellable cancellable;

    private volatile long lastRunEpochMillis = 0;
    private volatile String lastErrorMessage = null;
    private volatile int lastSkippedInFlightShardCopies = 0;

    private volatile boolean enabled;
    private volatile long intervalMillis;

    public ScanScheduler(
            ThreadPool threadPool,
            ClusterSettings clusterSettings,
            Settings nodeSettings,
            ShardCopyResolver resolver,
            ShardScanner scanner
    ) {
        this.threadPool = threadPool;
        this.clusterSettings = clusterSettings;
        this.resolver = resolver;
        this.scanner = scanner;

        // Initial values from node settings
        this.enabled = IndexDigestSettings.ENABLED.get(nodeSettings);
        this.intervalMillis = IndexDigestSettings.SCAN_INTERVAL.get(nodeSettings).millis();

        // Dynamic updates
        this.clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.ENABLED, v -> {
            this.enabled = v;
            if (v == false) {
                cancelInternal();
            } else {
                if (running.get() && cancellable == null) {
                    scheduleFixedDelay();
                }
            }
        });

        this.clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.SCAN_INTERVAL, v -> {
            this.intervalMillis = v.millis();
            if (running.get() && enabled) {
                reschedule();
            }
        });
    }

    public void start() {
        if (running.compareAndSet(false, true) == false) {
            return;
        }
        if (enabled == false) {
            logger.debug("index-digest disabled; scheduler idle");
            return;
        }
        scheduleFixedDelay();
    }

    /**
     * Ticks fire at wall-clock boundaries (UTC epoch multiples of the interval),
     * not at fixed delays from node start. Every node and every cluster running
     * this plugin therefore seals within NTP skew + tick jitter of each other,
     * shrinking the cross-cluster seal-timing race from minutes to seconds.
     */
    private void scheduleFixedDelay() {
        final long generation = scheduleGeneration.incrementAndGet();
        long delay = delayToNextBoundaryMillis(threadPool.absoluteTimeInMillis(), intervalMillis);
        this.cancellable = threadPool.schedule(
                () -> runAndReschedule(generation),
                TimeValue.timeValueMillis(delay),
                ThreadPool.Names.GENERIC
        );
    }

    private void runAndReschedule(long generation) {
        if (scheduleGeneration.get() != generation) {
            // Superseded by a reschedule/cancel while queued: another chain owns
            // the schedule now.
            return;
        }
        try {
            safeRun();
        } finally {
            if (running.get() && enabled && scheduleGeneration.get() == generation) {
                scheduleFixedDelay();
            }
        }
    }

    // Package-private for tests.
    static long delayToNextBoundaryMillis(long nowMillis, long intervalMillis) {
        long untilBoundary = intervalMillis - (nowMillis % intervalMillis);
        return untilBoundary == 0 ? intervalMillis : untilBoundary;
    }

    private void safeRun() {
        if (running.get() == false || enabled == false) {
            return;
        }
        try {
            int failureCount = 0;
            int attemptedCount = 0;
            int skippedInFlightCount = 0;
            StringBuilder failures = new StringBuilder();
            for (ShardCopyKey key : resolver.resolveLocalShardCopies()) {
                try {
                    if (scanner.scanShardCopy(key)) {
                        attemptedCount++;
                    } else {
                        skippedInFlightCount++;
                    }
                } catch (Exception e) {
                    failureCount++;
                    appendFailure(failures, key, e);
                    logger.warn("index-digest scan failed for shard copy {}", key, e);
                }
            }
            if (failureCount == 0) {
                lastErrorMessage = null;
            } else {
                lastErrorMessage = "index-digest scan failed for " + failureCount + " shard copies: " + failures;
            }
            lastSkippedInFlightShardCopies = skippedInFlightCount;
            logger.debug(
                    "index-digest scheduled scan attempted_shard_copies={} skipped_in_flight_shard_copies={}",
                    attemptedCount,
                    skippedInFlightCount
            );
        } catch (Exception e) {
            lastErrorMessage = e.getMessage();
            logger.warn("index-digest scan failed", e);
        } finally {
            lastRunEpochMillis = threadPool.absoluteTimeInMillis();
        }
    }

    private static void appendFailure(StringBuilder failures, ShardCopyKey key, Exception e) {
        if (failures.length() > 0) {
            failures.append("; ");
        }
        failures.append(key).append(": ").append(e.getMessage() == null ? e.toString() : e.getMessage());
    }

    private void reschedule() {
        cancelInternal();
        if (enabled) {
            scheduleFixedDelay();
        }
    }

    private void cancelInternal() {
        // Invalidate any queued or mid-run chain even when Future.cancel cannot
        // reach it anymore.
        scheduleGeneration.incrementAndGet();
        org.opensearch.threadpool.Scheduler.Cancellable c = cancellable;
        cancellable = null;
        if (c != null) {
            c.cancel();
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public long getLastRunEpochMillis() {
        return lastRunEpochMillis;
    }

    public String getLastErrorMessage() {
        return lastErrorMessage;
    }

    public int getLastSkippedInFlightShardCopies() {
        return lastSkippedInFlightShardCopies;
    }

    @Override
    public void close() {
        running.set(false);
        cancelInternal();
    }
}
