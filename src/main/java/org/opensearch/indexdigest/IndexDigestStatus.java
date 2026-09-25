/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

/**
 * Minimal status payload returned by REST.
 */
public final class IndexDigestStatus {
    public final boolean running;
    public final long lastRunEpochMillis;
    public final String lastError;
    public final int lastSkippedInFlightShardCopies;

    public IndexDigestStatus(
            boolean running,
            long lastRunEpochMillis,
            String lastError,
            int lastSkippedInFlightShardCopies
    ) {
        this.running = running;
        this.lastRunEpochMillis = lastRunEpochMillis;
        this.lastError = lastError;
        this.lastSkippedInFlightShardCopies = lastSkippedInFlightShardCopies;
    }
}
