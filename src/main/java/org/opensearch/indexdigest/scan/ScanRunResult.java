/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

public final class ScanRunResult {
    public final int attemptedShardCopies;
    public final int skippedInFlightShardCopies;

    public ScanRunResult(int attemptedShardCopies, int skippedInFlightShardCopies) {
        this.attemptedShardCopies = attemptedShardCopies;
        this.skippedInFlightShardCopies = skippedInFlightShardCopies;
    }
}
