/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

final class SealedLeafRun {
    private static final SealedLeafRun EMPTY = new SealedLeafRun(-1L, -1L);

    final long startLeafBucketInclusive;
    final long endLeafBucketInclusive;

    SealedLeafRun(long startLeafBucketInclusive, long endLeafBucketInclusive) {
        this.startLeafBucketInclusive = startLeafBucketInclusive;
        this.endLeafBucketInclusive = endLeafBucketInclusive;
    }

    static SealedLeafRun empty() {
        return EMPTY;
    }

    boolean isEmpty() {
        return startLeafBucketInclusive < 0L || endLeafBucketInclusive < startLeafBucketInclusive;
    }
}
