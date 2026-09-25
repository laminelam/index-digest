/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

public final class LeafBucketDigest {
    public final int level;
    public final long bucketId;
    public final long bucketStartMillis;
    public final long bucketEndMillis;
    public final String digest;
    public final long liveDocCount;
    public final long deleteEventCount;

    public LeafBucketDigest(
            int level,
            long bucketId,
            long bucketStartMillis,
            long bucketEndMillis,
            String digest,
            long liveDocCount,
            long deleteEventCount
    ) {
        this.level = level;
        this.bucketId = bucketId;
        this.bucketStartMillis = bucketStartMillis;
        this.bucketEndMillis = bucketEndMillis;
        this.digest = digest;
        this.liveDocCount = liveDocCount;
        this.deleteEventCount = deleteEventCount;
    }
}
