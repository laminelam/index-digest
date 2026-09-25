/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.BucketRange;
import org.opensearch.indexdigest.model.ProgressDoc;

final class SealedLeafRunPlanner {
    private final BucketMath bucketMath;
    private final int minLevel;
    private final int maxLevel;
    private final int maxTopBucketsPerRun;
    private final int guardWindows;

    SealedLeafRunPlanner(
            BucketMath bucketMath,
            int minLevel,
            int maxLevel,
            int maxTopBucketsPerRun,
            int guardWindows
    ) {
        if (minLevel > maxLevel) {
            throw new IllegalArgumentException(
                    "index-digest requires min_level <= max_level but found min_level="
                            + minLevel + " max_level=" + maxLevel
            );
        }
        if (maxTopBucketsPerRun < 1) {
            throw new IllegalArgumentException("maxTopBucketsPerRun must be >= 1");
        }
        if (guardWindows < 0) {
            throw new IllegalArgumentException("guardWindows must be >= 0");
        }

        this.bucketMath = bucketMath;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.maxTopBucketsPerRun = maxTopBucketsPerRun;
        this.guardWindows = guardWindows;
    }

    SealedLeafRun plan(
            ProgressDoc progress,
            long minObservedExternalVersion,
            long maxObservedExternalVersion
    ) {
        long currentOpenLeafBucket = bucketMath.bucketId(maxObservedExternalVersion, minLevel);
        long lastSafeLeafBucket = bucketMath.prevBucket(currentOpenLeafBucket, minLevel);
        if (lastSafeLeafBucket < 0L) {
            return SealedLeafRun.empty();
        }

        long firstUnprocessedLeaf = progress.lastSealedLeafBucketId < 0L
                ? bucketMath.bucketId(minObservedExternalVersion, minLevel)
                : bucketMath.nextBucket(progress.lastSealedLeafBucketId, minLevel);

        if (firstUnprocessedLeaf > lastSafeLeafBucket) {
            return SealedLeafRun.empty();
        }

        long startTopBucket = bucketMath.bucketId(firstUnprocessedLeaf, maxLevel);

        long candidateEndTopBucket = bucketMath.bucketId(lastSafeLeafBucket, maxLevel);
        BucketRange candidateEndLeafRange = bucketMath.getBucketBoundaries(candidateEndTopBucket, maxLevel, minLevel);

        if (candidateEndLeafRange.endBucket() > lastSafeLeafBucket) {
            candidateEndTopBucket = bucketMath.prevBucket(candidateEndTopBucket, maxLevel);
        }

        for (int i = 0; i < guardWindows; i++) {
            candidateEndTopBucket = bucketMath.prevBucket(candidateEndTopBucket, maxLevel);
        }

        if (candidateEndTopBucket < startTopBucket) {
            return SealedLeafRun.empty();
        }

        long endTopBucket = startTopBucket;
        for (long i = 1; i < maxTopBucketsPerRun; i++) {
            long nextTopBucket = bucketMath.nextBucket(endTopBucket, maxLevel);
            if (nextTopBucket > candidateEndTopBucket) {
                break;
            }
            endTopBucket = nextTopBucket;
        }

        BucketRange startLeafRange = bucketMath.getBucketBoundaries(startTopBucket, maxLevel, minLevel);
        BucketRange endLeafRange = bucketMath.getBucketBoundaries(endTopBucket, maxLevel, minLevel);

        long startLeafBucket = startLeafRange.startBucket();
        long endLeafBucket = endLeafRange.endBucket();

        if (startLeafBucket > endLeafBucket) {
            return SealedLeafRun.empty();
        }

        return new SealedLeafRun(startLeafBucket, endLeafBucket);
    }
}
