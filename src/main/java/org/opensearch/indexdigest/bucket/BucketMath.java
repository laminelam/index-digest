/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.bucket;

/**
 * Pure bucket topology math.
 *
 * Bucket layout:
 *
 * {@code bucketId_L = (value >> L) << L}
 *
 * A bucket at level L has size 2^L.
 *
 * This class intentionally contains no runtime concepts such as
 * sealing, scanning, shard state, clocks, or persistence.
 */
public final class BucketMath {
    public static final BucketMath INSTANCE = new BucketMath();

    private BucketMath() {}

    /**
     * Returns bucket size for a given level.
     */
    public long bucketSize(int level) {
        if (level < 0 || level >= Long.SIZE - 1) {
            throw new IllegalArgumentException("level must be in [0, 63): " + level);
        }
        return 1L << level;
    }

    /**
     * Returns the bucket id/aligned bucket start for a value.
     */
    public long bucketId(long value, int level) {
        if (level < 0 || level >= Long.SIZE - 1) {
            throw new IllegalArgumentException("level must be in [0, 63): " + level);
        }
        return (value >> level) << level;
    }

    /**
     * Returns the next bucket at the same level.
     */
    public long nextBucket(long bucket, int level) {
        return bucket + bucketSize(level);
    }

    /**
     * Returns the previous bucket at the same level.
     */
    public long prevBucket(long bucket, int level) {
        return bucket - bucketSize(level);
    }

    /**
     * Returns the descendant bucket range of a parent bucket
     * at a lower level.
     *
     * Example:
     *
     * bucketLevel = 12
     * targetLevel = 10
     *
     * bucket 0 at L12 contains L10 buckets:
     *
     * [0, 1024, 2048, 3072]
     *
     * so this returns:
     *
     * startBucket = 0
     * endBucket = 3072
     */
    public BucketRange getBucketBoundaries(long bucket, int bucketLevel, int targetLevel) {
        if (targetLevel > bucketLevel) {
            throw new IllegalArgumentException("targetLevel cannot be higher than bucketLevel");
        }

        if (bucketLevel < 0 || bucketLevel >= Long.SIZE - 1) {
            throw new IllegalArgumentException("bucketLevel must be in [0, 63): " + bucketLevel);
        }

        if (targetLevel < 0 || targetLevel >= Long.SIZE - 1) {
            throw new IllegalArgumentException("targetLevel must be in [0, 63): " + targetLevel);
        }

        long start = bucketId(bucket, bucketLevel);
        if (targetLevel == bucketLevel) {
            return new BucketRange(start, start);
        }

        long endExclusive = nextBucket(start, bucketLevel);
        long lastTarget = prevBucket(endExclusive, targetLevel);

        return new BucketRange(start, lastTarget);
    }

    /**
     * Returns true if parentBucket is the direct parent of childBucket.
     */
    public boolean childOf(
            long child,
            long parent,
            int childLevel
    ) {
        // parent must be at level childLevel + 1
        return bucketId(child, childLevel + 1) == parent;
    }

    /**
     * Returns true if ancestorBucket contains the child bucket.
     */
    public boolean descendantOf(
            long child,
            long ancestorBucket,
            int ancestorLevel
    ) {
        return bucketId(child, ancestorLevel) == ancestorBucket;
    }

    /**
     * Returns the direct parent bucket of a bucket.
     */
    public long parentOf(long child, int childLevel) {
        return bucketId(child, childLevel + 1);
    }
}
