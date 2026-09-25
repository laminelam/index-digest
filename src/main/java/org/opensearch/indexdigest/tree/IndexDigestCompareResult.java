/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

import org.opensearch.indexdigest.model.BucketDoc;

import java.util.List;

public final class IndexDigestCompareResult {
    public final boolean comparable;
    public final boolean equal;
    public final boolean truncated;
    public final String reason;
    public final int minLevel;
    public final int rootLevel;
    public final int maxMismatches;
    public final long commonLastSealedLeafBucketId;
    public final long startRootBucket;
    public final long endRootBucket;
    public final Side left;
    public final Side right;
    public final List<Mismatch> mismatches;

    /**
     * Top windows for which an automatic reseal was requested as a result of
     * this comparison. Empty when the trees are equal or auto-reseal declined
     * to (re-)request (pending or recently done).
     */
    public final List<Long> resealRequestedWindows;

    public IndexDigestCompareResult(
            boolean comparable,
            boolean equal,
            boolean truncated,
            String reason,
            int minLevel,
            int rootLevel,
            int maxMismatches,
            long commonLastSealedLeafBucketId,
            long startRootBucket,
            long endRootBucket,
            Side left,
            Side right,
            List<Mismatch> mismatches
    ) {
        this.comparable = comparable;
        this.equal = equal;
        this.truncated = truncated;
        this.reason = reason;
        this.minLevel = minLevel;
        this.rootLevel = rootLevel;
        this.maxMismatches = maxMismatches;
        this.commonLastSealedLeafBucketId = commonLastSealedLeafBucketId;
        this.startRootBucket = startRootBucket;
        this.endRootBucket = endRootBucket;
        this.left = left;
        this.right = right;
        this.mismatches = List.copyOf(mismatches);
        this.resealRequestedWindows = List.of();
    }

    private IndexDigestCompareResult(IndexDigestCompareResult base, List<Long> resealRequestedWindows) {
        this.comparable = base.comparable;
        this.equal = base.equal;
        this.truncated = base.truncated;
        this.reason = base.reason;
        this.minLevel = base.minLevel;
        this.rootLevel = base.rootLevel;
        this.maxMismatches = base.maxMismatches;
        this.commonLastSealedLeafBucketId = base.commonLastSealedLeafBucketId;
        this.startRootBucket = base.startRootBucket;
        this.endRootBucket = base.endRootBucket;
        this.left = base.left;
        this.right = base.right;
        this.mismatches = base.mismatches;
        this.resealRequestedWindows = List.copyOf(resealRequestedWindows);
    }

    public IndexDigestCompareResult withResealRequestedWindows(List<Long> windows) {
        return new IndexDigestCompareResult(this, windows);
    }

    public static IndexDigestCompareResult notComparable(
            String reason,
            int minLevel,
            int rootLevel,
            int maxMismatches,
            long commonLastSealedLeafBucketId,
            Side left,
            Side right
    ) {
        return new IndexDigestCompareResult(
                false,
                false,
                false,
                reason,
                minLevel,
                rootLevel,
                maxMismatches,
                commonLastSealedLeafBucketId,
                0L,
                -1L,
                left,
                right,
                List.of()
        );
    }

    public static final class Side {
        public final String index;
        public final String indexUuid;
        public final int shardId;
        public final String allocationId;
        public final boolean progressFound;
        public final long lastSealedLeafBucketId;
        public final long lastSeenEpochMillis;
        public final int minLevel;
        public final int maxLevel;
        public final int formatVersion;

        public Side(
                String index,
                String indexUuid,
                int shardId,
                String allocationId,
                boolean progressFound,
                long lastSealedLeafBucketId,
                long lastSeenEpochMillis,
                int minLevel,
                int maxLevel,
                int formatVersion
        ) {
            this.index = index;
            this.indexUuid = indexUuid;
            this.shardId = shardId;
            this.allocationId = allocationId;
            this.progressFound = progressFound;
            this.lastSealedLeafBucketId = lastSealedLeafBucketId;
            this.lastSeenEpochMillis = lastSeenEpochMillis;
            this.minLevel = minLevel;
            this.maxLevel = maxLevel;
            this.formatVersion = formatVersion;
        }
    }

    public static final class Bucket {
        public final boolean present;
        public final int level;
        public final long bucketId;
        public final long bucketStartMillis;
        public final long bucketEndMillis;
        public final boolean leaf;
        public final String digest;
        public final long docCount;
        public final long childCount;
        public final long deleteEventCount;
        public final long sealedByMaxExternalVersion;

        public Bucket(
                boolean present,
                int level,
                long bucketId,
                long bucketStartMillis,
                long bucketEndMillis,
                boolean leaf,
                String digest,
                long docCount,
                long childCount,
                long deleteEventCount,
                long sealedByMaxExternalVersion
        ) {
            this.present = present;
            this.level = level;
            this.bucketId = bucketId;
            this.bucketStartMillis = bucketStartMillis;
            this.bucketEndMillis = bucketEndMillis;
            this.leaf = leaf;
            this.digest = digest;
            this.docCount = docCount;
            this.childCount = childCount;
            this.deleteEventCount = deleteEventCount;
            this.sealedByMaxExternalVersion = sealedByMaxExternalVersion;
        }

        public static Bucket fromDoc(BucketDoc doc) {
            if (doc == null) {
                return null;
            }
            return new Bucket(
                    true,
                    doc.level,
                    doc.bucketId,
                    doc.bucketStartMillis,
                    doc.bucketEndMillis,
                    doc.leaf,
                    doc.digest,
                    doc.docCount,
                    doc.childCount,
                    doc.deleteEventCount,
                    doc.sealedByMaxExternalVersion
            );
        }

        public static Bucket missing(
                int level,
                long bucketId,
                long bucketStartMillis,
                long bucketEndMillis,
                boolean leaf
        ) {
            return new Bucket(
                    false,
                    level,
                    bucketId,
                    bucketStartMillis,
                    bucketEndMillis,
                    leaf,
                    null,
                    0L,
                    0L,
                    0L,
                    -1L
            );
        }
    }

    public static final class Mismatch {
        public final int level;
        public final long bucketId;
        public final long bucketStartMillis;
        public final long bucketEndMillis;
        public final boolean leaf;
        public final String reason;
        public final Bucket left;
        public final Bucket right;

        public Mismatch(
                int level,
                long bucketId,
                long bucketStartMillis,
                long bucketEndMillis,
                boolean leaf,
                String reason,
                Bucket left,
                Bucket right
        ) {
            this.level = level;
            this.bucketId = bucketId;
            this.bucketStartMillis = bucketStartMillis;
            this.bucketEndMillis = bucketEndMillis;
            this.leaf = leaf;
            this.reason = reason;
            this.left = left;
            this.right = right;
        }
    }
}
