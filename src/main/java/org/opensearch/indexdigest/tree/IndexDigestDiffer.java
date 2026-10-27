/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.BucketRange;
import org.opensearch.indexdigest.model.BucketDoc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

public final class IndexDigestDiffer {
    private final BucketMath bucketMath;
    private final int minLevel;
    private final int maxLevel;

    public IndexDigestDiffer(BucketMath bucketMath, int minLevel, int maxLevel) {
        this.bucketMath = bucketMath;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
    }

    public IndexDigestCompareResult compare(
            IndexDigestCompareResult.Side left,
            IndexDigestCompareResult.Side right,
            int rootLevel,
            int maxMismatches,
            BucketReader leftReader,
            BucketReader rightReader
    ) {
        if (left.progressFound == false || right.progressFound == false) {
            return IndexDigestCompareResult.notComparable(
                    "missing_progress",
                    minLevel,
                    rootLevel,
                    maxMismatches,
                    commonLastSealedLeafBucket(left, right),
                    left,
                    right
            );
        }

        if (hasCompatibleLevels(left) == false || hasCompatibleLevels(right) == false) {
            return IndexDigestCompareResult.notComparable(
                    "incompatible_progress_levels",
                    minLevel,
                    rootLevel,
                    maxMismatches,
                    commonLastSealedLeafBucket(left, right),
                    left,
                    right
            );
        }

        long commonLastSealedLeafBucket = commonLastSealedLeafBucket(left, right);
        if (commonLastSealedLeafBucket < 0L) {
            return IndexDigestCompareResult.notComparable(
                    "no_common_sealed_bucket",
                    minLevel,
                    rootLevel,
                    maxMismatches,
                    commonLastSealedLeafBucket,
                    left,
                    right
            );
        }

        long endRootBucket = lastComparableRootBucket(commonLastSealedLeafBucket, rootLevel);
        if (endRootBucket < 0L) {
            return IndexDigestCompareResult.notComparable(
                    "no_common_complete_root_bucket",
                    minLevel,
                    rootLevel,
                    maxMismatches,
                    commonLastSealedLeafBucket,
                    left,
                    right
            );
        }

        CompareState state = new CompareState(maxMismatches);
        Map<Long, BucketDoc> leftRoots = byBucketId(leftReader.read(rootLevel, 0L, endRootBucket));
        Map<Long, BucketDoc> rightRoots = byBucketId(rightReader.read(rootLevel, 0L, endRootBucket));

        TreeSet<Long> rootBuckets = new TreeSet<>();
        rootBuckets.addAll(leftRoots.keySet());
        rootBuckets.addAll(rightRoots.keySet());

        for (long rootBucket : rootBuckets) {
            BucketDoc leftRoot = leftRoots.get(rootBucket);
            BucketDoc rightRoot = rightRoots.get(rootBucket);
            if (sameBucket(leftRoot, rightRoot)) {
                continue;
            }
            if (state.atLimit()) {
                state.truncated = true;
                break;
            }

            drillDown(rootLevel, rootBucket, leftRoot, rightRoot, leftReader, rightReader, state);
        }

        return new IndexDigestCompareResult(
                true,
                state.mismatches.isEmpty(),
                state.truncated,
                null,
                minLevel,
                rootLevel,
                maxMismatches,
                commonLastSealedLeafBucket,
                0L,
                endRootBucket,
                left,
                right,
                state.mismatches
        );
    }

    private void drillDown(
            int level,
            long bucketId,
            BucketDoc leftDoc,
            BucketDoc rightDoc,
            BucketReader leftReader,
            BucketReader rightReader,
            CompareState state
    ) {
        if (sameBucket(leftDoc, rightDoc)) {
            return;
        }
        if (state.atLimit()) {
            state.truncated = true;
            return;
        }

        if (level <= minLevel) {
            addMismatch(level, bucketId, leftDoc, rightDoc, state);
            return;
        }

        int childLevel = level - 1;
        BucketRange childRange = bucketMath.getBucketBoundaries(bucketId, level, childLevel);
        Map<Long, BucketDoc> leftChildren = byBucketId(leftReader.read(childLevel, childRange.startBucket(), childRange.endBucket()));
        Map<Long, BucketDoc> rightChildren = byBucketId(rightReader.read(childLevel, childRange.startBucket(), childRange.endBucket()));

        TreeSet<Long> childBuckets = new TreeSet<>();
        childBuckets.addAll(leftChildren.keySet());
        childBuckets.addAll(rightChildren.keySet());

        if (childBuckets.isEmpty()) {
            addMismatch(level, bucketId, leftDoc, rightDoc, state);
            return;
        }

        int mismatchCountBeforeChildren = state.mismatches.size();
        for (long childBucket : childBuckets) {
            BucketDoc leftChild = leftChildren.get(childBucket);
            BucketDoc rightChild = rightChildren.get(childBucket);
            if (sameBucket(leftChild, rightChild) == false) {
                if (state.atLimit()) {
                    state.truncated = true;
                    break;
                }
                drillDown(childLevel, childBucket, leftChild, rightChild, leftReader, rightReader, state);
            }
        }

        if (state.mismatches.size() == mismatchCountBeforeChildren && state.atLimit() == false) {
            addMismatch(level, bucketId, leftDoc, rightDoc, state);
        }
    }

    private void addMismatch(
            int level,
            long bucketId,
            BucketDoc leftDoc,
            BucketDoc rightDoc,
            CompareState state
    ) {
        if (state.atLimit()) {
            state.truncated = true;
            return;
        }

        long start = firstPresent(leftDoc, rightDoc) == null
                ? bucketId
                : firstPresent(leftDoc, rightDoc).bucketStartMillis;
        long end = firstPresent(leftDoc, rightDoc) == null
                ? bucketMath.nextBucket(bucketId, level)
                : firstPresent(leftDoc, rightDoc).bucketEndMillis;

        IndexDigestCompareResult.Bucket leftBucket = IndexDigestCompareResult.Bucket.fromDoc(leftDoc);
        IndexDigestCompareResult.Bucket rightBucket = IndexDigestCompareResult.Bucket.fromDoc(rightDoc);
        if (leftBucket == null) {
            leftBucket = IndexDigestCompareResult.Bucket.missing(level, bucketId, start, end, level == minLevel);
        }
        if (rightBucket == null) {
            rightBucket = IndexDigestCompareResult.Bucket.missing(level, bucketId, start, end, level == minLevel);
        }

        state.mismatches.add(new IndexDigestCompareResult.Mismatch(
                level,
                bucketId,
                start,
                end,
                level == minLevel,
                mismatchReason(leftDoc, rightDoc),
                leftBucket,
                rightBucket
        ));
    }

    private long lastComparableRootBucket(long commonLastSealedLeafBucket, int rootLevel) {
        long candidate = bucketMath.bucketId(commonLastSealedLeafBucket, rootLevel);
        BucketRange candidateRange = bucketMath.getBucketBoundaries(candidate, rootLevel, minLevel);

        if (candidateRange.endBucket() > commonLastSealedLeafBucket) {
            candidate = bucketMath.prevBucket(candidate, rootLevel);
        }
        return candidate;
    }

    private static long commonLastSealedLeafBucket(IndexDigestCompareResult.Side left, IndexDigestCompareResult.Side right) {
        return Math.min(left.lastSealedLeafBucketId, right.lastSealedLeafBucketId);
    }

    private boolean hasCompatibleLevels(IndexDigestCompareResult.Side side) {
        return side.minLevel == minLevel && side.maxLevel == maxLevel;
    }

    private static Map<Long, BucketDoc> byBucketId(List<BucketDoc> docs) {
        Map<Long, BucketDoc> out = new TreeMap<>();
        for (BucketDoc doc : docs) {
            out.put(doc.bucketId, doc);
        }
        return out;
    }

    private static boolean sameBucket(BucketDoc left, BucketDoc right) {
        if (left == null || right == null) {
            return left == right;
        }
        return left.level == right.level
                && left.bucketId == right.bucketId
                && left.leaf == right.leaf
                && left.docCount == right.docCount
                && left.childCount == right.childCount
                && left.deleteEventCount == right.deleteEventCount
                && Objects.equals(left.digest, right.digest);
    }

    private static String mismatchReason(BucketDoc left, BucketDoc right) {
        if (left == null) {
            return "missing_left";
        }
        if (right == null) {
            return "missing_right";
        }
        if (Objects.equals(left.digest, right.digest) == false) {
            return "digest_mismatch";
        }
        return "metadata_mismatch";
    }

    private static BucketDoc firstPresent(BucketDoc left, BucketDoc right) {
        return left == null ? right : left;
    }

    @FunctionalInterface
    public interface BucketReader {
        List<BucketDoc> read(int level, long startBucketInclusive, long endBucketInclusive);
    }

    private static final class CompareState {
        final int maxMismatches;
        final List<IndexDigestCompareResult.Mismatch> mismatches = new ArrayList<>();
        boolean truncated;

        private CompareState(int maxMismatches) {
            this.maxMismatches = maxMismatches;
        }

        boolean atLimit() {
            return mismatches.size() >= maxMismatches;
        }
    }
}
