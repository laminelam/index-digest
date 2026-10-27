/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.persistence.IndexDigestIds;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class IndexDigestDifferTests extends OpenSearchTestCase {

    public void testEqualRootBucketsDoNotDrillDown() {
        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);
        InMemoryReader leftReader = new InMemoryReader(
                bucket("left-uuid", "left-alloc", 12, 0L, false, "root", 3L, 2L)
        );
        InMemoryReader rightReader = new InMemoryReader(
                bucket("right-uuid", "right-alloc", 12, 0L, false, "root", 3L, 2L)
        );

        IndexDigestCompareResult result = differ.compare(
                side("left", "left-uuid", "left-alloc", 7168L),
                side("right", "right-uuid", "right-alloc", 7168L),
                12,
                100,
                leftReader,
                rightReader
        );

        assertTrue(result.comparable);
        assertTrue(result.equal);
        assertFalse(result.truncated);
        assertEquals(0, result.mismatches.size());
    }

    public void testDigestMismatchDrillsDownToLeafBucket() {
        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);
        InMemoryReader leftReader = new InMemoryReader(
                bucket("left-uuid", "left-alloc", 12, 0L, false, "left-root", 4L, 2L),
                bucket("left-uuid", "left-alloc", 11, 0L, false, "same-left-child", 2L, 2L),
                bucket("left-uuid", "left-alloc", 11, 2048L, false, "left-child", 2L, 2L),
                bucket("left-uuid", "left-alloc", 10, 0L, true, "same-leaf-0", 1L, 0L),
                bucket("left-uuid", "left-alloc", 10, 1024L, true, "same-leaf-1", 1L, 0L),
                bucket("left-uuid", "left-alloc", 10, 2048L, true, "same-leaf-2", 1L, 0L),
                bucket("left-uuid", "left-alloc", 10, 3072L, true, "left-leaf", 1L, 0L)
        );
        InMemoryReader rightReader = new InMemoryReader(
                bucket("right-uuid", "right-alloc", 12, 0L, false, "right-root", 4L, 2L),
                bucket("right-uuid", "right-alloc", 11, 0L, false, "same-left-child", 2L, 2L),
                bucket("right-uuid", "right-alloc", 11, 2048L, false, "right-child", 2L, 2L),
                bucket("right-uuid", "right-alloc", 10, 0L, true, "same-leaf-0", 1L, 0L),
                bucket("right-uuid", "right-alloc", 10, 1024L, true, "same-leaf-1", 1L, 0L),
                bucket("right-uuid", "right-alloc", 10, 2048L, true, "same-leaf-2", 1L, 0L),
                bucket("right-uuid", "right-alloc", 10, 3072L, true, "right-leaf", 1L, 0L)
        );

        IndexDigestCompareResult result = differ.compare(
                side("left", "left-uuid", "left-alloc", 7168L),
                side("right", "right-uuid", "right-alloc", 7168L),
                12,
                100,
                leftReader,
                rightReader
        );

        assertTrue(result.comparable);
        assertFalse(result.equal);
        assertEquals(1, result.mismatches.size());

        IndexDigestCompareResult.Mismatch mismatch = result.mismatches.get(0);
        assertEquals(10, mismatch.level);
        assertEquals(3072L, mismatch.bucketId);
        assertTrue(mismatch.leaf);
        assertEquals("digest_mismatch", mismatch.reason);
        assertEquals("left-leaf", mismatch.left.digest);
        assertEquals("right-leaf", mismatch.right.digest);
    }

    public void testMissingLeafIsReportedAtLeafLevel() {
        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);
        InMemoryReader leftReader = new InMemoryReader(
                bucket("left-uuid", "left-alloc", 12, 0L, false, "left-root", 2L, 1L),
                bucket("left-uuid", "left-alloc", 11, 2048L, false, "left-child", 2L, 1L),
                bucket("left-uuid", "left-alloc", 10, 3072L, true, "left-leaf", 2L, 0L)
        );
        InMemoryReader rightReader = new InMemoryReader(
                bucket("right-uuid", "right-alloc", 12, 0L, false, "right-root", 0L, 0L)
        );

        IndexDigestCompareResult result = differ.compare(
                side("left", "left-uuid", "left-alloc", 7168L),
                side("right", "right-uuid", "right-alloc", 7168L),
                12,
                100,
                leftReader,
                rightReader
        );

        assertFalse(result.equal);
        assertEquals(1, result.mismatches.size());

        IndexDigestCompareResult.Mismatch mismatch = result.mismatches.get(0);
        assertEquals(10, mismatch.level);
        assertEquals(3072L, mismatch.bucketId);
        assertEquals("missing_right", mismatch.reason);
        assertTrue(mismatch.left.present);
        assertFalse(mismatch.right.present);
    }

    public void testMissingProgressIsNotComparable() {
        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);

        IndexDigestCompareResult result = differ.compare(
                new IndexDigestCompareResult.Side("left", "left-uuid", 0, "left-alloc", false, -1L, 0L, -1, -1, -1),
                side("right", "right-uuid", "right-alloc", 7168L),
                12,
                100,
                new InMemoryReader(),
                new InMemoryReader()
        );

        assertFalse(result.comparable);
        assertFalse(result.equal);
        assertEquals("missing_progress", result.reason);
        assertEquals(123L, result.right.lastSeenEpochMillis);
    }

    public void testIncompatibleProgressLevelsAreNotComparable() {
        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);

        IndexDigestCompareResult result = differ.compare(
                new IndexDigestCompareResult.Side("left", "left-uuid", 0, "left-alloc", true, 7168L, 123L, 9, 12, 2),
                side("right", "right-uuid", "right-alloc", 7168L),
                12,
                100,
                new InMemoryReader(),
                new InMemoryReader()
        );

        assertFalse(result.comparable);
        assertFalse(result.equal);
        assertEquals("incompatible_progress_levels", result.reason);
    }

    public void testMismatchCapTruncatesOnlyWhenMoreMismatchesExist() {
        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);
        InMemoryReader leftReader = new InMemoryReader(
                bucket("left-uuid", "left-alloc", 12, 0L, false, "left-root-0", 1L, 0L),
                bucket("left-uuid", "left-alloc", 12, 4096L, false, "left-root-1", 1L, 0L)
        );
        InMemoryReader rightReader = new InMemoryReader(
                bucket("right-uuid", "right-alloc", 12, 0L, false, "right-root-0", 1L, 0L),
                bucket("right-uuid", "right-alloc", 12, 4096L, false, "right-root-1", 1L, 0L)
        );

        IndexDigestCompareResult result = differ.compare(
                side("left", "left-uuid", "left-alloc", 7168L),
                side("right", "right-uuid", "right-alloc", 7168L),
                12,
                1,
                leftReader,
                rightReader
        );

        assertFalse(result.equal);
        assertTrue(result.truncated);
        assertEquals(1, result.mismatches.size());
    }

    private static IndexDigestCompareResult.Side side(
            String index,
            String indexUuid,
            String allocationId,
            long lastSealedLeafBucketId
    ) {
        return new IndexDigestCompareResult.Side(index, indexUuid, 0, allocationId, true, lastSealedLeafBucketId, 123L, 10, 12, 2);
    }

    public void testEqualDigestsWithDifferentDeleteEventCountsAreMetadataMismatch() {
        BucketDoc left = bucketWithDeleteEvents("left-uuid", "left-alloc", 10, 1024L, "aa", 3L, 0L);
        BucketDoc right = bucketWithDeleteEvents("right-uuid", "right-alloc", 10, 1024L, "aa", 3L, 1L);

        IndexDigestDiffer differ = new IndexDigestDiffer(BucketMath.INSTANCE, 10, 12);
        IndexDigestCompareResult result = differ.compare(
                side("left", "left-uuid", "left-alloc", 7168L),
                side("right", "right-uuid", "right-alloc", 7168L),
                10,
                16,
                new InMemoryReader(left),
                new InMemoryReader(right)
        );

        assertTrue(result.comparable);
        assertFalse(result.equal);
        assertEquals(1, result.mismatches.size());
        assertEquals("metadata_mismatch", result.mismatches.get(0).reason);
        assertEquals(1024L, result.mismatches.get(0).bucketId);
    }

    private static BucketDoc bucketWithDeleteEvents(
            String indexUuid,
            String allocationId,
            int level,
            long bucketId,
            String digest,
            long docCount,
            long deleteEventCount
    ) {
        long end = BucketMath.INSTANCE.nextBucket(bucketId, level);
        return new BucketDoc(
                org.opensearch.indexdigest.persistence.IndexDigestIds.bucketId(indexUuid, 0, allocationId, level, bucketId),
                indexUuid,
                0,
                allocationId,
                null,
                null,
                null,
                level,
                bucketId,
                true,
                bucketId,
                end,
                digest,
                docCount,
                0L,
                deleteEventCount,
                9000L
        );
    }

    private static BucketDoc bucket(
            String indexUuid,
            String allocationId,
            int level,
            long bucketId,
            boolean leaf,
            String digest,
            long docCount,
            long childCount
    ) {
        long end = BucketMath.INSTANCE.nextBucket(bucketId, level);
        return new BucketDoc(
                IndexDigestIds.bucketId(indexUuid, 0, allocationId, level, bucketId),
                indexUuid,
                0,
                allocationId,
                level,
                bucketId,
                leaf,
                bucketId,
                end,
                digest,
                docCount,
                childCount,
                9000L
        );
    }

    private static final class InMemoryReader implements IndexDigestDiffer.BucketReader {
        private final List<BucketDoc> docs;

        private InMemoryReader(BucketDoc... docs) {
            this.docs = List.of(docs);
        }

        @Override
        public List<BucketDoc> read(int level, long startBucketInclusive, long endBucketInclusive) {
            List<BucketDoc> out = new ArrayList<>();
            for (BucketDoc doc : docs) {
                if (doc.level == level
                        && doc.bucketId >= startBucketInclusive
                        && doc.bucketId <= endBucketInclusive) {
                    out.add(doc);
                }
            }
            out.sort(Comparator.comparingLong(d -> d.bucketId));
            return out;
        }
    }
}
