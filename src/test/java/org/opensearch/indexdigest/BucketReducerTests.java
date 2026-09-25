/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.BucketReducer;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.indexdigest.persistence.IndexDigestIds;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;

public class BucketReducerTests extends OpenSearchTestCase {

    public void testReduceAggregatesCounts() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc child0 = leaf(key, 10, 0L, hex("a"), 2L);
        BucketDoc child1 = leaf(key, 10, 1024L, hex("b"), 3L);

        BucketDoc parent = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, child1),
                5000L
        );

        assertEquals(IndexDigestIds.bucketId("index-uuid", 0, "alloc-1", 11, 0L), parent.id);
        assertEquals("index-uuid", parent.indexUuid);
        assertEquals(0, parent.shardId);
        assertEquals("alloc-1", parent.allocationId);

        assertEquals(11, parent.level);
        assertEquals(0L, parent.bucketId);
        assertFalse(parent.leaf);

        assertNotNull(parent.digest);
        assertFalse(parent.digest.isBlank());

        assertEquals(5L, parent.docCount);
        assertEquals(2L, parent.childCount);
        assertEquals(5000L, parent.sealedByMaxExternalVersion);

        assertEquals(0L, parent.bucketStartMillis);
        assertEquals(2048L, parent.bucketEndMillis);
        assertEquals("235db80e6991411aa35e4a8ce0783d4c9fc5191e1ab108c8f2a58de879268c9e", parent.digest);
    }

    public void testReduceDigestIsIndependentOfInputOrder() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc child0 = leaf(key, 10, 0L, hex("a"), 2L);
        BucketDoc child1 = leaf(key, 10, 1024L, hex("b"), 3L);

        BucketDoc parentForward = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, child1),
                5000L
        );

        BucketDoc parentReverse = reducer.reduce(
                key,
                11,
                0L,
                List.of(child1, child0),
                5000L
        );

        assertEquals(parentForward.digest, parentReverse.digest);
        assertEquals(parentForward.docCount, parentReverse.docCount);
        assertEquals(parentForward.childCount, parentReverse.childCount);
    }

    public void testReduceDigestChangesWhenChildDigestChanges() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc child0 = leaf(key, 10, 0L, hex("a"), 2L);
        BucketDoc child1 = leaf(key, 10, 1024L, hex("b"), 3L);
        BucketDoc changedChild1 = leaf(key, 10, 1024L, hex("c"), 3L);

        BucketDoc parentOriginal = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, child1),
                5000L
        );

        BucketDoc parentChanged = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, changedChild1),
                5000L
        );

        assertNotEquals(parentOriginal.digest, parentChanged.digest);
    }

    public void testReduceDigestChangesWhenChildDocCountChanges() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc child0 = leaf(key, 10, 0L, hex("a"), 2L);
        BucketDoc child1 = leaf(key, 10, 1024L, hex("b"), 3L);
        BucketDoc changedChild1 = leaf(key, 10, 1024L, hex("b"), 4L);

        BucketDoc parentOriginal = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, child1),
                5000L
        );

        BucketDoc parentChanged = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, changedChild1),
                5000L
        );

        assertNotEquals(parentOriginal.digest, parentChanged.digest);
        assertEquals(5L, parentOriginal.docCount);
        assertEquals(6L, parentChanged.docCount);
    }

    public void testReduceDigestChangesWhenChildBucketIdChanges() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc child0 = leaf(key, 10, 0L, hex("a"), 2L);
        BucketDoc child1 = leaf(key, 10, 1024L, hex("b"), 3L);
        BucketDoc movedChild1 = leaf(key, 10, 2048L, hex("b"), 3L);

        BucketDoc parentOriginal = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, child1),
                5000L
        );

        BucketDoc parentChanged = reducer.reduce(
                key,
                11,
                0L,
                List.of(child0, movedChild1),
                5000L
        );

        assertNotEquals(parentOriginal.digest, parentChanged.digest);
    }

    public void testRejectEmptyChildren() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> reducer.reduce(key, 11, 0L, List.of(), 5000L)
        );

        assertTrue(e.getMessage().contains("children must not be empty"));
    }

    public void testRejectInvalidHexDigest() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc bad = leaf(key, 10, 0L, "invalid", 1L);

        expectThrows(
                IllegalArgumentException.class,
                () -> reducer.reduce(key, 11, 0L, List.of(bad), 5000L)
        );
    }

    public void testReduceSumsDeleteEventCounts() {
        BucketReducer reducer = new BucketReducer(BucketMath.INSTANCE);
        ShardCopyKey key = new ShardCopyKey("index-uuid", 0, "alloc-1");

        BucketDoc child0 = leafWithDeleteEvents(key, 10, 0L, hex("a"), 2L, 2L);
        BucketDoc child1 = leafWithDeleteEvents(key, 10, 1024L, hex("b"), 3L, 3L);

        BucketDoc parent = reducer.reduce(key, 11, 0L, java.util.List.of(child0, child1), 5000L);

        assertEquals(5L, parent.docCount);
        assertEquals(5L, parent.deleteEventCount);
    }

    private static BucketDoc leafWithDeleteEvents(
            ShardCopyKey key,
            int level,
            long bucketId,
            String digest,
            long docCount,
            long deleteEventCount
    ) {
        long size = BucketMath.INSTANCE.bucketSize(level);
        return new BucketDoc(
                IndexDigestIds.bucketId(key.indexUUID, key.shardId, key.allocationId, level, bucketId),
                key.indexUUID,
                key.shardId,
                key.allocationId,
                null,
                null,
                null,
                level,
                bucketId,
                true,
                bucketId,
                bucketId + size,
                digest,
                docCount,
                0L,
                deleteEventCount,
                5000L
        );
    }

    private static BucketDoc leaf(
            ShardCopyKey key,
            int level,
            long bucketId,
            String digest,
            long docCount
    ) {
        long size = BucketMath.INSTANCE.bucketSize(level);

        return new BucketDoc(
                IndexDigestIds.bucketId(key.indexUUID, key.shardId, key.allocationId, level, bucketId),
                key.indexUUID,
                key.shardId,
                key.allocationId,
                level,
                bucketId,
                true,
                bucketId,
                bucketId + size,
                digest,
                docCount,
                0L,
                5000L
        );
    }

    private static String hex(String seed) {
        byte[] bytes = seed.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();

        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }

        while (sb.length() < 64) {
            sb.append('0');
        }

        return sb.substring(0, 64);
    }
}
