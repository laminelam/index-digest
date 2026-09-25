/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.persistence.IndexDigestIds;
import org.opensearch.indexdigest.tree.TreeBuilder;
import org.opensearch.indexdigest.tree.TreeRequest;
import org.opensearch.indexdigest.tree.TreeView;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class TreeBuilderTests extends OpenSearchTestCase {

    public void testBuildDepthTwoTree() {
        TreeBuilder builder = new TreeBuilder(BucketMath.INSTANCE);

        TreeRequest request = new TreeRequest(
                "test-index-digest",
                0,
                "alloc-1",
                12,
                0L,
                2,
                true
        );

        TreeView view = builder.build(
                request,
                "index-uuid",
                fullTreeDocs()
        );

        assertEquals("test-index-digest", view.index);
        assertEquals("index-uuid", view.indexUuid);
        assertEquals(0, view.shardId);
        assertEquals("alloc-1", view.allocationId);
        assertEquals(12, view.rootLevel);
        assertEquals(0L, view.rootBucket);
        assertEquals(2, view.depth);
        assertFalse(view.truncated);
        assertNotNull(view.root);

        TreeView.Node root = view.root;
        assertEquals(12, root.level);
        assertEquals(0L, root.bucketId);
        assertEquals(8L, root.docCount);
        assertEquals(2, root.children.size());

        TreeView.Node left = root.children.get(0);
        TreeView.Node right = root.children.get(1);

        assertEquals(11, left.level);
        assertEquals(0L, left.bucketId);
        assertEquals(4L, left.docCount);
        assertEquals(2, left.children.size());

        assertEquals(10, left.children.get(0).level);
        assertEquals(0L, left.children.get(0).bucketId);
        assertEquals(2L, left.children.get(0).docCount);

        assertEquals(10, left.children.get(1).level);
        assertEquals(1024L, left.children.get(1).bucketId);
        assertEquals(2L, left.children.get(1).docCount);

        assertEquals(11, right.level);
        assertEquals(2048L, right.bucketId);
        assertEquals(4L, right.docCount);
        assertEquals(2, right.children.size());

        assertEquals(10, right.children.get(0).level);
        assertEquals(2048L, right.children.get(0).bucketId);

        assertEquals(10, right.children.get(1).level);
        assertEquals(3072L, right.children.get(1).bucketId);
    }

    public void testDepthOnePrunesLeafChildren() {
        TreeBuilder builder = new TreeBuilder(BucketMath.INSTANCE);

        TreeRequest request = new TreeRequest(
                "test-index-digest",
                0,
                "alloc-1",
                12,
                0L,
                1,
                true
        );

        TreeView view = builder.build(
                request,
                "index-uuid",
                fullTreeDocs()
        );

        assertNotNull(view.root);
        assertTrue(view.truncated);

        assertEquals(2, view.root.children.size());
        assertEquals(0, view.root.children.get(0).children.size());
        assertEquals(0, view.root.children.get(1).children.size());
    }

    public void testDepthZeroReturnsOnlyRoot() {
        TreeBuilder builder = new TreeBuilder(BucketMath.INSTANCE);

        TreeRequest request = new TreeRequest(
                "test-index-digest",
                0,
                "alloc-1",
                12,
                0L,
                0,
                true
        );

        TreeView view = builder.build(
                request,
                "index-uuid",
                fullTreeDocs()
        );

        assertNotNull(view.root);
        assertTrue(view.truncated);
        assertEquals(0, view.root.children.size());
    }

    public void testMissingRootReturnsNullRoot() {
        TreeBuilder builder = new TreeBuilder(BucketMath.INSTANCE);

        TreeRequest request = new TreeRequest(
                "test-index-digest",
                0,
                "alloc-1",
                12,
                4096L,
                2,
                true
        );

        TreeView view = builder.build(
                request,
                "index-uuid",
                fullTreeDocs()
        );

        assertNull(view.root);
        assertFalse(view.truncated);
    }

    public void testChildrenAreSortedByBucketId() {
        TreeBuilder builder = new TreeBuilder(BucketMath.INSTANCE);

        TreeRequest request = new TreeRequest(
                "test-index-digest",
                0,
                "alloc-1",
                12,
                0L,
                2,
                true
        );

        List<BucketDoc> shuffled = List.of(
                bucket(10, 3072L, true, 2L, 0L),
                bucket(11, 2048L, false, 4L, 2L),
                bucket(10, 0L, true, 2L, 0L),
                bucket(12, 0L, false, 8L, 2L),
                bucket(10, 2048L, true, 2L, 0L),
                bucket(11, 0L, false, 4L, 2L),
                bucket(10, 1024L, true, 2L, 0L)
        );

        TreeView view = builder.build(request, "index-uuid", shuffled);

        assertNotNull(view.root);
        assertEquals(0L, view.root.children.get(0).bucketId);
        assertEquals(2048L, view.root.children.get(1).bucketId);

        assertEquals(0L, view.root.children.get(0).children.get(0).bucketId);
        assertEquals(1024L, view.root.children.get(0).children.get(1).bucketId);

        assertEquals(2048L, view.root.children.get(1).children.get(0).bucketId);
        assertEquals(3072L, view.root.children.get(1).children.get(1).bucketId);
    }

    public void testTruncatedCanBeForcedWhenFetchWasDepthLimited() {
        TreeBuilder builder = new TreeBuilder(BucketMath.INSTANCE);

        TreeRequest request = new TreeRequest(
                "test-index-digest",
                0,
                "alloc-1",
                12,
                0L,
                2,
                true
        );

        TreeView view = builder.build(request, "index-uuid", fullTreeDocs(), true);

        assertNotNull(view.root);
        assertTrue(view.truncated);
    }

    private static List<BucketDoc> fullTreeDocs() {
        return List.of(
                bucket(12, 0L, false, 8L, 2L),

                bucket(11, 0L, false, 4L, 2L),
                bucket(11, 2048L, false, 4L, 2L),

                bucket(10, 0L, true, 2L, 0L),
                bucket(10, 1024L, true, 2L, 0L),
                bucket(10, 2048L, true, 2L, 0L),
                bucket(10, 3072L, true, 2L, 0L)
        );
    }

    private static BucketDoc bucket(
            int level,
            long bucketId,
            boolean leaf,
            long docCount,
            long childCount
    ) {
        long size = BucketMath.INSTANCE.bucketSize(level);

        return new BucketDoc(
                IndexDigestIds.bucketId("index-uuid", 0, "alloc-1", level, bucketId),
                "index-uuid",
                0,
                "alloc-1",
                level,
                bucketId,
                leaf,
                bucketId,
                bucketId + size,
                "digest-" + level + "-" + bucketId,
                docCount,
                childCount,
                5000L
        );
    }

}
