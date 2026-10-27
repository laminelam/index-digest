/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.model.BucketDoc;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TreeBuilder {

    private final BucketMath bucketMath;

    public TreeBuilder(BucketMath bucketMath) {
        this.bucketMath = bucketMath;
    }

    public TreeView build(
            TreeRequest request,
            String indexUuid,
            List<BucketDoc> docs
    ) {
        return build(request, indexUuid, docs, false);
    }

    public TreeView build(
            TreeRequest request,
            String indexUuid,
            List<BucketDoc> docs,
            boolean truncatedByFetch
    ) {
        Map<NodeKey, TreeView.Node> nodes = new HashMap<>();

        for (BucketDoc doc : docs) {
            nodes.put(new NodeKey(doc.level, doc.bucketId), new TreeView.Node(doc));
        }

        TreeView.Node root = nodes.get(new NodeKey(request.rootLevel, request.rootBucket));
        if (root == null) {
            return new TreeView(
                    request.index,
                    indexUuid,
                    request.shardId,
                    request.allocationId,
                    request.rootLevel,
                    request.rootBucket,
                    request.depth,
                    request.includeDigest,
                    false,
                    null
            );
        }

        for (TreeView.Node node : nodes.values()) {
            if (node.level >= request.rootLevel) {
                continue;
            }

            long parentBucketId = bucketMath.bucketId(node.bucketId, node.level + 1);
            TreeView.Node parent = nodes.get(new NodeKey(node.level + 1, parentBucketId));

            if (parent != null) {
                parent.children.add(node);
            }
        }

        sortRecursively(root);

        boolean truncated = truncatedByFetch || pruneToDepth(root, 0, request.depth);

        return new TreeView(
                request.index,
                indexUuid,
                request.shardId,
                request.allocationId,
                request.rootLevel,
                request.rootBucket,
                request.depth,
                request.includeDigest,
                truncated,
                root
        );
    }

    private void sortRecursively(TreeView.Node node) {
        node.children.sort(
                Comparator.comparingInt((TreeView.Node n) -> n.level).reversed()
                        .thenComparingLong(n -> n.bucketId)
        );

        for (TreeView.Node child : node.children) {
            sortRecursively(child);
        }
    }

    private boolean pruneToDepth(TreeView.Node node, int currentDepth, int maxDepth) {
        boolean truncated = false;

        if (currentDepth >= maxDepth) {
            if (node.children.isEmpty() == false) {
                node.children.clear();
                return true;
            }
            return false;
        }

        for (TreeView.Node child : node.children) {
            truncated |= pruneToDepth(child, currentDepth + 1, maxDepth);
        }

        return truncated;
    }

    private static final class NodeKey {
        final int level;
        final long bucketId;

        NodeKey(int level, long bucketId) {
            this.level = level;
            this.bucketId = bucketId;
        }

        @Override
        public boolean equals(Object o) {
            if ((o instanceof NodeKey) == false) return false;
            NodeKey that = (NodeKey) o;
            return level == that.level && bucketId == that.bucketId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(level, bucketId);
        }
    }
}
