/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

import org.opensearch.indexdigest.bucket.LeafBucketHasher;
import org.opensearch.indexdigest.model.BucketDoc;

import java.util.ArrayList;
import java.util.List;

public final class TreeView {
    public final String index;
    public final String indexUuid;
    public final int shardId;
    public final String allocationId;
    public final int rootLevel;
    public final long rootBucket;
    public final int depth;
    public final boolean includeDigest;
    public final boolean truncated;
    public final Node root;

    public TreeView(
            String index,
            String indexUuid,
            int shardId,
            String allocationId,
            int rootLevel,
            long rootBucket,
            int depth,
            boolean includeDigest,
            boolean truncated,
            Node root
    ) {
        this.index = index;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.rootLevel = rootLevel;
        this.rootBucket = rootBucket;
        this.depth = depth;
        this.includeDigest = includeDigest;
        this.truncated = truncated;
        this.root = root;
    }

    public static final class Node {
        public final int level;
        public final long bucketId;
        public final long bucketStartMillis;
        public final long bucketEndMillis;
        public final boolean leaf;
        public final String digestAlgo;
        public final String digest;
        public final long docCount;
        public final long childCount;
        public final long deleteEventCount;
        public final long sealedByMaxExternalVersion;
        public final List<Node> children = new ArrayList<>();

        public Node(BucketDoc doc) {
            this.level = doc.level;
            this.bucketId = doc.bucketId;
            this.bucketStartMillis = doc.bucketStartMillis;
            this.bucketEndMillis = doc.bucketEndMillis;
            this.leaf = doc.leaf;
            this.digestAlgo = LeafBucketHasher.digestAlgo();
            this.digest = doc.digest;
            this.docCount = doc.docCount;
            this.childCount = doc.childCount;
            this.deleteEventCount = doc.deleteEventCount;
            this.sealedByMaxExternalVersion = doc.sealedByMaxExternalVersion;
        }
    }
}
