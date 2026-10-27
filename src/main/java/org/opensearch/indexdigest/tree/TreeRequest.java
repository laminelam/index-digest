/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

public final class TreeRequest {
    public final String index;
    public final int shardId;
    public final String allocationId;
    public final int rootLevel;
    public final long rootBucket;
    public final int depth;
    public final boolean includeDigest;

    public TreeRequest(
            String index,
            int shardId,
            String allocationId,
            int rootLevel,
            long rootBucket,
            int depth,
            boolean includeDigest
    ) {
        this.index = index;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.rootLevel = rootLevel;
        this.rootBucket = rootBucket;
        this.depth = depth;
        this.includeDigest = includeDigest;
    }
}
