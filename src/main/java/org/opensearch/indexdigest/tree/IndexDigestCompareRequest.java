/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.tree;

public final class IndexDigestCompareRequest {
    public static final int DEFAULT_ROOT_LEVEL = -1;

    public final String leftIndex;
    public final int leftShardId;
    public final String leftAllocationId;
    public final String rightIndex;
    public final int rightShardId;
    public final String rightAllocationId;
    public final int rootLevel;
    public final int maxMismatches;
    public final boolean includeDigest;

    public IndexDigestCompareRequest(
            String leftIndex,
            int leftShardId,
            String leftAllocationId,
            String rightIndex,
            int rightShardId,
            String rightAllocationId,
            int rootLevel,
            int maxMismatches,
            boolean includeDigest
    ) {
        this.leftIndex = leftIndex;
        this.leftShardId = leftShardId;
        this.leftAllocationId = leftAllocationId;
        this.rightIndex = rightIndex;
        this.rightShardId = rightShardId;
        this.rightAllocationId = rightAllocationId;
        this.rootLevel = rootLevel;
        this.maxMismatches = maxMismatches;
        this.includeDigest = includeDigest;
    }
}
