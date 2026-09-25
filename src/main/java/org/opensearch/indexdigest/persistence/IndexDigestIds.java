/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.persistence;

/**
 * Deterministic IDs for system-index docs.
 */
public final class IndexDigestIds {
    private IndexDigestIds() {}

    public static String progressId(String indexUuid, int shardId, String allocationId) {
        return "p:" + indexUuid + ":" + shardId + ":" + allocationId;
    }

    public static String bucketId(String indexUuid, int shardId, String allocationId, int level, long bucketId) {
        return "b:" + indexUuid + ":" + shardId + ":" + allocationId + ":" + level + ":" + bucketId;
    }

    public static String deleteEventId(String indexUuid, int shardId, String allocationId, long seqNo) {
        return "e:" + indexUuid + ":" + shardId + ":" + allocationId + ":" + seqNo;
    }
}
