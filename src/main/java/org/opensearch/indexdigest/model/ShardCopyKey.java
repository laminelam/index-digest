/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

import org.opensearch.indexdigest.scan.ScanMode;

import java.util.Objects;

/**
 * Stable identity for a shard copy (primary or replica): (indexUUID, shardId, allocationId)
 */


public final class ShardCopyKey {
    public final String indexUUID;
    public final int shardId;
    public final String allocationId;
    public final boolean primary;
    public final String nodeId;
    public final String scanMode;

    public ShardCopyKey(String indexUUID, int shardId, String allocationId) {
        this(indexUUID, shardId, allocationId, false, null, (String) null);
    }

    public ShardCopyKey(
            String indexUUID,
            int shardId,
            String allocationId,
            boolean primary,
            String nodeId,
            ScanMode scanMode
    ) {
        this(indexUUID, shardId, allocationId, primary, nodeId, scanMode == null ? null : scanMode.settingValue());
    }

    public ShardCopyKey(
            String indexUUID,
            int shardId,
            String allocationId,
            boolean primary,
            String nodeId,
            String scanMode
    ) {
        this.indexUUID = Objects.requireNonNull(indexUUID);
        this.shardId = shardId;
        this.allocationId = Objects.requireNonNull(allocationId);
        this.primary = primary;
        this.nodeId = nodeId;
        this.scanMode = scanMode;
    }

    public String shardRole() {
        return primary ? "primary" : "replica";
    }

    public String identityString() {
        return indexUUID + ":" + shardId + ":" + allocationId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof ShardCopyKey) == false) return false;
        ShardCopyKey that = (ShardCopyKey) o;
        return shardId == that.shardId
                && indexUUID.equals(that.indexUUID)
                && allocationId.equals(that.allocationId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexUUID, shardId, allocationId);
    }

    @Override
    public String toString() {
        return identityString();
    }
}
