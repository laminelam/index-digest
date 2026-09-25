/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

import org.opensearch.indexdigest.persistence.IndexDigestIds;

import java.util.HashMap;
import java.util.Map;

/**
 * Progress document stored in .index_digest.
 *
 * Tracks two independent watermarks per shard copy:
 * - lastSealedLeafBucketId: how far the external-version axis has been sealed
 * - lastProcessedSeqNo: how far the delete-tombstone history has been harvested
 */
public final class ProgressDoc {
    public static final String TYPE = "progress";

    public final String id;
    public final String indexUuid;
    public final int shardId;
    public final String allocationId;
    public final String nodeId;
    public final String shardRole;
    public final String scanMode;
    public final int minLevel;
    public final int maxLevel;
    public final long lastSeenEpochMillis;

    /**
     * Start of the most recently sealed/persisted leaf bucket.
     * -1 means none yet.
     */
    public final long lastSealedLeafBucketId;

    /**
     * Highest _seq_no whose delete tombstone has been harvested and durably
     * persisted as a delete-event doc. -1 means none yet. The retention lease
     * is advanced to this value + 1, never past it.
     */
    public final long lastProcessedSeqNo;

    /**
     * Digest/tree format generation this copy's derived data was built with.
     * -1 for legacy docs; a mismatch with the running build self-heals via
     * reset + rebuild.
     */
    public final int formatVersion;

    /**
     * Highest external version observed at the last successful harvest. A
     * delete issued while the scanner was down necessarily carries a newer
     * external version, so a history gap can be repaired by re-sealing only
     * from here forward instead of rebuilding all of history.
     * -1 when unknown.
     */
    public final long lastHarvestMaxExternalVersion;

    public ProgressDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            String nodeId,
            String shardRole,
            String scanMode,
            int minLevel,
            int maxLevel,
            long lastSeenEpochMillis,
            long lastSealedLeafBucketId,
            long lastProcessedSeqNo,
            int formatVersion,
            long lastHarvestMaxExternalVersion
    ) {
        this.id = id;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.nodeId = nodeId;
        this.shardRole = shardRole;
        this.scanMode = scanMode;
        this.minLevel = minLevel;
        this.maxLevel = maxLevel;
        this.lastSeenEpochMillis = lastSeenEpochMillis;
        this.lastSealedLeafBucketId = lastSealedLeafBucketId;
        this.lastProcessedSeqNo = lastProcessedSeqNo;
        this.formatVersion = formatVersion;
        this.lastHarvestMaxExternalVersion = lastHarvestMaxExternalVersion;
    }

    public ProgressDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            String nodeId,
            String shardRole,
            String scanMode,
            int minLevel,
            int maxLevel,
            long lastSeenEpochMillis,
            long lastSealedLeafBucketId
    ) {
        this(
                id,
                indexUuid,
                shardId,
                allocationId,
                nodeId,
                shardRole,
                scanMode,
                minLevel,
                maxLevel,
                lastSeenEpochMillis,
                lastSealedLeafBucketId,
                -1L,
                -1,
                -1L
        );
    }

    public ProgressDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            long lastSeenEpochMillis,
            long lastSealedLeafBucketId
    ) {
        this(
                id,
                indexUuid,
                shardId,
                allocationId,
                null,
                null,
                null,
                -1,
                -1,
                lastSeenEpochMillis,
                lastSealedLeafBucketId,
                -1L,
                -1,
                -1L
        );
    }

    public static ProgressDoc initialForShardCopy(
            String indexUuid,
            int shardId,
            String allocationId,
            long nowMillis
    ) {
        String id = IndexDigestIds.progressId(indexUuid, shardId, allocationId);
        return new ProgressDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                null,
                null,
                null,
                -1,
                -1,
                nowMillis,
                -1L,
                -1L,
                -1,
                -1L
        );
    }

    public static ProgressDoc initialForShardCopy(
            ShardCopyKey key,
            long nowMillis,
            int minLevel,
            int maxLevel
    ) {
        return initialForShardCopy(key, nowMillis, minLevel, maxLevel, -1);
    }

    public static ProgressDoc initialForShardCopy(
            ShardCopyKey key,
            long nowMillis,
            int minLevel,
            int maxLevel,
            int formatVersion
    ) {
        String id = IndexDigestIds.progressId(key.indexUUID, key.shardId, key.allocationId);
        return new ProgressDoc(
                id,
                key.indexUUID,
                key.shardId,
                key.allocationId,
                key.nodeId,
                key.shardRole(),
                key.scanMode,
                minLevel,
                maxLevel,
                nowMillis,
                -1L,
                -1L,
                formatVersion,
                -1L
        );
    }

    public static ProgressDoc initialForShardCopy(ShardCopyKey key, long nowMillis) {
        return initialForShardCopy(key, nowMillis, -1, -1);
    }

    public ProgressDoc withUpdatedHeartbeat(long nowMillis) {
        return new ProgressDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                nodeId,
                shardRole,
                scanMode,
                minLevel,
                maxLevel,
                nowMillis,
                lastSealedLeafBucketId,
                lastProcessedSeqNo,
                formatVersion,
                lastHarvestMaxExternalVersion
        );
    }

    public ProgressDoc withUpdatedHeartbeat(ShardCopyKey key, long nowMillis) {
        return new ProgressDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                key.nodeId,
                key.shardRole(),
                key.scanMode,
                minLevel,
                maxLevel,
                nowMillis,
                lastSealedLeafBucketId,
                lastProcessedSeqNo,
                formatVersion,
                lastHarvestMaxExternalVersion
        );
    }

    public ProgressDoc withProcessedSeqNo(ShardCopyKey key, long nowMillis, long processedSeqNo, long harvestMaxExternalVersion) {
        return new ProgressDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                key.nodeId,
                key.shardRole(),
                key.scanMode,
                minLevel,
                maxLevel,
                nowMillis,
                lastSealedLeafBucketId,
                processedSeqNo,
                formatVersion,
                harvestMaxExternalVersion
        );
    }

    public ProgressDoc advance(long nowMillis, long sealedLeafBucketId) {
        return new ProgressDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                nodeId,
                shardRole,
                scanMode,
                minLevel,
                maxLevel,
                nowMillis,
                sealedLeafBucketId,
                lastProcessedSeqNo,
                formatVersion,
                lastHarvestMaxExternalVersion
        );
    }

    public ProgressDoc advance(ShardCopyKey key, long nowMillis, long sealedLeafBucketId) {
        return new ProgressDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                key.nodeId,
                key.shardRole(),
                key.scanMode,
                minLevel,
                maxLevel,
                nowMillis,
                sealedLeafBucketId,
                lastProcessedSeqNo,
                formatVersion,
                lastHarvestMaxExternalVersion
        );
    }

    /**
     * Rewinds the sealed watermark so the windows covering a damaged version
     * range are re-sealed on subsequent ticks. Everything below stays sealed.
     */
    public ProgressDoc rewindSealedWatermark(ShardCopyKey key, long nowMillis, long sealedLeafBucketId) {
        return advance(key, nowMillis, sealedLeafBucketId);
    }

    public Map<String, Object> toSource() {
        Map<String, Object> m = new HashMap<>();
        m.put("type", TYPE);
        m.put("index_uuid", indexUuid);
        m.put("shard_id", shardId);
        m.put("allocation_id", allocationId);
        m.put("node_id", nodeId);
        m.put("shard_role", shardRole);
        m.put("scan_mode", scanMode);
        m.put("min_level", minLevel);
        m.put("max_level", maxLevel);
        m.put("last_seen_epoch_millis", lastSeenEpochMillis);
        m.put("last_sealed_leaf_bucket_id", lastSealedLeafBucketId);
        m.put("last_processed_seq_no", lastProcessedSeqNo);
        m.put("format_version", formatVersion);
        m.put("last_harvest_max_external_version", lastHarvestMaxExternalVersion);
        return m;
    }

    public static ProgressDoc fromSource(String id, Map<String, Object> source) {
        return new ProgressDoc(
                id,
                asString(source.get("index_uuid")),
                asInt(source.get("shard_id")),
                asString(source.get("allocation_id")),
                asString(source.get("node_id")),
                asString(source.get("shard_role")),
                asString(source.get("scan_mode")),
                asInt(source.getOrDefault("min_level", -1)),
                asInt(source.getOrDefault("max_level", -1)),
                asLong(source.get("last_seen_epoch_millis")),
                asLong(source.getOrDefault("last_sealed_leaf_bucket_id", -1L)),
                asLong(source.getOrDefault("last_processed_seq_no", -1L)),
                asInt(source.getOrDefault("format_version", -1)),
                asLong(source.getOrDefault(
                        "last_harvest_max_external_version",
                        source.getOrDefault("last_harvest_frontier", -1L)
                ))
        );
    }

    private static String asString(Object o) {
        return o == null ? null : o.toString();
    }

    private static int asInt(Object o) {
        return ((Number) o).intValue();
    }

    private static long asLong(Object o) {
        return ((Number) o).longValue();
    }

}
