/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

import org.opensearch.indexdigest.persistence.IndexDigestIds;

import java.util.HashMap;
import java.util.Map;

public final class BucketDoc {
    public static final String TYPE = "bucket";

    public final String id;
    public final String indexUuid;
    public final int shardId;
    public final String allocationId;
    public final String nodeId;
    public final String shardRole;
    public final String scanMode;

    public final int level;
    public final long bucketId;
    public final boolean leaf;

    public final long bucketStartMillis;
    public final long bucketEndMillis;

    public final String digest;

    /**
     * Number of live docs represented by this bucket subtree.
     */
    public final long docCount;

    /**
     * Number of direct children reduced into this bucket.
     * 0 for leaves.
     */
    public final long childCount;

    /**
     * Number of delete events folded into this bucket subtree. Delete events
     * change digests but not docCount, so this is what distinguishes a
     * delete-only leaf from an empty one.
     */
    public final long deleteEventCount;

    /**
     * Max observed external version used to decide sealing for this persisted
     * bucket.
     */
    public final long sealedByMaxExternalVersion;

    public BucketDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            String nodeId,
            String shardRole,
            String scanMode,
            int level,
            long bucketId,
            boolean leaf,
            long bucketStartMillis,
            long bucketEndMillis,
            String digest,
            long docCount,
            long childCount,
            long deleteEventCount,
            long sealedByMaxExternalVersion
    ) {
        this.id = id;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.nodeId = nodeId;
        this.shardRole = shardRole;
        this.scanMode = scanMode;
        this.level = level;
        this.bucketId = bucketId;
        this.leaf = leaf;
        this.bucketStartMillis = bucketStartMillis;
        this.bucketEndMillis = bucketEndMillis;
        this.digest = digest;
        this.docCount = docCount;
        this.childCount = childCount;
        this.deleteEventCount = deleteEventCount;
        this.sealedByMaxExternalVersion = sealedByMaxExternalVersion;
    }

    public BucketDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            int level,
            long bucketId,
            boolean leaf,
            long bucketStartMillis,
            long bucketEndMillis,
            String digest,
            long docCount,
            long childCount,
            long sealedByMaxExternalVersion
    ) {
        this(
                id,
                indexUuid,
                shardId,
                allocationId,
                null,
                null,
                null,
                level,
                bucketId,
                leaf,
                bucketStartMillis,
                bucketEndMillis,
                digest,
                docCount,
                childCount,
                0L,
                sealedByMaxExternalVersion
        );
    }

    public static BucketDoc leaf(
            ShardCopyKey key,
            LeafBucketDigest leafDigest,
            long sealedByMaxExternalVersion
    ) {
        return new BucketDoc(
                IndexDigestIds.bucketId(key.indexUUID, key.shardId, key.allocationId, leafDigest.level, leafDigest.bucketId),
                key.indexUUID,
                key.shardId,
                key.allocationId,
                key.nodeId,
                key.shardRole(),
                key.scanMode,
                leafDigest.level,
                leafDigest.bucketId,
                true,
                leafDigest.bucketStartMillis,
                leafDigest.bucketEndMillis,
                leafDigest.digest,
                leafDigest.liveDocCount,
                0L,
                leafDigest.deleteEventCount,
                sealedByMaxExternalVersion
        );
    }

    public static BucketDoc parent(
            ShardCopyKey key,
            int level,
            long bucketId,
            long bucketStartMillis,
            long bucketEndMillis,
            String digest,
            long docCount,
            long childCount,
            long deleteEventCount,
            long sealedByMaxExternalVersion
    ) {
        return new BucketDoc(
                IndexDigestIds.bucketId(key.indexUUID, key.shardId, key.allocationId, level, bucketId),
                key.indexUUID,
                key.shardId,
                key.allocationId,
                key.nodeId,
                key.shardRole(),
                key.scanMode,
                level,
                bucketId,
                false,
                bucketStartMillis,
                bucketEndMillis,
                digest,
                docCount,
                childCount,
                deleteEventCount,
                sealedByMaxExternalVersion
        );
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
        m.put("level", level);
        m.put("bucket_id", bucketId);
        m.put("leaf", leaf);
        m.put("bucket_start_millis", bucketStartMillis);
        m.put("bucket_end_millis", bucketEndMillis);
        m.put("digest", digest);
        m.put("doc_count", docCount);
        m.put("child_count", childCount);
        m.put("delete_event_count", deleteEventCount);
        m.put("sealed_by_max_external_version", sealedByMaxExternalVersion);
        return m;
    }

    public static BucketDoc fromSource(String id, Map<String, Object> source) {
        return new BucketDoc(
                id,
                asString(source.get("index_uuid")),
                asInt(source.get("shard_id")),
                asString(source.get("allocation_id")),
                asString(source.get("node_id")),
                asString(source.get("shard_role")),
                asString(source.get("scan_mode")),
                asInt(source.get("level")),
                asLong(source.get("bucket_id")),
                asBoolean(source.get("leaf")),
                asLong(source.get("bucket_start_millis")),
                asLong(source.get("bucket_end_millis")),
                asString(source.get("digest")),
                asLong(source.get("doc_count")),
                asLong(source.get("child_count")),
                asLong(source.getOrDefault("delete_event_count", 0L)),
                asLong(source.getOrDefault(
                        "sealed_by_max_external_version",
                        source.getOrDefault("sealed_by_max_version_ts", -1L)
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

    private static boolean asBoolean(Object o) {
        return (Boolean) o;
    }
}
