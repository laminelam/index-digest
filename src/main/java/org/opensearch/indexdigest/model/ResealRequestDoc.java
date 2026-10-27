/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

import java.util.HashMap;
import java.util.Map;

/**
 * A request to recompute one sealed top window of one shard copy from current
 * live data plus retained delete events. Created by the compare path when it
 * finds a mismatch below both watermarks; executed by the owning node's
 * scanner on its next tick. Timing artifacts (seal-race shadows) dissolve on
 * reseal; real divergence survives it, and the done-marker's cooldown prevents
 * re-requesting confirmed-real mismatches in a loop.
 */
public final class ResealRequestDoc {
    public static final String TYPE = "reseal_request";
    public static final String STATUS_PENDING = "pending";
    public static final String STATUS_DONE = "done";

    public final String id;
    public final String indexUuid;
    public final int shardId;
    public final String allocationId;
    public final long windowBucketId;
    public final String status;
    public final long requestedAtEpochMillis;
    public final long resealedAtEpochMillis;

    public ResealRequestDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            long windowBucketId,
            String status,
            long requestedAtEpochMillis,
            long resealedAtEpochMillis
    ) {
        this.id = id;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.windowBucketId = windowBucketId;
        this.status = status;
        this.requestedAtEpochMillis = requestedAtEpochMillis;
        this.resealedAtEpochMillis = resealedAtEpochMillis;
    }

    public static String docId(String indexUuid, int shardId, String allocationId, long windowBucketId) {
        return "r:" + indexUuid + ":" + shardId + ":" + allocationId + ":" + windowBucketId;
    }

    public static ResealRequestDoc pending(
            String indexUuid,
            int shardId,
            String allocationId,
            long windowBucketId,
            long nowMillis
    ) {
        return new ResealRequestDoc(
                docId(indexUuid, shardId, allocationId, windowBucketId),
                indexUuid,
                shardId,
                allocationId,
                windowBucketId,
                STATUS_PENDING,
                nowMillis,
                -1L
        );
    }

    public ResealRequestDoc done(long nowMillis) {
        return new ResealRequestDoc(
                id,
                indexUuid,
                shardId,
                allocationId,
                windowBucketId,
                STATUS_DONE,
                requestedAtEpochMillis,
                nowMillis
        );
    }

    public Map<String, Object> toSource() {
        Map<String, Object> m = new HashMap<>();
        m.put("type", TYPE);
        m.put("index_uuid", indexUuid);
        m.put("shard_id", shardId);
        m.put("allocation_id", allocationId);
        m.put("bucket_id", windowBucketId);
        m.put("status", status);
        m.put("requested_at_epoch_millis", requestedAtEpochMillis);
        m.put("resealed_at_epoch_millis", resealedAtEpochMillis);
        return m;
    }

    public static ResealRequestDoc fromSource(String id, Map<String, Object> source) {
        return new ResealRequestDoc(
                id,
                asString(source.get("index_uuid")),
                ((Number) source.get("shard_id")).intValue(),
                asString(source.get("allocation_id")),
                ((Number) source.get("bucket_id")).longValue(),
                asString(source.get("status")),
                ((Number) source.getOrDefault("requested_at_epoch_millis", -1L)).longValue(),
                ((Number) source.getOrDefault("resealed_at_epoch_millis", -1L)).longValue()
        );
    }

    private static String asString(Object o) {
        return o == null ? null : o.toString();
    }
}
