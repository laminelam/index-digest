/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

import org.opensearch.indexdigest.persistence.IndexDigestIds;

import java.util.HashMap;
import java.util.Map;

/**
 * A harvested delete tombstone, persisted in .index_digest. Keyed by (shard copy,
 * seq_no) so re-harvesting after a crash is idempotent. Retained PERMANENTLY
 * after folding: the Lucene tombstone may be merged away at any time, so these
 * docs are the durable operation history that makes rebuilds and reseals
 * lossless within an allocation's life. Only a full reset of the system index
 * (or allocation change) discards them.
 */
public final class DeleteEventDoc {
    public static final String TYPE = "delete_event";

    public final String id;
    public final String indexUuid;
    public final int shardId;
    public final String allocationId;
    public final long seqNo;
    public final String eventDocId;
    public final long eventExternalVersion;

    public DeleteEventDoc(
            String id,
            String indexUuid,
            int shardId,
            String allocationId,
            long seqNo,
            String eventDocId,
            long eventExternalVersion
    ) {
        this.id = id;
        this.indexUuid = indexUuid;
        this.shardId = shardId;
        this.allocationId = allocationId;
        this.seqNo = seqNo;
        this.eventDocId = eventDocId;
        this.eventExternalVersion = eventExternalVersion;
    }

    public static DeleteEventDoc create(
            ShardCopyKey key,
            long seqNo,
            String eventDocId,
            long eventExternalVersion
    ) {
        return new DeleteEventDoc(
                IndexDigestIds.deleteEventId(key.indexUUID, key.shardId, key.allocationId, seqNo),
                key.indexUUID,
                key.shardId,
                key.allocationId,
                seqNo,
                eventDocId,
                eventExternalVersion
        );
    }

    public Map<String, Object> toSource() {
        Map<String, Object> m = new HashMap<>();
        m.put("type", TYPE);
        m.put("index_uuid", indexUuid);
        m.put("shard_id", shardId);
        m.put("allocation_id", allocationId);
        m.put("seq_no", seqNo);
        m.put("event_doc_id", eventDocId);
        m.put("event_external_version", eventExternalVersion);
        return m;
    }

    public static DeleteEventDoc fromSource(String id, Map<String, Object> source) {
        return new DeleteEventDoc(
                id,
                asString(source.get("index_uuid")),
                ((Number) source.get("shard_id")).intValue(),
                asString(source.get("allocation_id")),
                ((Number) source.get("seq_no")).longValue(),
                asString(source.get("event_doc_id")),
                asLong(source.getOrDefault("event_external_version", source.get("event_version_ts")))
        );
    }

    private static String asString(Object o) {
        return o == null ? null : o.toString();
    }

    private static long asLong(Object o) {
        return ((Number) o).longValue();
    }
}
