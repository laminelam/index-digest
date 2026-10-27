/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.model;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

public class ProgressDocTests extends OpenSearchTestCase {

    public void testBucketLevelsRoundTripThroughSource() {
        ShardCopyKey key = new ShardCopyKey("index-uuid", 2, "alloc-1", true, "node-1", "primary_only");
        ProgressDoc original = ProgressDoc.initialForShardCopy(key, 123L, 10, 20)
                .advance(key, 456L, 8192L);

        ProgressDoc parsed = ProgressDoc.fromSource(original.id, original.toSource());

        assertEquals(10, parsed.minLevel);
        assertEquals(20, parsed.maxLevel);
        assertEquals(8192L, parsed.lastSealedLeafBucketId);
        assertEquals(456L, parsed.lastSeenEpochMillis);
        assertEquals("primary", parsed.shardRole);
        assertEquals("primary_only", parsed.scanMode);
    }

    public void testProcessedSeqNoRoundTripsAndSurvivesOtherWithers() {
        ShardCopyKey key = new ShardCopyKey("index-uuid", 2, "alloc-1", true, "node-1", "primary_only");
        ProgressDoc progress = ProgressDoc.initialForShardCopy(key, 123L, 10, 20);
        assertEquals(-1L, progress.lastProcessedSeqNo);

        progress = progress.withProcessedSeqNo(key, 200L, 57L, 9_000L)
                .advance(key, 300L, 8192L)
                .withUpdatedHeartbeat(key, 400L);

        ProgressDoc parsed = ProgressDoc.fromSource(progress.id, progress.toSource());
        assertEquals(57L, parsed.lastProcessedSeqNo);
        assertEquals(8192L, parsed.lastSealedLeafBucketId);
        assertEquals("harvest frontier bounds any future gap repair", 9_000L, parsed.lastHarvestMaxExternalVersion);
    }

    public void testFormatVersionRoundTripsAndLegacyDefaultsToUnknown() {
        ShardCopyKey key = new ShardCopyKey("index-uuid", 2, "alloc-1", true, "node-1", "primary_only");
        ProgressDoc progress = ProgressDoc.initialForShardCopy(key, 123L, 10, 20, 3)
                .advance(key, 300L, 8192L);

        assertEquals(3, ProgressDoc.fromSource(progress.id, progress.toSource()).formatVersion);

        Map<String, Object> legacy = new HashMap<>();
        legacy.put("index_uuid", "index-uuid");
        legacy.put("shard_id", 2);
        legacy.put("allocation_id", "alloc-1");
        legacy.put("last_seen_epoch_millis", 123L);
        legacy.put("last_sealed_leaf_bucket_id", 8192L);
        assertEquals(-1, ProgressDoc.fromSource("progress", legacy).formatVersion);
    }

    public void testLegacySourceWithoutProcessedSeqNoDefaultsToNone() {
        Map<String, Object> source = new HashMap<>();
        source.put("index_uuid", "index-uuid");
        source.put("shard_id", 2);
        source.put("allocation_id", "alloc-1");
        source.put("last_seen_epoch_millis", 123L);
        source.put("last_sealed_leaf_bucket_id", 8192L);

        assertEquals(-1L, ProgressDoc.fromSource("progress", source).lastProcessedSeqNo);
    }

    public void testLegacySourceWithoutBucketLevelsDefaultsToUnknown() {
        Map<String, Object> source = new HashMap<>();
        source.put("index_uuid", "index-uuid");
        source.put("shard_id", 2);
        source.put("allocation_id", "alloc-1");
        source.put("last_seen_epoch_millis", 123L);
        source.put("last_sealed_leaf_bucket_id", 8192L);

        ProgressDoc parsed = ProgressDoc.fromSource("progress", source);

        assertEquals(-1, parsed.minLevel);
        assertEquals(-1, parsed.maxLevel);
        assertEquals(8192L, parsed.lastSealedLeafBucketId);
    }
}
