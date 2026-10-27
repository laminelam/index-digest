/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.common.settings.Settings;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.model.LeafBucketDigest;
import org.opensearch.indexdigest.model.ProgressDoc;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.indexdigest.scan.ScanMode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

public class ScanModeTests extends OpenSearchTestCase {

    public void testScanModeParsingAndDefaultSetting() {
        assertSame(ScanMode.PRIMARY_ONLY, ScanMode.parse("primary_only"));
        assertSame(ScanMode.ALL_COPIES, ScanMode.parse("ALL_COPIES"));
        assertSame(ScanMode.SELECTED_COPIES, ScanMode.parse(" selected_copies "));

        assertEquals(ScanMode.PRIMARY_ONLY.settingValue(), IndexDigestSettings.SCAN_MODE.get(Settings.EMPTY));
        assertEquals(10, IndexDigestSettings.MIN_LEVEL.get(Settings.EMPTY).intValue());
        assertEquals(20, IndexDigestSettings.MAX_LEVEL.get(Settings.EMPTY).intValue());
        assertEquals(1, IndexDigestSettings.GUARD_WINDOWS.get(Settings.EMPTY).intValue());

        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> ScanMode.parse("replicas")
        );
        assertTrue(e.getMessage().contains("invalid index-digest scan mode"));
    }

    public void testBucketLevelSettingsRejectInvalidLongShiftLevels() {
        Settings settings = Settings.builder()
                .put("plugins.index_digest.max_level", 63)
                .build();

        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> IndexDigestSettings.MAX_LEVEL.get(settings)
        );
        assertTrue(e.getMessage().contains("62"));
    }

    public void testShardCopyIdentityDoesNotIncludeRoutingMetadata() {
        ShardCopyKey primary = new ShardCopyKey(
                "index-uuid",
                0,
                "alloc-1",
                true,
                "node-a",
                ScanMode.PRIMARY_ONLY
        );
        ShardCopyKey sameCopyAsReplica = new ShardCopyKey(
                "index-uuid",
                0,
                "alloc-1",
                false,
                "node-b",
                ScanMode.ALL_COPIES
        );

        assertEquals(primary, sameCopyAsReplica);
        assertEquals(primary.hashCode(), sameCopyAsReplica.hashCode());
        assertEquals("index-uuid:0:alloc-1", primary.identityString());
        assertEquals("primary", primary.shardRole());
        assertEquals("replica", sameCopyAsReplica.shardRole());
    }

    public void testBucketDocPersistsShardCopyMetadata() {
        ShardCopyKey key = new ShardCopyKey(
                "index-uuid",
                1,
                "alloc-2",
                false,
                "node-b",
                ScanMode.ALL_COPIES
        );
        LeafBucketDigest leafDigest = new LeafBucketDigest(
                10,
                1024L,
                1024L,
                2048L,
                "digest",
                3L,
                0L
        );

        BucketDoc doc = BucketDoc.leaf(key, leafDigest, 4096L);
        Map<String, Object> source = doc.toSource();
        BucketDoc roundTrip = BucketDoc.fromSource(doc.id, source);

        assertEquals("node-b", source.get("node_id"));
        assertEquals("replica", source.get("shard_role"));
        assertEquals("all_copies", source.get("scan_mode"));

        assertEquals("node-b", roundTrip.nodeId);
        assertEquals("replica", roundTrip.shardRole);
        assertEquals("all_copies", roundTrip.scanMode);
        assertEquals(3L, roundTrip.docCount);
    }

    public void testProgressDocRefreshesShardCopyMetadata() {
        ShardCopyKey key = new ShardCopyKey(
                "index-uuid",
                1,
                "alloc-2",
                false,
                "node-b",
                ScanMode.ALL_COPIES
        );
        ProgressDoc progress = ProgressDoc.initialForShardCopy(key, 100L);

        ShardCopyKey updatedKey = new ShardCopyKey(
                "index-uuid",
                1,
                "alloc-2",
                true,
                "node-c",
                ScanMode.SELECTED_COPIES
        );
        ProgressDoc advanced = progress.advance(updatedKey, 200L, 1024L);
        ProgressDoc roundTrip = ProgressDoc.fromSource(advanced.id, advanced.toSource());

        assertEquals("node-c", roundTrip.nodeId);
        assertEquals("primary", roundTrip.shardRole);
        assertEquals("selected_copies", roundTrip.scanMode);
        assertEquals(200L, roundTrip.lastSeenEpochMillis);
        assertEquals(1024L, roundTrip.lastSealedLeafBucketId);
    }
}
