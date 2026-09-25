/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end flow against a real cluster with the plugin installed:
 * index versioned docs, scan, inspect the tree, compare two indices,
 * inject divergence, and assert the compare localizes it to the exact
 * leaf bucket.
 *
 * Level geometry (node defaults): min_level=10 (leaf span 1024),
 * max_level=20 (top window span 1,048,576), guard_windows=1. A frontier
 * doc in window 3 therefore seals windows 0 and 1.
 */
public class IndexDigestIT extends OpenSearchRestTestCase {

    private static final long WINDOW = 1L << 20;
    private static final long LEAF = 1L << 10;
    private static final long FRONTIER_VERSION = 3 * WINDOW + 7;

    public void testDigestsAreDeterministicAcrossSegmentLayoutsAndUpdates() throws IOException {
        String left = "index-digest-det-a";
        String right = "index-digest-det-b";
        configurePlugin(left, right);
        createVersionedIndex(left);
        createVersionedIndex(right);

        // Left: one segment per doc, and doc "u" reaches 2500 via an update.
        indexDoc(left, "d1", 1000);
        refresh(left);
        indexDoc(left, "u", 1500);
        refresh(left);
        indexDoc(left, "u", 2500);
        refresh(left);
        indexDoc(left, "d2", 500_000);
        refresh(left);
        indexDoc(left, "frontier", FRONTIER_VERSION);
        refresh(left);

        // Right: same final logical state, single segment.
        indexDoc(right, "d1", 1000);
        indexDoc(right, "u", 2500);
        indexDoc(right, "d2", 500_000);
        indexDoc(right, "frontier", FRONTIER_VERSION);
        refresh(right);
        forceMerge(right);

        runOnce();

        Map<String, Object> compare = compare(left, right);
        assertEquals(Boolean.TRUE, compare.get("comparable"));
        assertEquals(Boolean.TRUE, compare.get("equal"));
        assertEquals(0, ((Number) compare.get("mismatch_count")).intValue());

        Map<String, Object> leftSide = sub(compare, "left");
        Map<String, Object> rightSide = sub(compare, "right");
        assertEquals(Boolean.TRUE, leftSide.get("progress_found"));
        assertEquals(
                ((Number) leftSide.get("last_sealed_leaf_bucket")).longValue(),
                ((Number) rightSide.get("last_sealed_leaf_bucket")).longValue()
        );
    }

    public void testCompareLocalizesDivergenceToLeafBucket() throws IOException {
        String left = "index-digest-div-a";
        String right = "index-digest-div-b";
        configurePlugin(left, right);
        createVersionedIndex(left);
        createVersionedIndex(right);

        for (String index : List.of(left, right)) {
            indexDoc(index, "shared-1", 1000);
            indexDoc(index, "shared-2", 250_000);
            indexDoc(index, "frontier", FRONTIER_VERSION);
        }
        long divergentVersion = 5000;
        indexDoc(left, "only-in-left", divergentVersion);
        refresh(left);
        refresh(right);

        runOnce();

        Map<String, Object> compare = compare(left, right);
        assertEquals(Boolean.TRUE, compare.get("comparable"));
        assertEquals(Boolean.FALSE, compare.get("equal"));
        assertTrue(((Number) compare.get("mismatch_count")).intValue() >= 1);

        long expectedLeafBucket = (divergentVersion >> 10) << 10;
        boolean localized = false;
        for (Object o : list(compare, "mismatches")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mismatch = (Map<String, Object>) o;
            if (Boolean.TRUE.equals(mismatch.get("leaf"))
                    && ((Number) mismatch.get("bucket_id")).longValue() == expectedLeafBucket) {
                assertEquals("missing_right", mismatch.get("reason"));
                localized = true;
            }
        }
        assertTrue(
                "compare must localize the divergence to leaf bucket " + expectedLeafBucket,
                localized
        );

        // Idempotence: rescanning sealed windows must not change the verdict.
        runOnce();
        Map<String, Object> again = compare(left, right);
        assertEquals(Boolean.FALSE, again.get("equal"));
        assertEquals(
                ((Number) compare.get("mismatch_count")).intValue(),
                ((Number) again.get("mismatch_count")).intValue()
        );
    }

    public void testMissedDeleteIsDetectedViaTombstoneEvents() throws IOException {
        String left = "index-digest-del-a";
        String right = "index-digest-del-b";
        configurePlugin(left, right);
        createVersionedIndex(left);
        createVersionedIndex(right);

        // Round 1: identical docs on both sides; a window-2 frontier seals
        // window 0 with the ADD entries for x, y and shared on both sides.
        for (String index : List.of(left, right)) {
            indexDoc(index, "x", 100_000);
            indexDoc(index, "y", 150_000);
            indexDoc(index, "shared", 200_000);
            indexDoc(index, "frontier-1", 2 * WINDOW + 7);
            refresh(index);
        }
        runOnce();
        assertEquals(Boolean.TRUE, compare(left, right).get("equal"));

        // Round 2: window 0 is sealed on both sides. Delete of x replicates to
        // BOTH sides; delete of y reaches only the left side. Both deletes carry
        // fresh external versions in the still-open window 1, and a window-4
        // frontier seals it.
        long xDeleteVersion = WINDOW + 400_000;
        long yDeleteVersion = WINDOW + 500_000;
        deleteDocExternal(left, "x", xDeleteVersion);
        deleteDocExternal(right, "x", xDeleteVersion);
        deleteDocExternal(left, "y", yDeleteVersion);
        indexDoc(left, "frontier-2", 4 * WINDOW + 7);
        indexDoc(right, "frontier-2", 4 * WINDOW + 7);
        refresh(left);
        refresh(right);

        runOnce();

        Map<String, Object> compare = compare(left, right);
        assertEquals(Boolean.TRUE, compare.get("comparable"));
        assertEquals(Boolean.FALSE, compare.get("equal"));

        long yLeafBucket = (yDeleteVersion >> 10) << 10;
        long xLeafBucket = (xDeleteVersion >> 10) << 10;
        boolean localized = false;
        for (Object o : list(compare, "mismatches")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mismatch = (Map<String, Object>) o;
            long bucketId = ((Number) mismatch.get("bucket_id")).longValue();
            // x's delete event exists identically on both sides: never divergent.
            assertNotEquals(xLeafBucket, bucketId);
            // Window 0 stays sealed and identical on both sides: x and y keep
            // their ADD entries there (the "shadow"), and the deletes appear
            // only as events in window 1 -- appends, not mutations.
            assertTrue("window 0 must not be reported divergent", bucketId >= WINDOW);
            if (Boolean.TRUE.equals(mismatch.get("leaf")) && bucketId == yLeafBucket) {
                assertEquals("missing_right", mismatch.get("reason"));
                localized = true;
            }
        }
        assertTrue("missed delete must surface as a leaf mismatch at bucket " + yLeafBucket, localized);

        // Replaying the missed delete AFTER the right side sealed that window
        // is a late event (below its watermark). The repair is surgical: only
        // the window holding that version is resealed, and it refolds the now-
        // present delete event -- so the two sides CONVERGE, without rebuilding
        // any of the older history.
        deleteDocExternal(right, "y", yDeleteVersion);
        refresh(right);
        runOnce();   // detects the late event, requests the reseal
        runOnce();   // executes it

        Map<String, Object> repaired = wait_equal_helper(left, right);
        assertEquals(Boolean.TRUE, repaired.get("comparable"));
        assertEquals(
                "targeted reseal must converge the two sides after a late delete replay",
                Boolean.TRUE,
                repaired.get("equal")
        );
    }

    /** Polls compare until the trees are equal, or fails after a bounded wait. */
    private Map<String, Object> wait_equal_helper(String left, String right) throws IOException {
        Map<String, Object> last = null;
        for (int i = 0; i < 6; i++) {
            last = compare(left, right);
            if (Boolean.TRUE.equals(last.get("equal"))) {
                return last;
            }
            runOnce();
        }
        return last;
    }

    public void testLateWriteIsRepairedByTargetedReseal() throws IOException {
        String index = "index-digest-late-a";
        configurePlugin(index);
        createVersionedIndex(index);

        indexDoc(index, "d1", 1000);
        indexDoc(index, "d2", 2000);
        indexDoc(index, "frontier", FRONTIER_VERSION);
        refresh(index);
        runOnce();

        Map<String, Object> before = tree(index, 20, 0);
        assertEquals(2L, ((Number) sub(before, "root").get("doc_count")).longValue());

        // Contract violation: a write BELOW the sealed watermark. The scanner
        // detects it (new seq_no, old version) and repairs ONLY the window that
        // contains it - the rest of the tree is never rebuilt.
        indexDoc(index, "late", 3000);
        refresh(index);
        runOnce();   // detects, requests the reseal
        runOnce();   // executes it

        Map<String, Object> after = tree(index, 20, 0);
        assertEquals(
                "late write must be folded into the rebuilt window",
                3L,
                ((Number) sub(after, "root").get("doc_count")).longValue()
        );
        assertNotEquals(
                "the rebuilt digest must differ from the pre-late-write digest",
                sub(before, "root").get("digest"),
                sub(after, "root").get("digest")
        );

        // Idempotence: the detector must not re-fire on the folded doc.
        runOnce();
        Map<String, Object> stable = tree(index, 20, 0);
        assertEquals(sub(after, "root").get("digest"), sub(stable, "root").get("digest"));
        assertEquals(3L, ((Number) sub(stable, "root").get("doc_count")).longValue());
    }

    public void testSealTimingShadowIsAutoResealedToConvergence() throws IOException {
        String left = "index-digest-shadow-a";
        String right = "index-digest-shadow-b";
        createVersionedIndex(left);
        createVersionedIndex(right);

        // Identical docs on both sides, but only LEFT is allowlisted first, so
        // only LEFT seals windows 0..2 while x is still alive.
        for (String index : List.of(left, right)) {
            indexDoc(index, "x", 100_000);
            indexDoc(index, "shared", 200_000);
            indexDoc(index, "frontier", 4 * WINDOW + 7);
            refresh(index);
        }
        configurePlugin(left);
        runOnce();

        // The delete replicates to BOTH sides (fresh external version in the
        // still-open window 3), landing between the two sides' seal moments.
        long deleteVersion = 3 * WINDOW + 100_000;
        deleteDocExternal(left, "x", deleteVersion);
        deleteDocExternal(right, "x", deleteVersion);
        refresh(left);
        refresh(right);

        // Now RIGHT seals the same windows - without x. The classic shadow.
        configurePlugin(left, right);
        runOnce();

        long xAddBucket = (100_000L >> 10) << 10;
        Map<String, Object> mismatched = compare(left, right);
        assertEquals(Boolean.TRUE, mismatched.get("comparable"));
        assertEquals(Boolean.FALSE, mismatched.get("equal"));
        boolean shadowSeen = false;
        for (Object o : list(mismatched, "mismatches")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mismatch = (Map<String, Object>) o;
            if (((Number) mismatch.get("bucket_id")).longValue() == xAddBucket) {
                assertEquals("missing_right", mismatch.get("reason"));
                shadowSeen = true;
            }
        }
        assertTrue("the seal-timing shadow must surface at x's ADD bucket", shadowSeen);
        // ... and the compare must have requested automatic healing for EXACTLY
        // the shadowed window - no spurious extras.
        List<Object> requestedWindows = list(mismatched, "reseal_requested_windows");
        assertEquals("exactly one window must be requested", 1, requestedWindows.size());
        assertEquals(0L, ((Number) requestedWindows.get(0)).longValue());

        // Next scan tick executes the reseal on both sides; the shadow
        // dissolves because both recompute window 0 from current reality.
        runOnce();

        Map<String, Object> healed = compare(left, right);
        assertEquals(Boolean.TRUE, healed.get("comparable"));
        assertEquals("auto-reseal must converge the timing artifact", Boolean.TRUE, healed.get("equal"));
        assertTrue(list(healed, "reseal_requested_windows").isEmpty());
    }

    /**
     * Pins the safety half of the visibility bound. The scanner's bookmark must
     * never pass an operation the reader it holds cannot see, or that
     * operation's delete tombstone is skipped forever.
     * <p>
     * With scheduled refresh disabled, a flush refreshes only the engine's
     * INTERNAL reader and advances the last-refreshed checkpoint, while the
     * EXTERNAL reader still predates the delete. A scanner reading through the
     * external reader would see the bound move, find no tombstone, advance its
     * bookmark past it, and lose the delete permanently - and the two sides
     * would then compare EQUAL despite one having deleted x.
     */
    public void testUnrefreshedDeleteIsNotSkippedByBookmark() throws IOException {
        String left = "index-digest-vis-a";
        String right = "index-digest-vis-b";
        configurePlugin(left, right);
        createVersionedIndex(left, "\"refresh_interval\":\"-1\"");
        createVersionedIndex(right, "\"refresh_interval\":\"-1\"");

        // Round 1: identical, sealed window 0 on both sides.
        for (String index : List.of(left, right)) {
            indexDoc(index, "x", 100_000);
            indexDoc(index, "shared", 200_000);
            indexDoc(index, "frontier-1", 2 * WINDOW + 7);
            refresh(index);
        }
        runOnce();
        assertEquals(Boolean.TRUE, compare(left, right).get("equal"));

        // Round 2: x is deleted on LEFT only. No refresh anywhere - only a
        // flush on the left, which moves the internal reader and the refreshed
        // checkpoint but leaves the external reader where it was.
        long xDeleteVersion = WINDOW + 400_000;
        deleteDocExternal(left, "x", xDeleteVersion);
        indexDoc(left, "frontier-2", 4 * WINDOW + 7);
        indexDoc(right, "frontier-2", 4 * WINDOW + 7);
        flush(left);
        runOnce();   // the bookmark may only pass the tombstone if it was harvested

        // Everything becomes visible everywhere; both sides seal window 1.
        refresh(left);
        refresh(right);
        runOnce();

        Map<String, Object> compare = compare(left, right);
        assertEquals(Boolean.TRUE, compare.get("comparable"));
        assertEquals(
                "the left-only delete must have been harvested before the bookmark passed it",
                Boolean.FALSE,
                compare.get("equal")
        );
        long xDeleteBucket = (xDeleteVersion >> 10) << 10;
        boolean localized = false;
        for (Object o : list(compare, "mismatches")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mismatch = (Map<String, Object>) o;
            if (Boolean.TRUE.equals(mismatch.get("leaf"))
                    && ((Number) mismatch.get("bucket_id")).longValue() == xDeleteBucket) {
                assertEquals("missing_right", mismatch.get("reason"));
                localized = true;
            }
        }
        assertTrue("the harvested delete must surface as a leaf mismatch at bucket " + xDeleteBucket, localized);
    }

    private Map<String, Object> tree(String index, int rootLevel, long rootBucket) throws IOException {
        Request request = new Request("GET", "/_plugins/_index_digest/" + index + "/_tree");
        request.addParameter("shard_id", "0");
        request.addParameter("allocation_id", primaryAllocationId(index));
        request.addParameter("root_level", Integer.toString(rootLevel));
        request.addParameter("root_bucket", Long.toString(rootBucket));
        request.addParameter("depth", "8");
        return entityAsMap(client().performRequest(request));
    }

    public void testTreeEndpointReturnsSealedRoot() throws IOException {
        String index = "index-digest-tree-a";
        configurePlugin(index);
        createVersionedIndex(index);

        indexDoc(index, "d1", 1000);
        indexDoc(index, "d2", 2000);
        indexDoc(index, "frontier", FRONTIER_VERSION);
        refresh(index);

        runOnce();

        String allocationId = primaryAllocationId(index);
        Request request = new Request("GET", "/_plugins/_index_digest/" + index + "/_tree");
        request.addParameter("shard_id", "0");
        request.addParameter("allocation_id", allocationId);
        request.addParameter("root_level", "20");
        request.addParameter("root_bucket", "0");
        request.addParameter("depth", "8");
        Map<String, Object> tree = entityAsMap(client().performRequest(request));

        Map<String, Object> root = sub(tree, "root");
        assertNotNull("window 0 must have a sealed root", root);
        assertEquals(20, ((Number) root.get("level")).intValue());
        assertEquals(2L, ((Number) root.get("doc_count")).longValue());
        assertNotNull(root.get("digest"));
        // depth=8 stops above the leaf level (20 - 8 = 12 > 10).
        assertEquals(Boolean.TRUE, tree.get("truncated"));
    }

    private void configurePlugin(String... indices) throws IOException {
        StringBuilder indexList = new StringBuilder();
        for (String index : indices) {
            if (indexList.length() > 0) {
                indexList.append(',');
            }
            indexList.append('"').append(index).append('"');
        }
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
                "{\"persistent\":{"
                        + "\"plugins.index_digest.enabled\":false,"
                        + "\"plugins.index_digest.indices\":[" + indexList + "],"
                        + "\"plugins.index_digest.max_top_buckets_per_run\":8"
                        + "}}"
        );
        client().performRequest(request);
    }

    private void createVersionedIndex(String index) throws IOException {
        createVersionedIndex(index, null);
    }

    /** @param extraSettingsJson additional `"key":value` pairs for the settings object, or null */
    private void createVersionedIndex(String index, String extraSettingsJson) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity(
                "{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0"
                        + (extraSettingsJson == null ? "" : "," + extraSettingsJson) + "},"
                        + "\"mappings\":{\"properties\":{\"version_ts\":{\"type\":\"long\"}}}}"
        );
        client().performRequest(request);
    }

    private void flush(String index) throws IOException {
        client().performRequest(new Request("POST", "/" + index + "/_flush"));
    }

    private void indexDoc(String index, String id, long externalVersion) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.setJsonEntity("{\"version_ts\":" + externalVersion + "}");
        client().performRequest(request);
    }

    private void refresh(String index) throws IOException {
        client().performRequest(new Request("POST", "/" + index + "/_refresh"));
    }

    private void deleteDocExternal(String index, String id, long externalVersion) throws IOException {
        Request request = new Request("DELETE", "/" + index + "/_doc/" + id);
        request.addParameter("version", Long.toString(externalVersion));
        request.addParameter("version_type", "external");
        client().performRequest(request);
    }

    private void forceMerge(String index) throws IOException {
        Request request = new Request("POST", "/" + index + "/_forcemerge");
        request.addParameter("max_num_segments", "1");
        client().performRequest(request);
    }

    /**
     * The scanner never forces a refresh - it reads only as far as the shard's
     * last refreshed checkpoint proves visible. Tests therefore call
     * {@link #refresh(String)} after every write they expect a tick to see;
     * one tick per call then does the whole tick's work.
     */
    private void runOnce() throws IOException {
        triggerRunOnce();
    }

    private void triggerRunOnce() throws IOException {
        Response response = client().performRequest(new Request("POST", "/_plugins/_index_digest/_run_once"));
        Map<String, Object> body = entityAsMap(response);
        assertEquals(Boolean.TRUE, body.get("triggered"));
        assertEquals(0, ((Number) body.get("skipped_in_flight_shard_copies")).intValue());
        assertTrue(((Number) body.get("attempted_shard_copies")).intValue() >= 1);
    }

    private Map<String, Object> compare(String leftIndex, String rightIndex) throws IOException {
        Request request = new Request("GET", "/_plugins/_index_digest/_compare");
        request.addParameter("left_index", leftIndex);
        request.addParameter("left_shard_id", "0");
        request.addParameter("left_allocation_id", primaryAllocationId(leftIndex));
        request.addParameter("right_index", rightIndex);
        request.addParameter("right_shard_id", "0");
        request.addParameter("right_allocation_id", primaryAllocationId(rightIndex));
        return entityAsMap(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private String primaryAllocationId(String index) throws IOException {
        Response response = client().performRequest(
                new Request("GET", "/_cluster/state/routing_table/" + index)
        );
        Map<String, Object> state = entityAsMap(response);
        Map<String, Object> routingTable = sub(state, "routing_table");
        Map<String, Object> indices = sub(routingTable, "indices");
        Map<String, Object> indexRouting = sub(indices, index);
        Map<String, Object> shards = sub(indexRouting, "shards");
        for (Object shardCopy : (List<Object>) shards.get("0")) {
            Map<String, Object> copy = (Map<String, Object>) shardCopy;
            if (Boolean.TRUE.equals(copy.get("primary"))) {
                return (String) sub(copy, "allocation_id").get("id");
            }
        }
        throw new AssertionError("no started primary for index " + index);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sub(Map<String, Object> map, String key) {
        return (Map<String, Object>) map.get(key);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Map<String, Object> map, String key) {
        return (List<Object>) map.get(key);
    }
}
