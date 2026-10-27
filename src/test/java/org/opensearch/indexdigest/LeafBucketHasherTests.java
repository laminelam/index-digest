/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.LeafBucketHasher;
import org.opensearch.indexdigest.model.LeafBucketDigest;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class LeafBucketHasherTests extends OpenSearchTestCase {

    public void testRejectsNegativeBucketRange() {
        LeafBucketHasher hasher = new LeafBucketHasher(BucketMath.INSTANCE, "version_ts");

        IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> hasher.computeRun(null, 10, -1024L, -1024L)
        );
        assertTrue(e.getMessage().contains("non-negative bucket range"));
    }

    public void testDigestIsStableAcrossSegmentLayoutsAndUpdates() throws IOException {
        try (Directory multiSegmentDirectory = newDirectory();
             Directory forceMergedDirectory = newDirectory()) {

            indexSampleDocs(multiSegmentDirectory, false);
            indexSampleDocs(forceMergedDirectory, true);

            List<LeafBucketDigest> multiSegmentDigests = computeDigests(multiSegmentDirectory);
            List<LeafBucketDigest> forceMergedDigests = computeDigests(forceMergedDirectory);

            assertEquals(2, multiSegmentDigests.size());
            assertEquals(2, forceMergedDigests.size());

            assertEquals(1024L, multiSegmentDigests.get(0).bucketId);
            assertEquals(2L, multiSegmentDigests.get(0).liveDocCount);
            assertEquals("59f1ba4a637be4652a1d8aa5edfd35cf69cad49c5952f62efe410bc2d10ce61d", multiSegmentDigests.get(0).digest);

            assertEquals(2048L, multiSegmentDigests.get(1).bucketId);
            assertEquals(1L, multiSegmentDigests.get(1).liveDocCount);
            assertEquals("01a1bacefaa9ed3789360b22353e8e2a78b92e78a69006c0fe33ad5386537976", multiSegmentDigests.get(1).digest);

            assertEquals(multiSegmentDigests.get(0).digest, forceMergedDigests.get(0).digest);
            assertEquals(multiSegmentDigests.get(1).digest, forceMergedDigests.get(1).digest);
        }
    }

    public void testDeleteEventsFoldIntoLeafDigests() throws IOException {
        try (Directory multiSegmentDirectory = newDirectory();
             Directory forceMergedDirectory = newDirectory()) {

            indexSampleDocs(multiSegmentDirectory, false);
            indexSampleDocs(forceMergedDirectory, true);

            List<LeafBucketHasher.DeleteEventEntry> events = List.of(
                    new LeafBucketHasher.DeleteEventEntry(1500L, "doc-d"),
                    new LeafBucketHasher.DeleteEventEntry(2600L, "doc-e")
            );

            List<LeafBucketDigest> plain = computeDigests(multiSegmentDirectory, List.of());
            List<LeafBucketDigest> withEvents = computeDigests(multiSegmentDirectory, events);
            List<LeafBucketDigest> withEventsMerged = computeDigests(forceMergedDirectory, events);

            assertEquals(2, withEvents.size());

            assertEquals(1024L, withEvents.get(0).bucketId);
            assertEquals(2L, withEvents.get(0).liveDocCount);
            assertEquals(1L, withEvents.get(0).deleteEventCount);
            assertNotEquals(plain.get(0).digest, withEvents.get(0).digest);

            assertEquals(2048L, withEvents.get(1).bucketId);
            assertEquals(1L, withEvents.get(1).liveDocCount);
            assertEquals(1L, withEvents.get(1).deleteEventCount);
            assertNotEquals(plain.get(1).digest, withEvents.get(1).digest);

            // Determinism: identical live data + identical events => identical
            // digests regardless of segment layout.
            assertEquals(withEvents.get(0).digest, withEventsMerged.get(0).digest);
            assertEquals(withEvents.get(1).digest, withEventsMerged.get(1).digest);
        }
    }

    public void testDeleteOnlyLeafBucket() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSampleDocs(directory, false);

            // Bucket 3072 has no live docs in the sample data; the event alone
            // must materialize it.
            LeafBucketHasher hasher = new LeafBucketHasher(BucketMath.INSTANCE, "version_ts");
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                Engine.Searcher searcher = new Engine.Searcher(
                        "index-digest-test",
                        reader,
                        IndexSearcher.getDefaultSimilarity(),
                        IndexSearcher.getDefaultQueryCache(),
                        IndexSearcher.getDefaultQueryCachingPolicy(),
                        () -> {}
                );
                List<LeafBucketDigest> digests = hasher.computeRun(
                        searcher,
                        10,
                        1024L,
                        3072L,
                        List.of(new LeafBucketHasher.DeleteEventEntry(3100L, "doc-z"))
                );

                assertEquals(3, digests.size());
                LeafBucketDigest deleteOnly = digests.get(2);
                assertEquals(3072L, deleteOnly.bucketId);
                assertEquals(0L, deleteOnly.liveDocCount);
                assertEquals(1L, deleteOnly.deleteEventCount);
                assertNotNull(deleteOnly.digest);
            }
        }
    }

    public void testDeleteEventOutsideScanRangeIsRejected() throws IOException {
        try (Directory directory = newDirectory()) {
            indexSampleDocs(directory, false);

            IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> computeDigests(
                            directory,
                            List.of(new LeafBucketHasher.DeleteEventEntry(5000L, "doc-x"))
                    )
            );
            assertTrue(e.getMessage(), e.getMessage().contains("outside scan range"));
        }
    }

    private static List<LeafBucketDigest> computeDigests(Directory directory) throws IOException {
        return computeDigests(directory, List.of());
    }

    private static List<LeafBucketDigest> computeDigests(
            Directory directory,
            List<LeafBucketHasher.DeleteEventEntry> deleteEvents
    ) throws IOException {
        LeafBucketHasher hasher = new LeafBucketHasher(BucketMath.INSTANCE, "version_ts");
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            Engine.Searcher searcher = new Engine.Searcher(
                    "index-digest-test",
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    () -> {}
            );
            return hasher.computeRun(searcher, 10, 1024L, 2048L, deleteEvents);
        }
    }

    private static void indexSampleDocs(Directory directory, boolean forceMerge) throws IOException {
        IndexWriterConfig config = newIndexWriterConfig();
        if (forceMerge == false) {
            config.setMergePolicy(NoMergePolicy.INSTANCE);
        }

        try (IndexWriter writer = new IndexWriter(directory, config)) {
            writer.addDocument(doc("doc-a", 1200L));
            writer.commit();

            writer.addDocument(doc("doc-b", 1100L));
            writer.commit();

            writer.updateDocument(new Term("id", "doc-a"), doc("doc-a", 1300L));
            writer.commit();

            writer.addDocument(doc("doc-d", 1600L));
            writer.commit();
            writer.deleteDocuments(new Term("id", "doc-d"));
            writer.commit();

            writer.addDocument(doc("doc-c", 2500L));

            if (forceMerge) {
                writer.forceMerge(1);
            }

            writer.commit();
        }
    }

    private static Document doc(String id, long externalVersion) {
        Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.NO));
        doc.add(new StoredField("_id", Uid.encodeId(id)));
        doc.add(new LongPoint("version_ts", externalVersion));
        doc.add(new SortedNumericDocValuesField("version_ts", externalVersion));
        return doc;
    }
}
