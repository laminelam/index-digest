/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.Uid;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Exercises the harvester against synthetic segments shaped like the engine's:
 * live docs carry _seq_no/_primary_term but no _tombstone; tombstones carry
 * _tombstone, _seq_no, _primary_term, _version and are born soft-deleted, so
 * they are invisible to the (soft-deletes-aware) searcher and only reachable
 * through wrapAllDocsLive — exactly like production.
 */
public class TombstoneHarvesterTests extends OpenSearchTestCase {

    public void testHarvestsOnlyTombstonesAfterBookmark() throws IOException {
        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = tombstoneAwareConfig(NoMergePolicy.INSTANCE);

            try (IndexWriter writer = new IndexWriter(directory, config)) {
                writer.addDocument(liveDoc("keep-1", 10));
                writer.addDocument(tombstone("gone-1", 11, 5_000L));
                writer.commit();
                writer.addDocument(liveDoc("keep-2", 12));
                writer.addDocument(tombstone("gone-2", 13, 6_000L));
                // recovery can duplicate an operation into a second segment
                writer.commit();
                writer.addDocument(tombstone("gone-2", 13, 6_000L));
                writer.commit();

                // Production-faithful invariant: the engine's NRT reader hides
                // soft-deleted tombstones via liveDocs but keeps their segments.
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    assertEquals(2, reader.numDocs());
                    assertTrue(reader.maxDoc() > reader.numDocs());
                }

                List<TombstoneHarvester.DeleteEvent> all = harvest(writer, -1L, Long.MAX_VALUE);
                assertEquals(2, all.size());
                assertEquals(11L, all.get(0).seqNo);
                assertEquals(5_000L, all.get(0).externalVersion);
                assertEquals("gone-1", all.get(0).id);
                assertEquals(13L, all.get(1).seqNo);
                assertEquals(6_000L, all.get(1).externalVersion);
                assertEquals("gone-2", all.get(1).id);

                List<TombstoneHarvester.DeleteEvent> afterBookmark = harvest(writer, 11L, Long.MAX_VALUE);
                assertEquals(1, afterBookmark.size());
                assertEquals(13L, afterBookmark.get(0).seqNo);

                // The upper bound (global checkpoint) excludes operations above it.
                List<TombstoneHarvester.DeleteEvent> bounded = harvest(writer, -1L, 12L);
                assertEquals(1, bounded.size());
                assertEquals(11L, bounded.get(0).seqNo);

                assertEquals(0, harvest(writer, 13L, Long.MAX_VALUE).size());
                assertEquals(0, harvest(writer, 5L, 5L).size());
            }
        }
    }

    public void testNoopTombstonesAreSkipped() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, tombstoneAwareConfig())) {
                writer.addDocument(tombstone("gone-1", 20, 7_000L));
                // NOOP tombstone (e.g. primary failover seq_no gap fill):
                // _tombstone + seqID fields + _version, but NO stored _id.
                Document noop = new Document();
                noop.add(new LongPoint("_seq_no", 21));
                noop.add(new NumericDocValuesField("_seq_no", 21));
                noop.add(new NumericDocValuesField("_primary_term", 1));
                noop.add(new NumericDocValuesField("_version", 1));
                noop.add(new NumericDocValuesField("_tombstone", 1));
                noop.add(new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
                writer.addDocument(noop);
                writer.commit();

                List<TombstoneHarvester.DeleteEvent> events = harvest(writer, -1L, Long.MAX_VALUE);
                assertEquals(1, events.size());
                assertEquals("gone-1", events.get(0).id);
            }
        }
    }

    public void testRejectsNegativeDeleteVersion() throws IOException {
        try (Directory directory = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(directory, tombstoneAwareConfig())) {
                writer.addDocument(tombstone("bad", 7, -3L));
                writer.commit();

                IllegalStateException e = expectThrows(
                        IllegalStateException.class,
                        () -> harvest(writer, -1L, Long.MAX_VALUE)
                );
                assertTrue(e.getMessage(), e.getMessage().contains("non-negative delete versions"));
            }
        }
    }

    private static IndexWriterConfig tombstoneAwareConfig() {
        IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        return tombstoneAwareConfig(config.getMergePolicy(), config);
    }

    private static IndexWriterConfig tombstoneAwareConfig(MergePolicy basePolicy) {
        IndexWriterConfig config = newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        return tombstoneAwareConfig(basePolicy, config);
    }

    /**
     * Mirrors the engine: without a SoftDeletesRetentionMergePolicy, segments
     * whose docs are ALL soft-deleted (e.g. tombstone-only flushes) are dropped
     * at commit — the same reason production needs the retention lease.
     */
    private static IndexWriterConfig tombstoneAwareConfig(MergePolicy basePolicy,
                                                          IndexWriterConfig config
    ) {
        config.setMergePolicy(new SoftDeletesRetentionMergePolicy(
                Lucene.SOFT_DELETES_FIELD,
                MatchAllDocsQuery::new,
                basePolicy
        ));
        return config;
    }

    private static List<TombstoneHarvester.DeleteEvent> harvest(
            IndexWriter writer,
            long afterSeqNo,
            long uptoSeqNo
    ) throws IOException {
        // The engine hands out NRT readers: soft-deleted docs are masked via
        // liveDocs but their segments stay, which is what wrapAllDocsLive relies on.
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
            Engine.Searcher searcher = new Engine.Searcher(
                    "tombstone-harvester-test",
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    () -> {}
            );
            return new TombstoneHarvester().harvest(searcher, afterSeqNo, uptoSeqNo);
        }
    }

    private static Document liveDoc(String id, long seqNo) {
        Document doc = new Document();
        doc.add(new StoredField("_id", Uid.encodeId(id)));
        doc.add(new LongPoint("_seq_no", seqNo));
        doc.add(new NumericDocValuesField("_seq_no", seqNo));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        doc.add(new NumericDocValuesField("_version", 1));
        return doc;
    }

    private static Document tombstone(String id, long seqNo, long deleteVersion) {
        Document doc = new Document();
        doc.add(new StoredField("_id", Uid.encodeId(id)));
        doc.add(new LongPoint("_seq_no", seqNo));
        doc.add(new NumericDocValuesField("_seq_no", seqNo));
        doc.add(new NumericDocValuesField("_primary_term", 1));
        doc.add(new NumericDocValuesField("_version", deleteVersion));
        doc.add(new NumericDocValuesField("_tombstone", 1));
        doc.add(new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
        return doc;
    }
}
