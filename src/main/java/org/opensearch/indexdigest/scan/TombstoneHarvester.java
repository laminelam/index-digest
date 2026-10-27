/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Harvests delete tombstones directly from the Lucene segments.
 *
 * Delete tombstones are real (soft-deleted) documents carrying only metadata:
 * _id, _seq_no, _primary_term and _version — where _version is the external
 * version supplied on the DELETE call, i.e. the event's position on the
 * monotonic external-version axis. They are invisible to normal searches, so this
 * reads through {@link Lucene#wrapAllDocsLive}, and matches only tombstones
 * via FieldExistsQuery(_tombstone): cost is proportional to the number of new
 * deletes, not to write throughput.
 */
public final class TombstoneHarvester {

    /**
     * Harvests delete events in the seq_no range (afterSeqNo, uptoSeqNo].
     * The caller bounds uptoSeqNo at min(global checkpoint, last refreshed
     * checkpoint) of the reader it passes: operations above the global
     * checkpoint can still be rolled back on failover, and operations above
     * the refreshed checkpoint are not yet in this reader - neither may
     * become events yet.
     */
    public List<DeleteEvent> harvest(Engine.Searcher searcher, long afterSeqNo, long uptoSeqNo) throws IOException {
        if (uptoSeqNo <= afterSeqNo) {
            return List.of();
        }

        IndexSearcher allDocsSearcher = new IndexSearcher(Lucene.wrapAllDocsLive(searcher.getDirectoryReader()));
        // One-shot filters; keep them out of the JVM-global query cache.
        allDocsSearcher.setQueryCache(null);

        // IndexOrDocValuesQuery lets the tombstone-existence clause drive the
        // iteration and verifies the seq_no range per candidate via doc values,
        // instead of materializing a BKD bitset over every write since the
        // bookmark: cost stays proportional to tombstones, not to write TPS.
        Query seqNoRange = new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(SeqNoFieldMapper.NAME, afterSeqNo + 1, uptoSeqNo),
                SortedNumericDocValuesField.newSlowRangeQuery(SeqNoFieldMapper.NAME, afterSeqNo + 1, uptoSeqNo)
        );
        Query query = new BooleanQuery.Builder()
                .add(new FieldExistsQuery(SeqNoFieldMapper.TOMBSTONE_NAME), BooleanClause.Occur.FILTER)
                // excludes nested child docs, which carry no primary term
                .add(new FieldExistsQuery(SeqNoFieldMapper.PRIMARY_TERM_NAME), BooleanClause.Occur.FILTER)
                .add(seqNoRange, BooleanClause.Occur.FILTER)
                .build();

        List<DeleteEvent> events = new ArrayList<>();

        allDocsSearcher.search(query, new Collector() {
            @Override
            public LeafCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
                LeafReader reader = ctx.reader();
                NumericDocValues seqNos = DocValues.getNumeric(reader, SeqNoFieldMapper.NAME);
                NumericDocValues versions = DocValues.getNumeric(reader, VersionFieldMapper.NAME);
                StoredFields storedFields = reader.storedFields();

                return new LeafCollector() {
                    @Override
                    public void setScorer(Scorable scorer) {}

                    @Override
                    public void collect(int docId) throws IOException {
                        if (seqNos.advanceExact(docId) == false) {
                            throw new IllegalStateException("index-digest tombstone doc is missing [_seq_no] doc values");
                        }
                        long seqNo = seqNos.longValue();

                        if (versions.advanceExact(docId) == false) {
                            throw new IllegalStateException("index-digest tombstone doc is missing [_version] doc values");
                        }
                        long externalVersion = versions.longValue();
                        if (externalVersion < 0L) {
                            throw new IllegalStateException(
                                    "index-digest requires non-negative delete versions but found [" + externalVersion
                                            + "] at seq_no [" + seqNo + "]"
                            );
                        }

                        IdFieldVisitor visitor = new IdFieldVisitor();
                        storedFields.document(docId, visitor);
                        String id = visitor.getId();
                        if (id == null) {
                            // A tombstone without a stored _id is a NOOP (written to
                            // fill seq_no gaps, e.g. on primary failover), not a
                            // delete — the same discriminator LuceneChangesSnapshot
                            // uses. Skip it.
                            return;
                        }

                        events.add(new DeleteEvent(seqNo, externalVersion, id));
                    }
                };
            }

            @Override
            public ScoreMode scoreMode() {
                return ScoreMode.COMPLETE_NO_SCORES;
            }
        });

        events.sort(Comparator.comparingLong(e -> e.seqNo));

        // Recovery can leave the same operation in more than one segment;
        // one seq_no is one delete event.
        List<DeleteEvent> deduped = new ArrayList<>(events.size());
        long previousSeqNo = Long.MIN_VALUE;
        for (DeleteEvent event : events) {
            if (event.seqNo != previousSeqNo) {
                deduped.add(event);
                previousSeqNo = event.seqNo;
            }
        }
        return deduped;
    }

    public static final class DeleteEvent {
        public final long seqNo;
        public final long externalVersion;
        public final String id;

        public DeleteEvent(long seqNo, long externalVersion, String id) {
            this.seqNo = seqNo;
            this.externalVersion = externalVersion;
            this.id = id;
        }
    }
}
