/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.bucket;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorable;
import org.opensearch.indexdigest.model.LeafBucketDigest;
import org.opensearch.indexdigest.scan.IdFieldVisitor;
import org.opensearch.index.engine.Engine;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class LeafBucketHasher {
    private static final String DIGEST_ALGO = "SHA-256";
    private static final byte[] FORMAT_MAGIC = "INDEX_DIGEST_LEAF_V3".getBytes(StandardCharsets.UTF_8);
    private static final byte OP_ADD = 1;
    private static final byte OP_DELETE = 2;

    private final BucketMath bucketMath;
    private final String versionFieldName;

    public LeafBucketHasher(BucketMath bucketMath, String versionFieldName) {
        this.bucketMath = bucketMath;
        this.versionFieldName = versionFieldName;
    }

    public static String digestAlgo() {
        return DIGEST_ALGO;
    }

    public List<LeafBucketDigest> computeRun(
            Engine.Searcher searcher,
            int level,
            long startBucketInclusive,
            long endBucketInclusive
    ) throws IOException {
        return computeRun(searcher, level, startBucketInclusive, endBucketInclusive, List.of());
    }

    /**
     * Computes sparse leaf digests for the range from live documents (ADD
     * entries via the searcher) merged with harvested delete events (DELETE
     * entries). Events outside the range are rejected: callers filter to the
     * window being sealed.
     */
    public List<LeafBucketDigest> computeRun(
            Engine.Searcher searcher,
            int level,
            long startBucketInclusive,
            long endBucketInclusive,
            List<DeleteEventEntry> deleteEvents
    ) throws IOException {
        if (endBucketInclusive < startBucketInclusive) {
            return List.of();
        }
        if (startBucketInclusive < 0L || endBucketInclusive < 0L) {
            throw new IllegalArgumentException(
                    "index-digest requires non-negative bucket range start="
                            + startBucketInclusive + " end=" + endBucketInclusive
            );
        }

        long endExclusive = bucketMath.nextBucket(endBucketInclusive, level);
        if (endExclusive <= startBucketInclusive) {
            throw new IllegalArgumentException(
                    "invalid index-digest bucket range start=" + startBucketInclusive
                            + " endBucket=" + endBucketInclusive
                            + " endExclusive=" + endExclusive
            );
        }

        Query rangeQuery = LongPoint.newRangeQuery(
                versionFieldName,
                startBucketInclusive,
                endExclusive - 1
        );

        Map<Long, List<Entry>> bucketToEntries = searcher.search(
                rangeQuery,
                new CollectorManager<BucketCollector, Map<Long, List<Entry>>>() {
                    @Override
                    public BucketCollector newCollector() {
                        return new BucketCollector(level, startBucketInclusive, endExclusive);
                    }

                    @Override
                    public Map<Long, List<Entry>> reduce(Collection<BucketCollector> collectors) {
                        Map<Long, List<Entry>> merged = new TreeMap<>();
                        for (BucketCollector collector : collectors) {
                            for (Map.Entry<Long, List<Entry>> bucketEntries : collector.bucketToEntries.entrySet()) {
                                merged.computeIfAbsent(bucketEntries.getKey(), ignored -> new ArrayList<>())
                                        .addAll(bucketEntries.getValue());
                            }
                        }
                        return merged;
                    }
                }
        );

        for (DeleteEventEntry event : deleteEvents) {
            validateExternalVersion(versionFieldName, event.externalVersion);
            if (event.externalVersion < startBucketInclusive || event.externalVersion >= endExclusive) {
                throw new IllegalStateException(
                        "index-digest delete event version [" + event.externalVersion
                                + "] outside scan range [" + startBucketInclusive + ", " + endExclusive + ")"
                );
            }
            long bucket = bucketMath.bucketId(event.externalVersion, level);
            bucketToEntries.computeIfAbsent(bucket, ignored -> new ArrayList<>())
                    .add(new Entry(OP_DELETE, event.externalVersion, event.id));
        }

        List<LeafBucketDigest> out = new ArrayList<>(bucketToEntries.size());
        for (Map.Entry<Long, List<Entry>> bucketEntries : bucketToEntries.entrySet()) {
            long bucket = bucketEntries.getKey();
            long bucketStart = bucket;
            long bucketEndExclusive = bucketMath.nextBucket(bucket, level);
            List<Entry> entries = bucketEntries.getValue();

            if (bucket < startBucketInclusive || bucket > endBucketInclusive) {
                throw new IllegalStateException(
                        "index-digest computed out-of-range bucket=" + bucket
                                + " start=" + startBucketInclusive
                                + " end=" + endBucketInclusive
                );
            }

            out.add(computeLeafDigest(level, bucket, bucketStart, bucketEndExclusive, entries));
        }

        return out;
    }

    private static void validateExternalVersion(String versionFieldName, long externalVersion) {
        if (externalVersion < 0L) {
            throw new IllegalStateException(
                    "index-digest requires non-negative [" + versionFieldName + "] but found [" + externalVersion + "]"
            );
        }
    }

    private static LeafBucketDigest computeLeafDigest(
            int level,
            long bucket,
            long bucketStart,
            long bucketEndExclusive,
            List<Entry> entries
    ) {
        entries.sort(
                Comparator.comparingLong((Entry e) -> e.externalVersion)
                        .thenComparing((Entry e) -> e.id)
                        .thenComparing((Entry e) -> e.opType)
        );

        String digest = computeDigest(level, bucket, bucketStart, bucketEndExclusive, entries);

        long liveDocCount = 0L;
        long deleteEventCount = 0L;
        for (Entry entry : entries) {
            if (entry.opType == OP_DELETE) {
                deleteEventCount++;
            } else {
                liveDocCount++;
            }
        }

        return new LeafBucketDigest(
                level,
                bucket,
                bucketStart,
                bucketEndExclusive,
                digest,
                liveDocCount,
                deleteEventCount
        );
    }

    private static String loadId(StoredFields storedFields, int docId) throws IOException {
        IdFieldVisitor visitor = new IdFieldVisitor();
        storedFields.document(docId, visitor);
        return visitor.getId();
    }

    private final class BucketCollector implements Collector {
        private final int level;
        private final long startInclusive;
        private final long endExclusive;
        private final Map<Long, List<Entry>> bucketToEntries = new TreeMap<>();

        private BucketCollector(int level, long startInclusive, long endExclusive) {
            this.level = level;
            this.startInclusive = startInclusive;
            this.endExclusive = endExclusive;
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
            LeafReader reader = ctx.reader();
            SortedNumericDocValues versionValues = DocValues.getSortedNumeric(reader, versionFieldName);
            StoredFields storedFields = reader.storedFields();
            if (versionValues == null) {
                throw new IllegalStateException(
                        "index-digest requires doc values on field [" + versionFieldName + "]"
                );
            }

            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) {}

                @Override
                public void collect(int docId) throws IOException {
                    if (versionValues.advanceExact(docId) == false) {
                        throw new IllegalStateException(
                                "Matched doc is missing doc values for field [" + versionFieldName + "]"
                        );
                    }

                    int count = versionValues.docValueCount();
                    if (count != 1) {
                        throw new IllegalStateException(
                                "index-digest requires exactly one doc value for field ["
                                        + versionFieldName + "] but found [" + count + "]"
                        );
                    }

                    long externalVersion = versionValues.nextValue();
                    validateExternalVersion(versionFieldName, externalVersion);
                    if (externalVersion < startInclusive || externalVersion >= endExclusive) {
                        throw new IllegalStateException(
                                "Matched doc has inconsistent point/doc-value for field ["
                                        + versionFieldName + "] value [" + externalVersion
                                        + "] outside scan range [" + startInclusive + ", " + endExclusive + ")"
                        );
                    }

                    String id = loadId(storedFields, docId);
                    if (id == null) {
                        throw new IllegalStateException("Matched doc is missing stored [_id]");
                    }

                    long bucket = bucketMath.bucketId(externalVersion, level);
                    bucketToEntries.computeIfAbsent(bucket, ignored -> new ArrayList<>())
                            .add(new Entry(OP_ADD, externalVersion, id));
                }
            };
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    private static String computeDigest(
            int level,
            long bucketId,
            long bucketStart,
            long bucketEnd,
            List<Entry> entries
    ) {
        try {
            MessageDigest md = MessageDigest.getInstance(DIGEST_ALGO);

            md.update(FORMAT_MAGIC);
            md.update(longBytes(level));
            md.update(longBytes(bucketId));
            md.update(longBytes(bucketStart));
            md.update(longBytes(bucketEnd));
            md.update(longBytes(entries.size()));

            for (Entry entry : entries) {
                md.update(encodeEntry(entry));
            }

            return HexFormat.of().formatHex(md.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Missing digest algorithm " + DIGEST_ALGO, e);
        }
    }

    private static byte[] encodeEntry(Entry entry) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);

            out.writeByte(entry.opType);
            out.writeLong(entry.externalVersion);

            byte[] idBytes = entry.id.getBytes(StandardCharsets.UTF_8);
            out.writeInt(idBytes.length);
            out.write(idBytes);

            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed encoding index-digest bucket entry", e);
        }
    }

    private static byte[] longBytes(long v) {
        return new byte[] {
                (byte) (v >>> 56),
                (byte) (v >>> 48),
                (byte) (v >>> 40),
                (byte) (v >>> 32),
                (byte) (v >>> 24),
                (byte) (v >>> 16),
                (byte) (v >>> 8),
                (byte) v
        };
    }

    private static final class Entry {
        final byte opType;
        final long externalVersion;
        final String id;

        private Entry(byte opType, long externalVersion, String id) {
            this.opType = opType;
            this.externalVersion = externalVersion;
            this.id = id;
        }
    }

    /**
     * A harvested delete event to fold into leaf digests: the deleted doc's id
     * and the external version supplied on the DELETE call.
     */
    public static final class DeleteEventEntry {
        public final long externalVersion;
        public final String id;

        public DeleteEventEntry(long externalVersion, String id) {
            this.externalVersion = externalVersion;
            this.id = id;
        }
    }
}
