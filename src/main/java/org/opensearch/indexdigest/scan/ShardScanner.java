/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.indexdigest.IndexDigestSettings;
import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.BucketRange;
import org.opensearch.indexdigest.bucket.BucketReducer;
import org.opensearch.indexdigest.bucket.LeafBucketHasher;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.model.DeleteEventDoc;
import org.opensearch.indexdigest.model.LeafBucketDigest;
import org.opensearch.indexdigest.model.ProgressDoc;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.indexdigest.persistence.IndexDigestIndexDao;
import org.opensearch.indexdigest.persistence.IndexDigestSystemIndex;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException;
import org.opensearch.index.seqno.RetentionLeaseNotFoundException;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Scans local shards and persists sealed bucket digests.
 * <p>
 * Separation of concerns:
 * 1) determine sealed leaf buckets for the run
 * 2) build an in-memory forest (real parent/children tree structure)
 * 3) hash the forest recursively bottom-up
 * 4) flatten final docs and persist
 */
public final class ShardScanner {
    private static final Logger logger = LogManager.getLogger(ShardScanner.class);

    /**
     * Digest/tree format generation. Bumped when persisted digests are no
     * longer comparable with earlier builds; progress from another format
     * self-heals via reset + rebuild.
     */
    public static final int FORMAT_VERSION = 3;

    private static final int MAX_RESEALS_PER_TICK = 4;

    private final IndicesService indicesService;
    private final IndexDigestIndexDao dao;
    private final ThreadPool threadPool;

    private final int minLevel;
    private final int maxLevel;
    private volatile int maxTopBucketsPerRun;
    private volatile int guardWindows;

    private final BucketMath bucketMath;
    private final String versionFieldName;
    private final LeafBucketHasher leafBucketHasher;
    private final BucketReducer bucketReducer;
    private final TombstoneHarvester tombstoneHarvester = new TombstoneHarvester();
    private final RepairPlanner repairPlanner;
    private final Set<ShardCopyKey> inFlightShardCopies = ConcurrentHashMap.newKeySet();


    public ShardScanner(
            IndicesService indicesService,
            IndexDigestIndexDao dao,
            ClusterSettings clusterSettings,
            Settings nodeSettings,
            BucketMath bucketMath,
            String versionFieldName,
            ThreadPool threadPool
    ) {
        this.indicesService = indicesService;
        this.dao = dao;
        this.threadPool = threadPool;
        this.bucketMath = bucketMath;
        this.versionFieldName = versionFieldName;
        this.leafBucketHasher = new LeafBucketHasher(bucketMath, versionFieldName);
        this.bucketReducer = new BucketReducer(bucketMath);
        this.repairPlanner = new RepairPlanner(
                bucketMath,
                IndexDigestSettings.MIN_LEVEL.get(nodeSettings),
                IndexDigestSettings.MAX_LEVEL.get(nodeSettings)
        );

        this.minLevel = IndexDigestSettings.MIN_LEVEL.get(nodeSettings);
        this.maxLevel = IndexDigestSettings.MAX_LEVEL.get(nodeSettings);
        this.maxTopBucketsPerRun = IndexDigestSettings.MAX_TOP_BUCKETS_PER_RUN.get(nodeSettings);
        this.guardWindows = IndexDigestSettings.GUARD_WINDOWS.get(nodeSettings);

        clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.MAX_TOP_BUCKETS_PER_RUN, v -> this.maxTopBucketsPerRun = v);
        clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.GUARD_WINDOWS, v -> this.guardWindows = v);
    }

    /**
     * Resolves and scans one local shard copy.
     * <p>
     * Returns {@code false} only when this exact shard copy is already being
     * scanned. A missing or currently non-scannable local copy is treated as a
     * successful no-op: the resolver/scheduler can try again on a later tick.
     */
    public boolean scanShardCopy(ShardCopyKey key) {
        if (inFlightShardCopies.add(key) == false) {
            logger.debug("index-digest skipping shard copy {} because a scan is already in progress", key);
            return false;
        }

        try {
            IndexShard shard = findLocalShard(key);
            if (shard == null) {
                return true;
            }
            if (shouldScan(shard) == false) {
                return true;
            }

            ShardCopyKey enrichedKey = enrichKey(key, shard);
            long t = System.nanoTime();
            try (Releasable ignored = () -> cleanupIfSystemIndexWasRecreated(enrichedKey)) {
                runScanTick(enrichedKey, shard);
            } finally {
                logger.debug("index-digest step={} took_ms={}", "scanShardCopy", elapsedMs(t));
            }
            return true;
        } finally {
            inFlightShardCopies.remove(key);
        }
    }

    private IndexShard findLocalShard(ShardCopyKey key) {
        for (var indexService : indicesService) {
            for (IndexShard shard : indexService) {
                if (shard == null) {
                    continue;
                }
                if (shard.shardId().id() != key.shardId) {
                    continue;
                }
                if (shard.shardId().getIndex().getUUID().equals(key.indexUUID) == false) {
                    continue;
                }
                if (shard.routingEntry() == null || shard.routingEntry().allocationId() == null) {
                    continue;
                }
                String allocId = shard.routingEntry().allocationId().getId();
                if (key.allocationId.equals(allocId) == false) {
                    continue;
                }
                return shard;
            }
        }
        return null;
    }

    private boolean shouldScan(IndexShard shard) {
        if (shard == null) return false;
        if (shard.state() != IndexShardState.STARTED) return false;
        if (shard.routingEntry() == null) return false;
        if (shard.routingEntry().started() == false) return false;

        /*
         * Segment-replication / remote-store REPLICAS run NRTReplicationEngine:
         * their global checkpoint tracks the translog while Lucene lags at the
         * last-copied segments, refresh is a no-op, and operation history is
         * never complete - the checkpoint/harvest contract only holds on
         * InternalEngine copies. Primaries of such indices index normally and
         * stay scannable.
         */
        if (shard.indexSettings().isSegRepEnabledOrRemoteNode()
                && shard.routingEntry().primary() == false) {
            logger.debug(
                    "index-digest skipping segment-replication/remote-store replica {} shard {}",
                    shard.shardId().getIndexName(),
                    shard.shardId().id()
            );
            return false;
        }

        /*
         * The visibility bound (getLastRefreshedCheckpoint) is defined only on
         * InternalEngine. Closed indices keep STARTED shards on a NoOpEngine and
         * remote-snapshot indices run a ReadOnlyEngine; neither can be scanned,
         * and neither has anything to scan.
         */
        if (shard.indexSettings().getIndexMetadata().getState() == IndexMetadata.State.CLOSE
                || shard.indexSettings().isRemoteSnapshot()) {
            logger.debug(
                    "index-digest skipping closed or remote-snapshot index {} shard {}",
                    shard.shardId().getIndexName(),
                    shard.shardId().id()
            );
            return false;
        }

        String indexName = shard.shardId().getIndexName();
        return IndexDigestSystemIndex.INDEX_NAME.equals(indexName) == false;
    }

    private ShardCopyKey enrichKey(ShardCopyKey key, IndexShard shard) {
        return new ShardCopyKey(
                key.indexUUID,
                key.shardId,
                key.allocationId,
                shard.routingEntry().primary(),
                shard.routingEntry().currentNodeId(),
                key.scanMode
        );
    }

    private void cleanupIfSystemIndexWasRecreated(ShardCopyKey key) {
        if (dao.consumeSystemIndexRecreated()) {
            // .index_digest disappeared mid-tick and was recreated: anything this
            // tick re-persisted (e.g. an advanced watermark) would sit over an
            // otherwise empty store. Reset; the next tick bootstraps cleanly.
            logger.warn(
                    "index-digest reason=system_index_recreated resetting derived data for shard copy {}",
                    key
            );
            dao.resetShardCopy(key);
        }
    }

    /**
     * Executes one scan tick for a single local shard copy.
     * <p>
     * The tick harvests delete tombstones, advances the seq_no watermark only
     * after those events are durable, schedules bounded reseals for late
     * arrivals, executes pending reseals, and seals newly eligible top-level
     * windows. Repair is intentionally expressed as queued reseal work or a
     * bounded watermark rewind, so this method does not recursively retry.
     */
    private void runScanTick(ShardCopyKey key, IndexShard shard) {
        final long nowMillis = threadPool.absoluteTimeInMillis();

        validateBucketLevels();
        validateVersionFieldMapping(shard);

        /*
         * Refresh policy belongs to the operator, so the scanner never forces a
         * refresh; it only ever reads as far as the reader it holds provably
         * covers. Two bounds, both captured BEFORE acquiring the searcher:
         *
         * - the global checkpoint: every operation at or below it has completed
         *   its Lucene write and been acknowledged;
         * - the last refreshed checkpoint: every operation at or below it is
         *   visible in the engine's INTERNAL reader as of its last refresh.
         *
         * The searcher is acquired from that same internal reader manager -
         * never the external one, which OpenSearch deliberately leaves behind
         * on flush, indexing-buffer writes and realtime GET while still
         * advancing the refreshed checkpoint. Readers only move forward, so the
         * reader acquired next is at least as fresh as the refresh that
         * published the second bound. The minimum of the two bounds is
         * therefore both acknowledged and visible: the frontier this tick may
         * harvest, and advance the bookmark, up to.
         */
        final long observedCheckpoint = shard.seqNoStats().getGlobalCheckpoint();
        final long refreshedCheckpoint;
        try {
            refreshedCheckpoint = shard.getLastRefreshedCheckpoint();
        } catch (ClassCastException e) {
            // Engine swapped under us (reset / promotion in progress): no
            // InternalEngine, no refreshed checkpoint, nothing provable this tick.
            logger.debug("index-digest skipping shard copy {} because its engine is being reset", key);
            return;
        }

        try (Engine.SearcherSupplier supplier = shard.acquireSearcherSupplier(Engine.SearcherScope.INTERNAL);
             Engine.Searcher searcher = supplier.acquireSearcher("index-digest-shard-copy-scan")) {
            long visibleCheckpoint = Math.min(observedCheckpoint, refreshedCheckpoint);
            if (visibleCheckpoint < 0L) {
                logger.debug(
                        "index-digest nothing provably visible yet for shard copy {} "
                                + "(global_checkpoint={} refreshed_checkpoint={})",
                        key,
                        observedCheckpoint,
                        refreshedCheckpoint
                );
                return;
            }

            ProgressDoc progress = dao.getProgress(key);
            /*
             * A recreation observed up to this point is benign: nothing was
             * written yet this tick, and a recreated store returns progress
             * null, which bootstraps cleanly below. Only a recreation AFTER
             * this read (caught by the tick-end check) can leave a resurrected
             * watermark over an empty store.
             */
            dao.consumeSystemIndexRecreated();
            if (progress == null) {
                progress = ProgressDoc.initialForShardCopy(key, nowMillis, minLevel, maxLevel, FORMAT_VERSION);
            } else if (needsReset(progress)) {
                logger.warn(
                        "index-digest reason=incompatible_progress resetting derived data for shard copy {} "
                                + "progress_min_level={} progress_max_level={} progress_format={} "
                                + "settings_min_level={} settings_max_level={} format={}",
                        key,
                        progress.minLevel,
                        progress.maxLevel,
                        progress.formatVersion,
                        minLevel,
                        maxLevel,
                        FORMAT_VERSION
                );
                dao.resetShardCopy(key);
                progress = ProgressDoc.initialForShardCopy(key, nowMillis, minLevel, maxLevel, FORMAT_VERSION);
            }

            /*
             * Delete pipeline, before any sealing:
             * gap-check the tombstone history, harvest new tombstones, persist
             * them durably, and only then advance the seq_no watermark and the
             * retention lease. A crash between steps re-reads the same
             * tombstones (idempotent event ids) because the lease has not moved.
             */
            if (progress.lastProcessedSeqNo >= 0
                    && shard.hasCompleteHistoryOperations("index-digest", progress.lastProcessedSeqNo + 1) == false) {
                /*
                 * Tombstones since the bookmark may have been merged away. The
                 * lost deletes carry versions ABOVE the frontier observed at the
                 * last successful harvest (versions are monotonic), so only the
                 * windows from that frontier forward can be wrong. Rewind the
                 * sealed watermark to that point and re-seal forward; every
                 * older window - years of history - stays sealed and untouched.
                 */
                long rewindTo = repairPlanner.rewindWatermarkForHistoryGap(
                        progress.lastHarvestMaxExternalVersion, progress.lastSealedLeafBucketId);
                logger.warn(
                        "index-digest reason=incomplete_delete_history rewinding shard copy {} from watermark={} to {} "
                                + "because tombstones after seq_no={} may have been merged away",
                        key,
                        progress.lastSealedLeafBucketId,
                        rewindTo,
                        progress.lastProcessedSeqNo
                );
                progress = progress.rewindSealedWatermark(key, nowMillis, rewindTo);
                dao.upsertProgress(progress);
            }

            List<TombstoneHarvester.DeleteEvent> harvested =
                    tombstoneHarvester.harvest(searcher, progress.lastProcessedSeqNo, visibleCheckpoint);

            if (harvested.isEmpty() == false) {
                // Persist first: the events are the durable record, and a late
                // one still belongs in the tree - it just needs its window redone.
                List<DeleteEventDoc> eventDocs = new ArrayList<>(harvested.size());
                List<Long> eventExternalVersions = new ArrayList<>(harvested.size());
                for (TombstoneHarvester.DeleteEvent event : harvested) {
                    eventDocs.add(DeleteEventDoc.create(key, event.seqNo, event.id, event.externalVersion));
                    eventExternalVersions.add(event.externalVersion);
                }
                dao.upsertDeleteEvents(eventDocs);
                Set<Long> lateWindows =
                        repairPlanner.windowsNeedingReseal(eventExternalVersions, progress.lastSealedLeafBucketId);

                /*
                 * A delete below the sealed watermark lands in an already-frozen
                 * window. Only THAT window is wrong, so request a reseal of it
                 * rather than rebuilding the copy: the reseal recomputes it from
                 * live docs plus the retained events, including this one.
                 */
                if (lateWindows.isEmpty() == false) {
                    logger.warn(
                            "index-digest reason=late_delete_event requesting reseal of {} window(s) for shard copy {} "
                                    + "sealed_watermark={}",
                            lateWindows.size(),
                            key,
                            progress.lastSealedLeafBucketId
                    );
                    requestResealWindows(key, lateWindows, nowMillis);
                }
            }

            /*
             * Late-WRITE detection, symmetric to the late-delete check above: a
             * contract-violating write identifies itself by carrying a NEW
             * seq_no with an OLD external version (below the sealed watermark). Its
             * window is frozen without it, so the only truthful continuation is
             * to reseal that window, folding the write at its proper bucket.
             */
            if (progress.lastSealedLeafBucketId >= 0L && visibleCheckpoint > progress.lastProcessedSeqNo) {
                long sealedBoundaryExclusive = bucketMath.nextBucket(progress.lastSealedLeafBucketId, minLevel);
                Query lateWriteQuery = new BooleanQuery.Builder()
                        .add(
                                LongPoint.newRangeQuery(versionFieldName, 0L, sealedBoundaryExclusive - 1),
                                BooleanClause.Occur.FILTER
                        )
                        .add(
                                LongPoint.newRangeQuery(
                                        SeqNoFieldMapper.NAME,
                                        progress.lastProcessedSeqNo + 1,
                                        visibleCheckpoint
                                ),
                                BooleanClause.Occur.FILTER
                        )
                        .build();
                Set<Long> lateWindows = repairPlanner.windowsNeedingReseal(
                        collectLateWriteVersions(searcher, lateWriteQuery), progress.lastSealedLeafBucketId);
                if (lateWindows.isEmpty() == false) {
                    /*
                     * Each late write corrupts only the window its version falls
                     * in. Reseal those windows; the rest of the tree - however
                     * many years of it - is untouched and stays sealed.
                     */
                    logger.warn(
                            "index-digest reason=late_write requesting reseal of {} window(s) for shard copy {} "
                                    + "sealed_watermark={}",
                            lateWindows.size(),
                            key,
                            progress.lastSealedLeafBucketId
                    );
                    requestResealWindows(key, lateWindows, nowMillis);
                }
            }

            /*
             * The bookmark advances to the visible checkpoint (min of global
             * and refreshed checkpoint), not just the last delete: the snapshot
             * held here covers everything at or below it, so every tombstone in
             * that range has been examined and persisted. This also keeps the
             * retention lease floor tracking write traffic on delete-free
             * workloads instead of pinning history forever.
             */
            if (visibleCheckpoint > progress.lastProcessedSeqNo) {
                long harvestMaxExternalVersion = resolveMaxExternalVersion(searcher);
                progress = progress.withProcessedSeqNo(key, nowMillis, visibleCheckpoint, harvestMaxExternalVersion);
                dao.upsertProgress(progress);
            }

            ensureRetentionLease(shard, key, progress.lastProcessedSeqNo + 1);

            List<DeleteEventDoc> pendingEvents = loadPendingEvents(key, progress);

            int resealed = executePendingReseals(searcher, key, progress, nowMillis);
            if (resealed > 0) {
                // Reseal replaced bucket docs with RefreshPolicy.NONE writes;
                // make them visible to compare/tree readers promptly.
                dao.refreshSystemIndex();
            }

            ObservedVersionRange observedRange = resolveObservedVersionRange(searcher);

            ObservedVersionRange effectiveRange = mergeRanges(observedRange, pendingEvents);
            if (effectiveRange == null) {
                logger.trace(
                        "index-digest skipping shard copy indexUUID={} shardId={} allocationId={} because no external version was found",
                        key.indexUUID,
                        key.shardId,
                        key.allocationId
                );
                return;
            }

            SealedLeafRun run = planSealedLeafRun(progress, effectiveRange.minExternalVersion, effectiveRange.maxExternalVersion);

            if (run.isEmpty()) {
                dao.upsertProgress(progress.withUpdatedHeartbeat(key, nowMillis));
                return;
            }

            processSealedTopWindows(searcher, key, progress, run, effectiveRange.maxExternalVersion, nowMillis, pendingEvents);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Failed scanning index-digest shard copy " + key,
                    e
            );
        }
    }

    /**
     * Loads pending (unfolded) delete events: everything above the sealed
     * watermark boundary. Folded events below it are retained permanently so
     * a reseal can recompute any window losslessly.
     */
    private List<DeleteEventDoc> loadPendingEvents(ShardCopyKey key, ProgressDoc progress) {
        long boundary = progress.lastSealedLeafBucketId < 0L
                ? 0L
                : bucketMath.nextBucket(progress.lastSealedLeafBucketId, minLevel);
        return dao.searchDeleteEventsInRange(key, boundary, Long.MAX_VALUE);
    }

    private static ObservedVersionRange mergeRanges(ObservedVersionRange live, List<DeleteEventDoc> pendingEvents) {
        long min = live == null ? Long.MAX_VALUE : live.minExternalVersion;
        long max = live == null ? Long.MIN_VALUE : live.maxExternalVersion;
        boolean found = live != null;

        for (DeleteEventDoc event : pendingEvents) {
            if (event.eventExternalVersion < min) {
                min = event.eventExternalVersion;
            }
            if (event.eventExternalVersion > max) {
                max = event.eventExternalVersion;
            }
            found = true;
        }

        return found ? new ObservedVersionRange(min, max) : null;
    }

    /**
     * Holds the tombstone-retention floor at the harvest watermark so merges
     * cannot purge unprocessed delete tombstones. Leases are primary-only;
     * the primary's lease is replicated and protects replica copies too. The
     * lease is renewed from the scan itself, never from a separate heartbeat,
     * so a wedged scanner stops renewing and the lease expires instead of
     * pinning history forever.
     */
    private void requestResealWindows(ShardCopyKey key, Set<Long> windows, long nowMillis) {
        List<org.opensearch.indexdigest.model.ResealRequestDoc> requests = new ArrayList<>();
        for (long window : windows) {
            requests.add(org.opensearch.indexdigest.model.ResealRequestDoc.pending(
                    key.indexUUID, key.shardId, key.allocationId, window, nowMillis));
        }
        dao.upsertResealRequests(requests);
    }

    /** External versions of live docs matching the late-write query. */
    private List<Long> collectLateWriteVersions(Engine.Searcher searcher, Query lateWriteQuery) throws IOException {
        List<Long> versions = new ArrayList<>();
        Weight weight = searcher.createWeight(
                searcher.rewrite(lateWriteQuery), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext ctx : searcher.getIndexReader().leaves()) {
            Scorer scorer = weight.scorer(ctx);
            if (scorer == null) {
                continue;
            }
            // The configured version field is indexed with sorted-numeric doc values, like the hasher reads.
            SortedNumericDocValues docValues =
                    DocValues.getSortedNumeric(ctx.reader(), versionFieldName);
            Bits live = ctx.reader().getLiveDocs();
            DocIdSetIterator it = scorer.iterator();
            for (int doc = it.nextDoc();
                 doc != DocIdSetIterator.NO_MORE_DOCS;
                 doc = it.nextDoc()) {
                if (live != null && live.get(doc) == false) {
                    continue;
                }
                if (docValues.advanceExact(doc) && docValues.docValueCount() > 0) {
                    versions.add(docValues.nextValue());
                }
            }
        }
        return versions;
    }

    private long resolveMaxExternalVersion(Engine.Searcher searcher) throws IOException {
        ObservedVersionRange range = resolveObservedVersionRange(searcher);
        return range == null ? -1L : range.maxExternalVersion;
    }

    private void ensureRetentionLease(IndexShard shard, ShardCopyKey key, long retainingSeqNo) {
        if (shard.routingEntry() == null || shard.routingEntry().primary() == false) {
            return;
        }
        /*
         * One lease per replication group (not per allocation): the new primary
         * takes over renewal after relocation/promotion, so no orphaned lease
         * ever pins history for the full expiry period.
         */
        String leaseId = "index-digest";
        long retain = Math.max(0L, retainingSeqNo);
        org.opensearch.index.seqno.RetentionLease existing = shard.getRetentionLeases().get(leaseId);
        if (existing != null) {
            // Never move the floor backwards (a fresh bookmark after a reset
            // starts below the previous floor).
            retain = Math.max(retain, existing.retainingSequenceNumber());
        }
        try {
            shard.renewRetentionLease(leaseId, retain, "index-digest");
        } catch (RetentionLeaseNotFoundException e) {
            try {
                shard.addRetentionLease(
                        leaseId,
                        retain,
                        "index-digest",
                        ActionListener.wrap(
                                (ReplicationResponse r) -> {},
                                ex -> logger.warn("index-digest retention lease sync failed for shard copy {}", key, ex)
                        )
                );
            } catch (RetentionLeaseAlreadyExistsException race) {
                logger.debug("index-digest retention lease add race for shard copy {}", key);
            }
        } catch (Exception e) {
            logger.warn("index-digest could not maintain retention lease for shard copy {}", key, e);
        }
    }

    /**
     * Step 1:
     * Determine a maxLevel-aligned sealed leaf run.
     * <p>
     * - do not process arbitrary leaf ranges
     * - process only full maxLevel bucket windows
     * - convert those maxLevel windows down to minLevel leaf boundaries
     * <p>
     * This gives each run a deterministic sealed window. The forest itself is
     * sparse, so it does not need synthetic empty sibling buckets.
     */
    private SealedLeafRun planSealedLeafRun(
            ProgressDoc progress,
            long minObservedExternalVersion,
            long maxObservedExternalVersion
    ) {
        return new SealedLeafRunPlanner(
                bucketMath,
                minLevel,
                maxLevel,
                maxTopBucketsPerRun,
                guardWindows
        ).plan(progress, minObservedExternalVersion, maxObservedExternalVersion);
    }

    private void processSealedTopWindows(
            Engine.Searcher searcher,
            ShardCopyKey key,
            ProgressDoc initialProgress,
            SealedLeafRun run,
            long maxObservedExternalVersion,
            long nowMillis,
            List<DeleteEventDoc> pendingEvents
    ) throws IOException {
        ProgressDoc progress = initialProgress;
        long topBucket = bucketMath.bucketId(run.startLeafBucketInclusive, maxLevel);
        long endTopBucket = bucketMath.bucketId(run.endLeafBucketInclusive, maxLevel);

        while (topBucket <= endTopBucket) {
            BucketRange leafRange = bucketMath.getBucketBoundaries(topBucket, maxLevel, minLevel);

            long windowStartVersion = topBucket;
            long windowEndVersionExclusive = bucketMath.nextBucket(topBucket, maxLevel);
            List<LeafBucketHasher.DeleteEventEntry> windowEvents = new ArrayList<>();
            for (DeleteEventDoc event : pendingEvents) {
                if (event.eventExternalVersion >= windowStartVersion && event.eventExternalVersion < windowEndVersionExclusive) {
                    windowEvents.add(new LeafBucketHasher.DeleteEventEntry(event.eventExternalVersion, event.eventDocId));
                }
            }

            List<BucketDoc> allBuckets = computeWindowDocs(searcher, key, topBucket, windowEvents, maxObservedExternalVersion);
            if (allBuckets.isEmpty() == false) {
                dao.upsertBuckets(allBuckets);
            }

            progress = progress.advance(key, nowMillis, leafRange.endBucket());
            dao.upsertProgress(progress);
            if (topBucket == endTopBucket) {
                break;
            }
            topBucket = bucketMath.nextBucket(topBucket, maxLevel);
        }
    }

    /**
     * Recomputes one top window's full bucket-doc set (leaves + parents) from
     * live docs and the given delete events. Empty when the window has no
     * content at all.
     */
    private List<BucketDoc> computeWindowDocs(
            Engine.Searcher searcher,
            ShardCopyKey key,
            long topBucket,
            List<LeafBucketHasher.DeleteEventEntry> windowEvents,
            long sealedByMaxExternalVersion
    ) throws IOException {
        BucketRange leafRange = bucketMath.getBucketBoundaries(topBucket, maxLevel, minLevel);
        SealedLeafRun topRun = new SealedLeafRun(leafRange.startBucket(), leafRange.endBucket());

        List<BucketDoc> sealedLeaves = buildSealedLeafBuckets(searcher, key, topRun, sealedByMaxExternalVersion, windowEvents);
        if (sealedLeaves.isEmpty()) {
            return List.of();
        }

        InMemoryForest forest = buildForest(sealedLeaves);
        hashForestBottomUp(key, forest, sealedByMaxExternalVersion);

        List<BucketDoc> allBuckets = new ArrayList<>();
        collectFinalDocs(forest, allBuckets);
        allBuckets.sort(Comparator
                .comparingInt((BucketDoc d) -> d.level)
                .thenComparingLong(d -> d.bucketId));
        return allBuckets;
    }

    /**
     * Executes compare-requested reseals for this copy: recompute the window
     * from current live data + retained delete events, replace its bucket docs
     * (deleting any that no longer exist, e.g. a dissolved shadow's leaf), and
     * mark the request done. Real divergence survives a reseal unchanged; the
     * done marker's cooldown stops the compare path from re-requesting it.
     */
    private int executePendingReseals(
            Engine.Searcher searcher,
            ShardCopyKey key,
            ProgressDoc progress,
            long nowMillis
    ) throws IOException {
        List<org.opensearch.indexdigest.model.ResealRequestDoc> pending = dao.searchPendingResealRequests(key);
        if (pending.isEmpty()) {
            return 0;
        }

        int executed = 0;
        for (org.opensearch.indexdigest.model.ResealRequestDoc request : pending) {
            if (executed >= MAX_RESEALS_PER_TICK) {
                break;
            }
            BucketRange leafRange = bucketMath.getBucketBoundaries(request.windowBucketId, maxLevel, minLevel);
            if (leafRange.endBucket() > progress.lastSealedLeafBucketId) {
                // Window not sealed on this copy yet; the regular sealing path
                // owns it. Leave the request pending.
                continue;
            }

            try {
                executeReseal(searcher, key, progress, request, leafRange);
            } catch (Exception e) {
                // A reseal must never wedge the copy's regular sealing (e.g. a
                // dense window tripping the read fail-fast). Give up on this
                // request; the mismatch stays visible in compare, and the
                // cooldown allows a future retry.
                logger.warn(
                        "index-digest reseal failed for window {} of shard copy {}; giving up on this request",
                        request.windowBucketId,
                        key,
                        e
                );
            }
            dao.upsertResealRequest(request.done(nowMillis));
            executed++;
        }
        return executed;
    }

    private void executeReseal(
            Engine.Searcher searcher,
            ShardCopyKey key,
            ProgressDoc progress,
            org.opensearch.indexdigest.model.ResealRequestDoc request,
            BucketRange leafRange
    ) throws IOException {
        long windowEndExclusive = bucketMath.nextBucket(request.windowBucketId, maxLevel);
        List<DeleteEventDoc> events =
                dao.searchDeleteEventsInRange(key, request.windowBucketId, windowEndExclusive);
        List<LeafBucketHasher.DeleteEventEntry> eventEntries = new ArrayList<>(events.size());
        for (DeleteEventDoc event : events) {
            eventEntries.add(new LeafBucketHasher.DeleteEventEntry(event.eventExternalVersion, event.eventDocId));
        }

        long sealedByMarker = bucketMath.nextBucket(progress.lastSealedLeafBucketId, minLevel);
        List<BucketDoc> newDocs = computeWindowDocs(searcher, key, request.windowBucketId, eventEntries, sealedByMarker);

        Set<String> newIds = new java.util.HashSet<>();
        for (BucketDoc doc : newDocs) {
            newIds.add(doc.id);
        }
        List<BucketDoc> existing = dao.searchBucketDocsForSubtree(
                key.indexUUID,
                key.shardId,
                key.allocationId,
                minLevel,
                maxLevel,
                leafRange.startBucket(),
                leafRange.endBucket()
        );
        List<String> staleIds = new ArrayList<>();
        for (BucketDoc doc : existing) {
            if (newIds.contains(doc.id) == false) {
                staleIds.add(doc.id);
            }
        }

        // Upsert first, delete stale after: a concurrent reader between the two
        // sees extra docs (harmless duplicates at worst), never a missing root.
        if (newDocs.isEmpty() == false) {
            dao.upsertBuckets(newDocs);
        }
        dao.deleteDocsByIds(staleIds);

        logger.warn(
                "index-digest reason=reseal window={} shard copy {} new_docs={} stale_docs={}",
                request.windowBucketId,
                key,
                newDocs.size(),
                staleIds.size()
        );
    }

    /**
     * Step 1b:
     * Materialize sealed leaf buckets that have at least one live document
     * or delete event.
     */
    private List<BucketDoc> buildSealedLeafBuckets(
            Engine.Searcher searcher,
            ShardCopyKey key,
            SealedLeafRun run,
            long sealedByMaxExternalVersion,
            List<LeafBucketHasher.DeleteEventEntry> deleteEvents
    ) throws IOException {
        List<LeafBucketDigest> leafDigests = leafBucketHasher.computeRun(
                searcher,
                minLevel,
                run.startLeafBucketInclusive,
                run.endLeafBucketInclusive,
                deleteEvents
        );
        List<BucketDoc> out = new ArrayList<>(leafDigests.size());
        for (LeafBucketDigest leafDigest : leafDigests) {
            out.add(BucketDoc.leaf(key, leafDigest, sealedByMaxExternalVersion));
        }
        return out;
    }


    /**
     * Step 2:
     * Build a real in-memory forest with parent/children links.
     * <p>
     * No hashing here.
     * No persistence here.
     * <p>
     * Important:
     * This is intentionally sparse. Parent buckets are built from the child
     * buckets that actually exist in this sealed run.
     */
    private InMemoryForest buildForest(List<BucketDoc> sealedLeaves) {
        InMemoryForest forest = new InMemoryForest();
        List<TreeNode> currentLevelNodes = new ArrayList<>(sealedLeaves.size());

        for (BucketDoc leaf : sealedLeaves) {
            TreeNode node = TreeNode.leaf(leaf);
            currentLevelNodes.add(node);
            forest.allNodes.add(node);
        }

        forest.leafNodes.addAll(currentLevelNodes);

        for (int level = minLevel + 1; level <= maxLevel; level++) {
            if (currentLevelNodes.isEmpty()) {
                break;
            }

            Map<Long, TreeNode> parentBucketToNode = new TreeMap<>();
            for (TreeNode child : currentLevelNodes) {
                long parentBucketId = bucketMath.bucketId(child.bucketId, level);

                TreeNode parent = parentBucketToNode.get(parentBucketId);
                if (parent == null) {
                    parent = TreeNode.parent(level, parentBucketId);
                    parentBucketToNode.put(parentBucketId, parent);
                    forest.allNodes.add(parent);
                }

                parent.addChild(child);
                child.parent = parent;
                parent.docCount += child.docCount;
            }

            currentLevelNodes = new ArrayList<>(parentBucketToNode.values());
        }

        forest.roots.addAll(currentLevelNodes);
        forest.roots.sort(
                Comparator.comparingInt((TreeNode n) -> n.level)
                        .thenComparingLong(n -> n.bucketId)
        );

        return forest;
    }

    /**
     * Step 3:
     * Hash the forest recursively bottom-up.
     * <p>
     * Leaves are already hashed.
     * Parents are reduced from their already-finalized children.
     */
    private void hashForestBottomUp(
            ShardCopyKey key,
            InMemoryForest forest,
            long sealedByMaxExternalVersion
    ) {
        for (TreeNode root : forest.roots) {
            hashNodeRecursive(key, root, sealedByMaxExternalVersion);
        }
    }

    private BucketDoc hashNodeRecursive(
            ShardCopyKey key,
            TreeNode node,
            long sealedByMaxExternalVersion
    ) {
        if (node.finalDoc != null) {
            return node.finalDoc;
        }

        if (node.children.isEmpty()) {
            if (node.leafDoc == null) {
                throw new IllegalStateException(
                        "Leaf node missing leafDoc. level=" + node.level + " bucketId=" + node.bucketId
                );
            }
            node.finalDoc = node.leafDoc;
            return node.finalDoc;
        }

        node.children.sort(Comparator.comparingLong(c -> c.bucketId));

        List<BucketDoc> childDocs = new ArrayList<>(node.children.size());
        for (TreeNode child : node.children) {
            childDocs.add(hashNodeRecursive(key, child, sealedByMaxExternalVersion));
        }

        node.finalDoc = bucketReducer.reduce(
                key,
                node.level,
                node.bucketId,
                childDocs,
                sealedByMaxExternalVersion
        );

        return node.finalDoc;
    }

    /**
     * Step 4:
     * Flatten final docs from the already-hashed forest.
     * <p>
     * Post-order traversal:
     * children first, then parent.
     */
    private void collectFinalDocs(InMemoryForest forest, List<BucketDoc> out) {
        for (TreeNode root : forest.roots) {
            collectFinalDocsRecursive(root, out);
        }
    }

    private void collectFinalDocsRecursive(TreeNode node, List<BucketDoc> out) {
        node.children.sort(Comparator.comparingLong(c -> c.bucketId));
        for (TreeNode child : node.children) {
            collectFinalDocsRecursive(child, out);
        }

        if (node.finalDoc == null) {
            throw new IllegalStateException(
                    "Node must be hashed before collecting final docs. level="
                            + node.level + " bucketId=" + node.bucketId
            );
        }

        out.add(node.finalDoc);
    }

    private ObservedVersionRange resolveObservedVersionRange(Engine.Searcher searcher) throws IOException {
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        boolean found = false;

        for (LeafReaderContext ctx : searcher.getDirectoryReader().leaves()) {
            PointValues values = ctx.reader().getPointValues(versionFieldName);
            if (values == null || values.size() == 0L) {
                continue;
            }

            if (values.getNumIndexDimensions() != 1 || values.getBytesPerDimension() != Long.BYTES) {
                throw new IllegalStateException(
                        "index-digest requires single-dimension long points for field [" + versionFieldName + "]"
                );
            }

            long segmentMin = LongPoint.decodeDimension(values.getMinPackedValue(), 0);
            long segmentMax = LongPoint.decodeDimension(values.getMaxPackedValue(), 0);
            validateExternalVersion(segmentMin);
            validateExternalVersion(segmentMax);

            if (segmentMin < min) {
                min = segmentMin;
            }
            if (segmentMax > max) {
                max = segmentMax;
            }
            found = true;
        }

        return found ? new ObservedVersionRange(min, max) : null;
    }

    private void validateBucketLevels() {
        if (minLevel > maxLevel) {
            throw new IllegalStateException(
                    "index-digest requires min_level <= max_level but found min_level="
                            + minLevel + " max_level=" + maxLevel
            );
        }
    }

    private boolean needsReset(ProgressDoc progress) {
        return progress.minLevel != minLevel
                || progress.maxLevel != maxLevel
                || progress.formatVersion != FORMAT_VERSION;
    }

    private void validateExternalVersion(long externalVersion) {
        if (externalVersion < 0L) {
            throw new IllegalStateException(
                    "index-digest requires non-negative [" + versionFieldName + "] but found [" + externalVersion + "]"
            );
        }
    }

    private void validateVersionFieldMapping(IndexShard shard) {
        MappedFieldType fieldType = shard.mapperService().fieldType(versionFieldName);
        if (fieldType == null) {
            throw new IllegalStateException(
                    "index-digest requires mapped field [" + versionFieldName + "]"
            );
        }
        if ("long".equals(fieldType.typeName()) == false) {
            throw new IllegalStateException(
                    "index-digest requires field [" + versionFieldName + "] to be mapped as [long] but found ["
                            + fieldType.typeName() + "]"
            );
        }
        if (fieldType.isSearchable() == false) {
            throw new IllegalStateException(
                    "index-digest requires indexed field [" + versionFieldName + "] because scans use range queries"
            );
        }
        if (fieldType.hasDocValues() == false) {
            throw new IllegalStateException(
                    "index-digest requires doc values on field [" + versionFieldName + "]"
            );
        }
    }

    private static final class ObservedVersionRange {
        final long minExternalVersion;
        final long maxExternalVersion;

        private ObservedVersionRange(long minExternalVersion, long maxExternalVersion) {
            this.minExternalVersion = minExternalVersion;
            this.maxExternalVersion = maxExternalVersion;
        }
    }

    private static final class InMemoryForest {
        final List<TreeNode> roots = new ArrayList<>();
        final List<TreeNode> leafNodes = new ArrayList<>();
        final List<TreeNode> allNodes = new ArrayList<>();
    }

    private static final class TreeNode {
        final int level;
        final long bucketId;
        long docCount;

        TreeNode parent;
        final List<TreeNode> children;

        final BucketDoc leafDoc;
        BucketDoc finalDoc;

        private TreeNode(
                int level,
                long bucketId,
                long docCount,
                BucketDoc leafDoc
        ) {
            this.level = level;
            this.bucketId = bucketId;
            this.docCount = docCount;
            this.leafDoc = leafDoc;
            this.children = new ArrayList<>();
        }

        static TreeNode leaf(BucketDoc leafDoc) {
            return new TreeNode(
                    leafDoc.level,
                    leafDoc.bucketId,
                    leafDoc.docCount,
                    leafDoc
            );
        }

        static TreeNode parent(int level, long bucketId) {
            return new TreeNode(level, bucketId, 0L, null);
        }

        void addChild(TreeNode child) {
            children.add(child);
        }
    }


    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }
}
