/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.indexdigest.bucket.BucketMath;
import org.opensearch.indexdigest.bucket.BucketRange;
import org.opensearch.indexdigest.client.PluginClient;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.model.ProgressDoc;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.indexdigest.persistence.IndexDigestIndexDao;
import org.opensearch.indexdigest.scan.ScanRunResult;
import org.opensearch.indexdigest.scan.ScanScheduler;
import org.opensearch.indexdigest.scan.ShardCopyResolver;
import org.opensearch.indexdigest.scan.ShardScanner;
import org.opensearch.indexdigest.tree.IndexDigestCompareRequest;
import org.opensearch.indexdigest.tree.IndexDigestCompareResult;
import org.opensearch.indexdigest.tree.IndexDigestDiffer;
import org.opensearch.indexdigest.tree.TreeBuilder;
import org.opensearch.indexdigest.tree.TreeRequest;
import org.opensearch.indexdigest.tree.TreeView;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.function.Consumer;

@Singleton
public class IndexDigestService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(IndexDigestService.class);

    private final IndexDigestIndexDao dao;
    private final ShardScanner scanner;
    private final ShardCopyResolver resolver;
    private final ScanScheduler scheduler;

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final BucketMath bucketMath;
    private final TreeBuilder treeBuilder;

    private final int minLevel;
    private final int maxLevel;
    private volatile boolean autoReseal;

    @Inject
    public IndexDigestService(
            ClusterService clusterService,
            ThreadPool threadPool,
            IndicesService indicesService,
            PluginClient pluginClient,
            Environment environment
    ) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;

        Settings settings = environment.settings();

        this.minLevel = IndexDigestSettings.MIN_LEVEL.get(settings);
        this.maxLevel = IndexDigestSettings.MAX_LEVEL.get(settings);
        this.autoReseal = IndexDigestSettings.AUTO_RESEAL.get(settings);
        clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(IndexDigestSettings.AUTO_RESEAL, v -> this.autoReseal = v);

        BucketMath bucketMath = BucketMath.INSTANCE;
        String versionFieldName = "version_ts";

        this.bucketMath = bucketMath;
        this.treeBuilder = new TreeBuilder(bucketMath);

        this.dao = new IndexDigestIndexDao(pluginClient, threadPool);

        this.scanner = new ShardScanner(
                indicesService,
                dao,
                clusterService.getClusterSettings(),
                settings,
                bucketMath,
                versionFieldName,
                threadPool
        );

        this.resolver = new ShardCopyResolver(
                clusterService.getClusterSettings(),
                settings,
                indicesService
        );

        this.scheduler = new ScanScheduler(
                threadPool,
                clusterService.getClusterSettings(),
                settings,
                resolver,
                scanner
        );
    }

    public IndexDigestStatus status() {
        return new IndexDigestStatus(
                scheduler.isRunning(),
                scheduler.getLastRunEpochMillis(),
                scheduler.getLastErrorMessage(),
                scheduler.getLastSkippedInFlightShardCopies()
        );
    }

    public ScanRunResult triggerScanOnce() {
        int failureCount = 0;
        int attemptedCount = 0;
        int skippedInFlightCount = 0;
        StringBuilder failures = new StringBuilder();
        for (ShardCopyKey key : resolver.resolveLocalShardCopies()) {
            try {
                if (scanner.scanShardCopy(key)) {
                    attemptedCount++;
                } else {
                    skippedInFlightCount++;
                }
            } catch (Exception e) {
                failureCount++;
                appendFailure(failures, key, e);
                logger.warn("index-digest manual scan failed for shard copy {}", key, e);
            }
        }
        if (failureCount > 0) {
            throw new RuntimeException(
                    "index-digest scan failed for " + failureCount + " shard copies: " + failures
            );
        }
        return new ScanRunResult(attemptedCount, skippedInFlightCount);
    }

    private static void appendFailure(StringBuilder failures, ShardCopyKey key, Exception e) {
        if (failures.length() > 0) {
            failures.append("; ");
        }
        failures.append(key).append(": ").append(e.getMessage() == null ? e.toString() : e.getMessage());
    }

    public void triggerScanOnceAsync(Consumer<ScanRunResult> onSuccess, Consumer<Exception> onFailure) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            final ScanRunResult result;
            try {
                result = triggerScanOnce();
            } catch (Exception e) {
                onFailure.accept(e);
                return;
            }
            // Outside the try: a failure while sending the success response must
            // not trigger a second response on the same channel via onFailure.
            onSuccess.accept(result);
        });
    }

    public void treeViewAsync(
            TreeRequest request,
            Consumer<TreeView> onSuccess,
            Consumer<Exception> onFailure
    ) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            final TreeView view;
            try {
                validateTreeRequest(request);
                String indexUuid = resolveIndexUuid(request.index);
                ShardCopyKey key = new ShardCopyKey(indexUuid, request.shardId, request.allocationId);
                ProgressDoc progress = dao.getProgress(key);
                requireCompatibleProgressLevels(key, progress);
                dao.refreshSystemIndex();

                BucketRange leafRange = bucketMath.getBucketBoundaries(
                        request.rootBucket,
                        request.rootLevel,
                        minLevel
                );
                int fetchMinLevel = Math.max(minLevel, request.rootLevel - request.depth);
                boolean truncatedByFetch = fetchMinLevel > minLevel;

                List<BucketDoc> docs = dao.searchBucketDocsForSubtree(
                        indexUuid,
                        request.shardId,
                        request.allocationId,
                        fetchMinLevel,
                        request.rootLevel,
                        leafRange.startBucket(),
                        leafRange.endBucket()
                );

                view = treeBuilder.build(request, indexUuid, docs, truncatedByFetch);
            } catch (Exception e) {
                onFailure.accept(e);
                return;
            }
            // Outside the try: a failure while sending the success response must
            // not trigger a second response on the same channel via onFailure.
            onSuccess.accept(view);
        });
    }

    public void compareAsync(
            IndexDigestCompareRequest request,
            Consumer<IndexDigestCompareResult> onSuccess,
            Consumer<Exception> onFailure
    ) {
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            IndexDigestCompareResult result;
            try {
                String leftIndexUuid = resolveIndexUuid(request.leftIndex);
                String rightIndexUuid = resolveIndexUuid(request.rightIndex);

                ShardCopyKey leftKey = new ShardCopyKey(leftIndexUuid, request.leftShardId, request.leftAllocationId);
                ShardCopyKey rightKey = new ShardCopyKey(rightIndexUuid, request.rightShardId, request.rightAllocationId);

                ProgressDoc leftProgress = dao.getProgress(leftKey);
                ProgressDoc rightProgress = dao.getProgress(rightKey);

                /*
                 * Keep this order: realtime progress GET first, then refresh bucket searches.
                 * The scanner writes bucket docs before progress, so any progress watermark
                 * observed here has already had its bucket writes submitted.
                 */
                dao.refreshSystemIndex();

                IndexDigestCompareResult.Side left = side(
                        request.leftIndex,
                        leftIndexUuid,
                        request.leftShardId,
                        request.leftAllocationId,
                        leftProgress
                );
                IndexDigestCompareResult.Side right = side(
                        request.rightIndex,
                        rightIndexUuid,
                        request.rightShardId,
                        request.rightAllocationId,
                        rightProgress
                );

                /*
                 * Read paths never mutate derived data: incompatible progress is only
                 * reported here, and the owning node's scanner (under its in-flight
                 * guard) is the single actor that resets and rebuilds it. Level
                 * compatibility is judged from the persisted progress of the two
                 * sides against each other, not against this coordinating node's
                 * settings, so compare works regardless of which node serves it.
                 */
                CompareLevels levels = resolveCompareLevels(leftProgress, rightProgress, minLevel, maxLevel);
                int rootLevel = request.rootLevel == IndexDigestCompareRequest.DEFAULT_ROOT_LEVEL
                        ? levels.maxLevel
                        : request.rootLevel;

                if (levels.incompatible) {
                    result = IndexDigestCompareResult.notComparable(
                            "incompatible_progress_levels",
                            levels.minLevel,
                            rootLevel,
                            request.maxMismatches,
                            Math.min(left.lastSealedLeafBucketId, right.lastSealedLeafBucketId),
                            left,
                            right
                    );
                } else {
                    validateCompareLevels(rootLevel, levels.minLevel, levels.maxLevel);

                    IndexDigestDiffer differ = new IndexDigestDiffer(bucketMath, levels.minLevel, levels.maxLevel);
                    result = differ.compare(
                            left,
                            right,
                            rootLevel,
                            request.maxMismatches,
                            (level, start, end) -> dao.searchBucketDocsAtLevel(
                                    leftIndexUuid,
                                    request.leftShardId,
                                    request.leftAllocationId,
                                    level,
                                    start,
                                    end
                            ),
                            (level, start, end) -> dao.searchBucketDocsAtLevel(
                                    rightIndexUuid,
                                    request.rightShardId,
                                    request.rightAllocationId,
                                    level,
                                    start,
                                    end
                            )
                    );

                    if (autoReseal && result.equal == false && result.mismatches.isEmpty() == false) {
                        List<Long> requested = requestResealsForMismatches(
                                result,
                                levels.maxLevel,
                                leftIndexUuid,
                                request.leftShardId,
                                request.leftAllocationId,
                                rightIndexUuid,
                                request.rightShardId,
                                request.rightAllocationId
                        );
                        if (requested.isEmpty() == false) {
                            result = result.withResealRequestedWindows(requested);
                        }
                    }
                }
            } catch (Exception e) {
                onFailure.accept(e);
                return;
            }
            // Outside the try: a failure while sending the success response must
            // not trigger a second response on the same channel via onFailure.
            onSuccess.accept(result);
        });
    }

    /**
     * Effective levels for a compare, resolved from the two sides' persisted
     * progress against EACH OTHER (never against this coordinating node's
     * settings), so compare behaves identically on every node. Legacy progress
     * without level fields (-1) is incompatible: its digests were built under an
     * unknown configuration. Missing progress falls back to node defaults; the
     * differ short-circuits to missing_progress before levels matter.
     */
    static CompareLevels resolveCompareLevels(
            ProgressDoc left,
            ProgressDoc right,
            int fallbackMinLevel,
            int fallbackMaxLevel
    ) {
        boolean bothFound = left != null && right != null;
        if (bothFound == false) {
            return new CompareLevels(false, fallbackMinLevel, fallbackMaxLevel);
        }
        boolean incompatible = left.minLevel < 0
                || left.maxLevel < 0
                || right.minLevel < 0
                || right.maxLevel < 0
                || left.minLevel != right.minLevel
                || left.maxLevel != right.maxLevel
                || left.formatVersion != right.formatVersion;
        if (incompatible) {
            return new CompareLevels(true, fallbackMinLevel, fallbackMaxLevel);
        }
        return new CompareLevels(false, left.minLevel, left.maxLevel);
    }

    static final class CompareLevels {
        final boolean incompatible;
        final int minLevel;
        final int maxLevel;

        CompareLevels(boolean incompatible, int minLevel, int maxLevel) {
            this.incompatible = incompatible;
            this.minLevel = minLevel;
            this.maxLevel = maxLevel;
        }
    }

    private static final int MAX_RESEAL_REQUESTS_PER_COMPARE = 8;
    private static final long RESEAL_COOLDOWN_MILLIS = 10 * 60 * 1000L;

    /**
     * Automatic repair trigger: every mismatch below the common watermark gets
     * a reseal request for BOTH sides' containing top windows, executed by the
     * owning nodes' scanners on their next tick. Timing artifacts (seal-race
     * shadows) dissolve on reseal; real divergence survives it, and the done
     * marker's cooldown stops confirmed-real mismatches from being re-requested
     * in a loop.
     */
    private List<Long> requestResealsForMismatches(
            IndexDigestCompareResult result,
            int effectiveMaxLevel,
            String leftIndexUuid,
            int leftShardId,
            String leftAllocationId,
            String rightIndexUuid,
            int rightShardId,
            String rightAllocationId
    ) {
        long nowMillis = threadPool.absoluteTimeInMillis();
        java.util.LinkedHashSet<Long> windows = new java.util.LinkedHashSet<>();
        for (IndexDigestCompareResult.Mismatch mismatch : result.mismatches) {
            windows.add(bucketMath.bucketId(mismatch.bucketId, effectiveMaxLevel));
            if (windows.size() >= MAX_RESEAL_REQUESTS_PER_COMPARE) {
                break;
            }
        }

        List<Long> requested = new java.util.ArrayList<>();
        List<org.opensearch.indexdigest.model.ResealRequestDoc> toCreate = new java.util.ArrayList<>();
        for (long window : windows) {
            boolean leftRequested =
                    maybeRequestReseal(leftIndexUuid, leftShardId, leftAllocationId, window, nowMillis, toCreate);
            boolean rightRequested =
                    maybeRequestReseal(rightIndexUuid, rightShardId, rightAllocationId, window, nowMillis, toCreate);
            if (leftRequested || rightRequested) {
                requested.add(window);
            }
        }
        // One bulk + one refresh instead of per-request refresh amplification.
        dao.upsertResealRequests(toCreate);
        return requested;
    }

    private boolean maybeRequestReseal(
            String indexUuid,
            int shardId,
            String allocationId,
            long window,
            long nowMillis,
            List<org.opensearch.indexdigest.model.ResealRequestDoc> toCreate
    ) {
        org.opensearch.indexdigest.model.ResealRequestDoc existing =
                dao.getResealRequest(indexUuid, shardId, allocationId, window);
        if (existing == null) {
            toCreate.add(
                    org.opensearch.indexdigest.model.ResealRequestDoc.pending(indexUuid, shardId, allocationId, window, nowMillis)
            );
            return true;
        }
        if (org.opensearch.indexdigest.model.ResealRequestDoc.STATUS_PENDING.equals(existing.status)) {
            // In flight: healing already scheduled; report it.
            return true;
        }
        if (nowMillis - existing.resealedAtEpochMillis > RESEAL_COOLDOWN_MILLIS) {
            toCreate.add(
                    org.opensearch.indexdigest.model.ResealRequestDoc.pending(indexUuid, shardId, allocationId, window, nowMillis)
            );
            return true;
        }
        // Recently resealed and still mismatched: the divergence is real, not a
        // timing artifact. Do not loop.
        return false;
    }

    private static void validateCompareLevels(int rootLevel, int effectiveMinLevel, int effectiveMaxLevel) {
        if (rootLevel < effectiveMinLevel) {
            throw new IllegalArgumentException("[root_level] must be >= " + effectiveMinLevel);
        }
        if (rootLevel > effectiveMaxLevel) {
            throw new IllegalArgumentException("[root_level] must be <= " + effectiveMaxLevel);
        }
    }

    private void validateTreeRequest(TreeRequest request) {
        if (request.shardId < 0) {
            throw new IllegalArgumentException("[shard_id] must be >= 0");
        }
        if (request.rootLevel < minLevel) {
            throw new IllegalArgumentException("[root_level] must be >= " + minLevel);
        }
        if (request.rootLevel > maxLevel) {
            throw new IllegalArgumentException("[root_level] must be <= " + maxLevel);
        }
        if (request.rootBucket < 0L) {
            throw new IllegalArgumentException("[root_bucket] must be >= 0");
        }
        if (bucketMath.bucketId(request.rootBucket, request.rootLevel) != request.rootBucket) {
            throw new IllegalArgumentException(
                    "[root_bucket] must be aligned to [root_level] " + request.rootLevel
            );
        }
    }

    /**
     * Read paths never mutate derived data. Incompatible progress fails loudly
     * with the machine-readable reason; the owning node's scanner (under its
     * in-flight guard) is the single actor that resets and rebuilds it.
     */
    private void requireCompatibleProgressLevels(ShardCopyKey key, ProgressDoc progress) {
        if (hasIncompatibleProgressLevels(progress) == false) {
            return;
        }
        throw new IllegalStateException(
                "incompatible_progress_levels: index-digest derived data for shard copy " + key
                        + " was built with min_level=" + progress.minLevel
                        + " max_level=" + progress.maxLevel
                        + " but this node requires min_level=" + minLevel
                        + " max_level=" + maxLevel
                        + "; the owning node's next scan resets and rebuilds it"
        );
    }

    private boolean hasIncompatibleProgressLevels(ProgressDoc progress) {
        return progress != null && (progress.minLevel != minLevel || progress.maxLevel != maxLevel);
    }

    private static IndexDigestCompareResult.Side side(
            String index,
            String indexUuid,
            int shardId,
            String allocationId,
            ProgressDoc progress
    ) {
        return new IndexDigestCompareResult.Side(
                index,
                indexUuid,
                shardId,
                allocationId,
                progress != null,
                progress == null ? -1L : progress.lastSealedLeafBucketId,
                progress == null ? 0L : progress.lastSeenEpochMillis,
                progress == null ? -1 : progress.minLevel,
                progress == null ? -1 : progress.maxLevel,
                progress == null ? -1 : progress.formatVersion
        );
    }

    private String resolveIndexUuid(String indexName) {
        IndexMetadata metadata = clusterService.state().metadata().index(indexName);
        if (metadata == null) {
            throw new IllegalArgumentException("Index not found: " + indexName);
        }
        return metadata.getIndexUUID();
    }

    @Override
    protected void doStart() {
        scheduler.start();
    }

    @Override
    protected void doStop() {
        scheduler.close();
    }

    @Override
    protected void doClose() {
        scheduler.close();
    }
}
