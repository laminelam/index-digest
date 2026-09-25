/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.ResourceAlreadyExistsException;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.indexdigest.client.PluginClient;
import org.opensearch.indexdigest.model.BucketDoc;
import org.opensearch.indexdigest.model.DeleteEventDoc;
import org.opensearch.indexdigest.model.ProgressDoc;
import org.opensearch.indexdigest.model.ResealRequestDoc;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.index.reindex.DeleteByQueryAction;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Persistence layer for .index_digest.
 *
 * Production notes:
 * - The system index may be deleted externally.
 * - indexEnsured is only an optimization, not a correctness guarantee.
 * - Every operation retries once after IndexNotFoundException.
 * - Index creation is idempotent and handles races.
 */
public final class IndexDigestIndexDao {
    private static final Logger logger = LogManager.getLogger(IndexDigestIndexDao.class);
    private static final int MAX_TREE_BUCKET_DOCS = 10_000;
    private static final int MAX_BUCKET_DOCS_PER_LEVEL_RANGE = 10_000;
    private static final String MAPPING_DRIFT_MESSAGE =
            "index-digest system index mapping is incompatible; delete and rebuild " + IndexDigestSystemIndex.INDEX_NAME;

    private static final long MAPPING_VERIFY_INTERVAL_MILLIS = 10_000;

    private final PluginClient pluginClient;
    private final ThreadPool threadPool;
    private final AtomicBoolean indexEnsured = new AtomicBoolean(false);
    private final AtomicBoolean systemIndexRecreated = new AtomicBoolean(false);
    private volatile long lastMappingVerifyEpochMillis = 0L;

    public IndexDigestIndexDao(PluginClient pluginClient, ThreadPool threadPool) {
        this.pluginClient = pluginClient;
        this.threadPool = threadPool;
    }

    public ProgressDoc getProgress(ShardCopyKey key) {
        return withSystemIndexRetry(() -> {
            String id = IndexDigestIds.progressId(key.indexUUID, key.shardId, key.allocationId);

            GetResponse response = pluginClient.client()
                    .get(new GetRequest(IndexDigestSystemIndex.INDEX_NAME, id))
                    .actionGet();

            if (response.isExists() == false) {
                return null;
            }

            return ProgressDoc.fromSource(id, response.getSourceAsMap());
        });
    }

    public void upsertProgress(ProgressDoc doc) {
        withSystemIndexRetry(() -> {
            IndexRequest req = new IndexRequest(IndexDigestSystemIndex.INDEX_NAME)
                    .id(doc.id)
                    .source(doc.toSource())
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);

            pluginClient.client().index(req).actionGet();
            maybeVerifySystemIndexMapping();
            return null;
        });
    }

    public void upsertBucket(BucketDoc doc) {
        withSystemIndexRetry(() -> {
            IndexRequest req = new IndexRequest(IndexDigestSystemIndex.INDEX_NAME)
                    .id(doc.id)
                    .source(doc.toSource())
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);

            pluginClient.client().index(req).actionGet();
            maybeVerifySystemIndexMapping();
            return null;
        });
    }

    public void refreshSystemIndex() {
        withSystemIndexRetry(() -> {
            pluginClient.client().admin().indices()
                    .refresh(new RefreshRequest(IndexDigestSystemIndex.INDEX_NAME))
                    .actionGet();
            return null;
        });
    }

    public void upsertBuckets(List<BucketDoc> docs) {
        if (docs == null || docs.isEmpty()) return;

        final int MAX_ACTIONS = 1000;
        final long MAX_BYTES = 5 * 1024 * 1024; // 5MB

        long t0 = System.nanoTime();
        withSystemIndexRetry(() -> {
            BulkRequest bulk = new BulkRequest();
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);

            int actions = 0;
            long bytes = 0;

            for (BucketDoc doc : docs) {
                IndexRequest req = new IndexRequest(IndexDigestSystemIndex.INDEX_NAME)
                        .id(doc.id)
                        .source(doc.toSource());

                bulk.add(req);

                actions++;
                bytes += req.source().length(); // rough size

                if (actions >= MAX_ACTIONS || bytes >= MAX_BYTES) {
                    executeBulk(bulk);
                    bulk = new BulkRequest();
                    bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                    actions = 0;
                    bytes = 0;
                }
            }

            if (actions > 0) {
                executeBulk(bulk);
            }

            maybeVerifySystemIndexMapping();
            return null;
        });
        logger.debug(
                "index-digest step=upsertBuckets total_docs={} total_ms={}",
                docs.size(),
                elapsedMs(t0)
        );
    }

    private void executeBulk(BulkRequest bulk) {
        throwIfBulkFailures(pluginClient.client().bulk(bulk).actionGet());
    }

    // Package-private for tests. Preserves the typed item-failure cause so the
    // IndexNotFoundException retry in withSystemIndexRetry stays reachable.
    static void throwIfBulkFailures(BulkResponse resp) {
        if (resp.hasFailures()) {
            for (BulkItemResponse item : resp.getItems()) {
                if (item.isFailed()) {
                    BulkItemResponse.Failure failure = item.getFailure();
                    throw new RuntimeException(
                            "Bulk failure writing index-digest system index: " + failure.getMessage(),
                            failure.getCause()
                    );
                }
            }
            throw new RuntimeException("Bulk failure writing index-digest system index: " + resp.buildFailureMessage());
        }
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private <T> T withSystemIndexRetry(SystemIndexOperation<T> op) {
        try {
            ensureSystemIndex();
            return pluginClient.runAsResult(op::run);
        } catch (RuntimeException e) {
            if (isIndexNotFound(e) == false) {
                throw e;
            }

            logger.warn(
                    "index-digest system index {} disappeared or was missing; recreating and retrying once",
                    IndexDigestSystemIndex.INDEX_NAME
            );

            invalidateSystemIndexCache();
            ensureSystemIndex();
            // The scanner consumes this at tick end: derived state written after
            // a mid-tick recreation must not survive as a watermark over an
            // otherwise empty store.
            systemIndexRecreated.set(true);

            return pluginClient.runAsResult(op::run);
        }
    }

    private void ensureSystemIndex() {
        if (indexEnsured.get()) {
            return;
        }

        synchronized (indexEnsured) {
            if (indexEnsured.get()) {
                return;
            }

            pluginClient.runAs(() -> {
                boolean exists = pluginClient.client().admin().indices()
                        .exists(new IndicesExistsRequest(IndexDigestSystemIndex.INDEX_NAME))
                        .actionGet()
                        .isExists();

                if (exists) {
                    updateSystemIndexMapping();
                    verifySystemIndexMapping();
                    indexEnsured.set(true);
                    return;
                }

                try {
                    CreateIndexRequest create = new CreateIndexRequest(IndexDigestSystemIndex.INDEX_NAME)
                            .settings(systemIndexSettings());

                    create.mapping(systemIndexMapping());

                    pluginClient.client().admin().indices().create(create).actionGet();
                    logger.info("index-digest created system index {}", IndexDigestSystemIndex.INDEX_NAME);
                } catch (ResourceAlreadyExistsException e) {
                    logger.debug("index-digest system index already exists after create race", e);
                    updateSystemIndexMapping();
                    verifySystemIndexMapping();
                }

                indexEnsured.set(true);
            });
        }
    }

    private Settings systemIndexSettings() {
        return Settings.builder()
                .put("index.hidden", true)
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.auto_expand_replicas", "0-1")
                .build();
    }

    // Package-private for tests: the canonical mapping must always pass verifyMappingProperties.
    static org.opensearch.core.xcontent.XContentBuilder systemIndexMapping() throws java.io.IOException {
        return jsonBuilder()
                .startObject()
                .startObject("properties")

                .startObject("type").field("type", "keyword").endObject()
                .startObject("index_uuid").field("type", "keyword").endObject()
                .startObject("shard_id").field("type", "integer").endObject()
                .startObject("allocation_id").field("type", "keyword").endObject()
                .startObject("node_id").field("type", "keyword").endObject()
                .startObject("shard_role").field("type", "keyword").endObject()
                .startObject("scan_mode").field("type", "keyword").endObject()
                .startObject("min_level").field("type", "integer").endObject()
                .startObject("max_level").field("type", "integer").endObject()

                .startObject("last_seen_epoch_millis").field("type", "date").field("format", "epoch_millis").endObject()
                .startObject("last_sealed_leaf_bucket_id").field("type", "long").endObject()
                .startObject("last_processed_seq_no").field("type", "long").endObject()
                .startObject("format_version").field("type", "integer").endObject()
                .startObject("last_harvest_max_external_version").field("type", "long").endObject()

                .startObject("seq_no").field("type", "long").endObject()
                .startObject("event_doc_id").field("type", "keyword").endObject()
                .startObject("event_external_version").field("type", "long").endObject()
                .startObject("status").field("type", "keyword").endObject()
                .startObject("requested_at_epoch_millis").field("type", "long").endObject()
                .startObject("resealed_at_epoch_millis").field("type", "long").endObject()

                .startObject("level").field("type", "integer").endObject()
                .startObject("bucket_id").field("type", "long").endObject()
                .startObject("leaf").field("type", "boolean").endObject()
                .startObject("bucket_start_millis").field("type", "long").endObject()
                .startObject("bucket_end_millis").field("type", "long").endObject()

                .startObject("digest").field("type", "keyword").endObject()

                .startObject("doc_count").field("type", "long").endObject()
                .startObject("child_count").field("type", "long").endObject()
                .startObject("delete_event_count").field("type", "long").endObject()
                .startObject("sealed_by_max_external_version").field("type", "long").endObject()

                .endObject()
                .endObject();
    }

    private void invalidateSystemIndexCache() {
        indexEnsured.set(false);
    }

    public boolean consumeSystemIndexRecreated() {
        return systemIndexRecreated.getAndSet(false);
    }

    /**
     * Additively updates the mapping of an existing system index so that fields
     * introduced by newer plugin versions are mapped with their canonical types
     * before any write can dynamic-map them (dynamic mapping would type integers
     * as [long], which verifySystemIndexMapping rejects). Type conflicts cannot
     * be updated in place; those fall through to verifySystemIndexMapping, which
     * reports the precise drift.
     */
    private void updateSystemIndexMapping() {
        try {
            pluginClient.client().admin().indices()
                    .putMapping(
                            new PutMappingRequest(IndexDigestSystemIndex.INDEX_NAME)
                                    .source(systemIndexMapping())
                    )
                    .actionGet();
        } catch (Exception e) {
            logger.warn(
                    "index-digest could not update system index mapping; drift check will report specifics",
                    e
            );
        }
    }

    public void resetShardCopy(ShardCopyKey key) {
        // Derived docs are written with RefreshPolicy.NONE; without a refresh
        // the delete-by-query below can miss the most recent ones.
        refreshSystemIndex();
        withSystemIndexRetry(() -> {
            org.opensearch.index.query.BoolQueryBuilder q =
                    org.opensearch.index.query.QueryBuilders.boolQuery()
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("index_uuid", key.indexUUID))
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("shard_id", key.shardId))
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("allocation_id", key.allocationId))
                            /*
                             * delete_event docs are deliberately NOT reset: they are
                             * immutable operation history, and the rebuild re-folds
                             * them (their tombstones may already be merged away, so
                             * they are the only durable copy). Losing them would
                             * manufacture permanent false divergence against peers
                             * that kept theirs.
                             */
                            .should(org.opensearch.index.query.QueryBuilders.termQuery("type", ProgressDoc.TYPE))
                            .should(org.opensearch.index.query.QueryBuilders.termQuery("type", BucketDoc.TYPE))
                            .should(org.opensearch.index.query.QueryBuilders.termQuery("type", ResealRequestDoc.TYPE))
                            .minimumShouldMatch(1);

            DeleteByQueryRequest req = new DeleteByQueryRequest(IndexDigestSystemIndex.INDEX_NAME)
                    .setQuery(q)
                    .setRefresh(true)
                    .setAbortOnVersionConflict(false);

            BulkByScrollResponse response = pluginClient.client()
                    .execute(DeleteByQueryAction.INSTANCE, req)
                    .actionGet();

            if (response.isTimedOut()
                    || response.getBulkFailures().isEmpty() == false
                    || response.getSearchFailures().isEmpty() == false) {
                throw new IllegalStateException(
                        "Failed resetting index-digest shard copy " + key
                                + " bulkFailures=" + response.getBulkFailures()
                                + " searchFailures=" + response.getSearchFailures()
                                + " timedOut=" + response.isTimedOut()
                );
            }

            logger.warn(
                    "index-digest reason=incompatible_progress_levels reset shard copy {} deleted_docs={}",
                    key,
                    response.getDeleted()
            );
            return null;
        });
    }

    /**
     * Post-write drift check, throttled: GetMappings is a cluster-manager-coordinated
     * read, so paying it on every write is too expensive. Throttling bounds the
     * undetected window after an external delete + auto-create to the interval;
     * ensureSystemIndex still verifies unconditionally.
     */
    private void maybeVerifySystemIndexMapping() {
        long now = threadPool.absoluteTimeInMillis();
        if (now - lastMappingVerifyEpochMillis < MAPPING_VERIFY_INTERVAL_MILLIS) {
            return;
        }
        verifySystemIndexMapping();
    }

    private void verifySystemIndexMapping() {
        GetMappingsResponse response = pluginClient.client().admin().indices()
                .getMappings(new GetMappingsRequest().indices(IndexDigestSystemIndex.INDEX_NAME))
                .actionGet();

        MappingMetadata mapping = response.mappings().get(IndexDigestSystemIndex.INDEX_NAME);
        try {
            if (mapping == null) {
                throw new IllegalStateException(MAPPING_DRIFT_MESSAGE + ": missing mappings");
            }
            verifyMappingProperties(mapping.sourceAsMap());
        } catch (IllegalStateException e) {
            invalidateSystemIndexCache();
            throw e;
        }

        lastMappingVerifyEpochMillis = threadPool.absoluteTimeInMillis();
    }

    // Package-private for tests: pure drift check over a mapping source map.
    @SuppressWarnings("unchecked")
    static void verifyMappingProperties(Map<String, Object> mappingSource) {
        Object propertiesObject = mappingSource.get("properties");
        if ((propertiesObject instanceof Map) == false) {
            throw new IllegalStateException(MAPPING_DRIFT_MESSAGE + ": missing properties");
        }

        Map<String, Object> properties = (Map<String, Object>) propertiesObject;
        requireFieldType(properties, "type", "keyword");
        requireFieldType(properties, "index_uuid", "keyword");
        requireFieldType(properties, "allocation_id", "keyword");
        requireFieldType(properties, "node_id", "keyword");
        requireFieldType(properties, "shard_role", "keyword");
        requireFieldType(properties, "scan_mode", "keyword");
        requireFieldType(properties, "digest", "keyword");
        requireFieldType(properties, "shard_id", "integer");
        requireFieldType(properties, "min_level", "integer");
        requireFieldType(properties, "max_level", "integer");
        requireFieldType(properties, "level", "integer");
        requireFieldType(properties, "bucket_id", "long");
        requireFieldType(properties, "bucket_start_millis", "long");
        requireFieldType(properties, "bucket_end_millis", "long");
        requireFieldType(properties, "doc_count", "long");
        requireFieldType(properties, "child_count", "long");
        requireFieldType(properties, "sealed_by_max_external_version", "long");
        requireFieldType(properties, "last_sealed_leaf_bucket_id", "long");
        requireFieldType(properties, "last_processed_seq_no", "long");
        requireFieldType(properties, "format_version", "integer");
        requireFieldType(properties, "last_harvest_max_external_version", "long");
        requireFieldType(properties, "delete_event_count", "long");
        requireFieldType(properties, "seq_no", "long");
        requireFieldType(properties, "event_doc_id", "keyword");
        requireFieldType(properties, "event_external_version", "long");
        requireFieldType(properties, "status", "keyword");
        requireFieldType(properties, "requested_at_epoch_millis", "long");
        requireFieldType(properties, "resealed_at_epoch_millis", "long");
        requireFieldType(properties, "last_seen_epoch_millis", "date");
        requireFieldType(properties, "leaf", "boolean");
    }

    @SuppressWarnings("unchecked")
    private static void requireFieldType(Map<String, Object> properties, String fieldName, String expectedType) {
        Object fieldObject = properties.get(fieldName);
        if ((fieldObject instanceof Map) == false) {
            throw new IllegalStateException(MAPPING_DRIFT_MESSAGE + ": missing field [" + fieldName + "]");
        }

        Object actualType = ((Map<String, Object>) fieldObject).get("type");
        if (expectedType.equals(actualType) == false) {
            throw new IllegalStateException(
                    MAPPING_DRIFT_MESSAGE + ": field [" + fieldName + "] expected type ["
                            + expectedType + "] but found [" + actualType + "]"
            );
        }
    }

    private boolean isIndexNotFound(Throwable t) {
        Throwable cur = t;
        while (cur != null) {
            if (cur instanceof IndexNotFoundException) {
                return true;
            }
            cur = cur.getCause();
        }
        return false;
    }

    @FunctionalInterface
    private interface SystemIndexOperation<T> {
        T run();
    }


    public void upsertDeleteEvents(List<DeleteEventDoc> events) {
        if (events == null || events.isEmpty()) return;

        withSystemIndexRetry(() -> {
            BulkRequest bulk = new BulkRequest();
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
            for (DeleteEventDoc event : events) {
                bulk.add(new IndexRequest(IndexDigestSystemIndex.INDEX_NAME).id(event.id).source(event.toSource()));
                if (bulk.numberOfActions() >= 1000) {
                    executeBulk(bulk);
                    bulk = new BulkRequest();
                    bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                }
            }
            if (bulk.numberOfActions() > 0) {
                executeBulk(bulk);
            }
            maybeVerifySystemIndexMapping();
            return null;
        });
        // The same scan tick reads pending events back via search; one refresh
        // after the batch makes them visible without per-chunk refresh cost.
        refreshSystemIndex();
    }

    /**
     * Delete events for one shard copy whose external version falls in
     * [fromExternalVersionInclusive, toExternalVersionExclusive), ordered by seq_no. Folded
     * events are retained permanently (reseal recomputes windows from them),
     * so callers scope the range: pending events via the sealed watermark
     * boundary, reseal via the window's version range.
     */
    public List<DeleteEventDoc> searchDeleteEventsInRange(
            ShardCopyKey key,
            long fromExternalVersionInclusive,
            long toExternalVersionExclusive
    ) {
        return withSystemIndexRetry(() -> {
            final int pageSize = 1000;
            Object[] searchAfter = null;
            List<DeleteEventDoc> out = new java.util.ArrayList<>();

            while (true) {
                org.opensearch.index.query.BoolQueryBuilder externalVersionRange =
                        org.opensearch.index.query.QueryBuilders.boolQuery()
                                .should(org.opensearch.index.query.QueryBuilders.rangeQuery("event_external_version")
                                        .gte(fromExternalVersionInclusive)
                                        .lt(toExternalVersionExclusive))
                                .should(org.opensearch.index.query.QueryBuilders.rangeQuery("event_version_ts")
                                        .gte(fromExternalVersionInclusive)
                                        .lt(toExternalVersionExclusive))
                                .minimumShouldMatch(1);

                org.opensearch.index.query.BoolQueryBuilder q =
                        org.opensearch.index.query.QueryBuilders.boolQuery()
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("type", DeleteEventDoc.TYPE))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("index_uuid", key.indexUUID))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("shard_id", key.shardId))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("allocation_id", key.allocationId))
                                .filter(externalVersionRange);

                org.opensearch.search.builder.SearchSourceBuilder source =
                        new org.opensearch.search.builder.SearchSourceBuilder()
                                .size(pageSize)
                                .query(q)
                                .sort("seq_no");

                if (searchAfter != null) {
                    source.searchAfter(searchAfter);
                }

                org.opensearch.action.search.SearchRequest req =
                        new org.opensearch.action.search.SearchRequest(IndexDigestSystemIndex.INDEX_NAME)
                                .source(source);

                org.opensearch.action.search.SearchResponse res =
                        pluginClient.client().search(req).actionGet();

                org.opensearch.search.SearchHit[] hits = res.getHits().getHits();
                if (hits.length == 0) {
                    break;
                }

                for (org.opensearch.search.SearchHit hit : hits) {
                    out.add(DeleteEventDoc.fromSource(hit.getId(), hit.getSourceAsMap()));
                }

                if (hits.length < pageSize) {
                    break;
                }

                searchAfter = hits[hits.length - 1].getSortValues();
            }

            return out;
        });
    }

    public void deleteDocsByIds(List<String> docIds) {
        if (docIds == null || docIds.isEmpty()) return;

        withSystemIndexRetry(() -> {
            BulkRequest bulk = new BulkRequest();
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
            for (String docId : docIds) {
                bulk.add(new org.opensearch.action.delete.DeleteRequest(IndexDigestSystemIndex.INDEX_NAME, docId));
                if (bulk.numberOfActions() >= 1000) {
                    executeBulk(bulk);
                    bulk = new BulkRequest();
                    bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                }
            }
            if (bulk.numberOfActions() > 0) {
                executeBulk(bulk);
            }
            return null;
        });
    }

    public ResealRequestDoc getResealRequest(String indexUuid, int shardId, String allocationId, long windowBucketId) {
        return withSystemIndexRetry(() -> {
            String id = ResealRequestDoc.docId(indexUuid, shardId, allocationId, windowBucketId);
            GetResponse response = pluginClient.client()
                    .get(new GetRequest(IndexDigestSystemIndex.INDEX_NAME, id))
                    .actionGet();
            if (response.isExists() == false) {
                return null;
            }
            return ResealRequestDoc.fromSource(id, response.getSourceAsMap());
        });
    }

    public void upsertResealRequest(ResealRequestDoc doc) {
        upsertResealRequests(List.of(doc));
    }

    /**
     * One bulk + one refresh: requests must be visible to the owning node's
     * scanner search on its next tick, without per-request refresh amplification
     * on the compare path.
     */
    public void upsertResealRequests(List<ResealRequestDoc> docs) {
        if (docs == null || docs.isEmpty()) return;
        withSystemIndexRetry(() -> {
            BulkRequest bulk = new BulkRequest();
            bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
            for (ResealRequestDoc doc : docs) {
                bulk.add(new IndexRequest(IndexDigestSystemIndex.INDEX_NAME).id(doc.id).source(doc.toSource()));
            }
            executeBulk(bulk);
            return null;
        });
        refreshSystemIndex();
    }

    public List<ResealRequestDoc> searchPendingResealRequests(ShardCopyKey key) {
        return withSystemIndexRetry(() -> {
            org.opensearch.index.query.BoolQueryBuilder q =
                    org.opensearch.index.query.QueryBuilders.boolQuery()
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("type", ResealRequestDoc.TYPE))
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("index_uuid", key.indexUUID))
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("shard_id", key.shardId))
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("allocation_id", key.allocationId))
                            .filter(org.opensearch.index.query.QueryBuilders.termQuery("status", ResealRequestDoc.STATUS_PENDING));

            org.opensearch.search.builder.SearchSourceBuilder source =
                    new org.opensearch.search.builder.SearchSourceBuilder()
                            .size(100)
                            .query(q)
                            .sort("bucket_id");

            org.opensearch.action.search.SearchRequest req =
                    new org.opensearch.action.search.SearchRequest(IndexDigestSystemIndex.INDEX_NAME)
                            .source(source);

            org.opensearch.action.search.SearchResponse res =
                    pluginClient.client().search(req).actionGet();

            List<ResealRequestDoc> out = new java.util.ArrayList<>();
            for (org.opensearch.search.SearchHit hit : res.getHits().getHits()) {
                out.add(ResealRequestDoc.fromSource(hit.getId(), hit.getSourceAsMap()));
            }
            return out;
        });
    }

    public List<BucketDoc> searchBucketDocsForSubtree(
            String indexUuid,
            int shardId,
            String allocationId,
            int minLevel,
            int rootLevel,
            long startLeafBucket,
            long endLeafBucket
    ) {
        if (endLeafBucket < startLeafBucket) {
            return List.of();
        }

        return withSystemIndexRetry(() -> {
            final int pageSize = 1000;
            Object[] searchAfter = null;
            List<BucketDoc> out = new java.util.ArrayList<>();

            while (true) {
                org.opensearch.index.query.BoolQueryBuilder q =
                        org.opensearch.index.query.QueryBuilders.boolQuery()
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("type", BucketDoc.TYPE))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("index_uuid", indexUuid))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("shard_id", shardId))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("allocation_id", allocationId))
                                .filter(org.opensearch.index.query.QueryBuilders.rangeQuery("level").gte(minLevel).lte(rootLevel))
                                .filter(org.opensearch.index.query.QueryBuilders.rangeQuery("bucket_id").gte(startLeafBucket).lte(endLeafBucket));

                org.opensearch.search.builder.SearchSourceBuilder source =
                        new org.opensearch.search.builder.SearchSourceBuilder()
                                .size(pageSize)
                                .query(q)
                                .sort("level")
                                .sort("bucket_id");

                if (searchAfter != null) {
                    source.searchAfter(searchAfter);
                }

                org.opensearch.action.search.SearchRequest req =
                        new org.opensearch.action.search.SearchRequest(IndexDigestSystemIndex.INDEX_NAME)
                                .source(source);

                org.opensearch.action.search.SearchResponse res =
                        pluginClient.client().search(req).actionGet();

                org.opensearch.search.SearchHit[] hits = res.getHits().getHits();
                if (hits.length == 0) {
                    break;
                }

                for (org.opensearch.search.SearchHit hit : hits) {
                    out.add(BucketDoc.fromSource(hit.getId(), hit.getSourceAsMap()));
                    if (out.size() > MAX_TREE_BUCKET_DOCS) {
                        throw new IllegalStateException(
                                "index-digest tree response exceeds [" + MAX_TREE_BUCKET_DOCS
                                        + "] bucket docs; request a smaller depth or narrower root"
                        );
                    }
                }

                if (hits.length < pageSize) {
                    break;
                }

                searchAfter = hits[hits.length - 1].getSortValues();
            }

            return out;
        });
    }

    public List<BucketDoc> searchBucketDocsAtLevel(
            String indexUuid,
            int shardId,
            String allocationId,
            int level,
            long startBucket,
            long endBucket
    ) {
        if (endBucket < startBucket) {
            return List.of();
        }

        return withSystemIndexRetry(() -> {
            final int pageSize = 1000;
            Object[] searchAfter = null;
            List<BucketDoc> out = new java.util.ArrayList<>();

            while (true) {
                org.opensearch.index.query.BoolQueryBuilder q =
                        org.opensearch.index.query.QueryBuilders.boolQuery()
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("type", BucketDoc.TYPE))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("index_uuid", indexUuid))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("shard_id", shardId))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("allocation_id", allocationId))
                                .filter(org.opensearch.index.query.QueryBuilders.termQuery("level", level))
                                .filter(org.opensearch.index.query.QueryBuilders.rangeQuery("bucket_id").gte(startBucket).lte(endBucket));

                org.opensearch.search.builder.SearchSourceBuilder source =
                        new org.opensearch.search.builder.SearchSourceBuilder()
                                .size(pageSize)
                                .query(q)
                                .sort("bucket_id");

                if (searchAfter != null) {
                    source.searchAfter(searchAfter);
                }

                org.opensearch.action.search.SearchRequest req =
                        new org.opensearch.action.search.SearchRequest(IndexDigestSystemIndex.INDEX_NAME)
                                .source(source);

                org.opensearch.action.search.SearchResponse res =
                        pluginClient.client().search(req).actionGet();

                org.opensearch.search.SearchHit[] hits = res.getHits().getHits();
                if (hits.length == 0) {
                    break;
                }

                for (org.opensearch.search.SearchHit hit : hits) {
                    out.add(BucketDoc.fromSource(hit.getId(), hit.getSourceAsMap()));
                    if (out.size() > MAX_BUCKET_DOCS_PER_LEVEL_RANGE) {
                        throw new IllegalStateException(
                                "index-digest exact-level response exceeds [" + MAX_BUCKET_DOCS_PER_LEVEL_RANGE
                                        + "] bucket docs; request a higher root_level or narrower range"
                        );
                    }
                }

                if (hits.length < pageSize) {
                    break;
                }

                searchAfter = hits[hits.length - 1].getSortValues();
            }

            return out;
        });
    }

}
