/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.scan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.indexdigest.IndexDigestSettings;
import org.opensearch.indexdigest.model.ShardCopyKey;
import org.opensearch.indexdigest.persistence.IndexDigestSystemIndex;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Resolves eligible local shard copies for index-digest processing.
 *
 * Filtering happens here (Option B):
 *  - allowlisted indices only (plugins.index_digest.indices)
 *  - scan mode (primary_only, all_copies, selected_copies)
 *  - local shards only (IndicesService iteration)
 *  - STARTED shard copies only
 *  - skips the .index_digest system index
 *
 * Output identity: (indexUUID, shardId, allocationId)
 */
public final class ShardCopyResolver {
    private static final Logger logger = LogManager.getLogger(ShardCopyResolver.class);

    private final IndicesService indicesService;

    /**
     * Cached allowlist patterns. Updated dynamically via ClusterSettings consumer.
     * Empty => hash nothing (safety default).
     */
    private volatile String[] allowedIndexPatterns = new String[0];
    private volatile ScanMode scanMode = ScanMode.PRIMARY_ONLY;
    private volatile Set<String> selectedShardCopyIds = Set.of();

    public ShardCopyResolver(ClusterSettings clusterSettings, Settings nodeSettings, IndicesService indicesService) {
        this.indicesService = Objects.requireNonNull(indicesService);

        // initial value (boot)
        setAllowedIndexPatterns(IndexDigestSettings.TARGET_INDICES.get(nodeSettings));
        setScanMode(IndexDigestSettings.SCAN_MODE.get(nodeSettings));
        setSelectedShardCopyIds(IndexDigestSettings.SELECTED_SHARD_COPIES.get(nodeSettings));

        // dynamic updates
        clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.TARGET_INDICES, this::setAllowedIndexPatterns);
        clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.SCAN_MODE, this::setScanMode);
        clusterSettings.addSettingsUpdateConsumer(IndexDigestSettings.SELECTED_SHARD_COPIES, this::setSelectedShardCopyIds);
    }

    private void setAllowedIndexPatterns(List<String> patterns) {
        if (patterns == null || patterns.isEmpty()) {
            this.allowedIndexPatterns = new String[0];
        } else {
            this.allowedIndexPatterns = patterns.toArray(new String[0]);
        }
        logger.debug("index-digest updated allowlist patterns={}", patterns);
    }

    private void setScanMode(String value) {
        this.scanMode = ScanMode.parse(value);
        logger.info("index-digest updated scan mode={}", scanMode.settingValue());
    }

    private void setSelectedShardCopyIds(List<String> selected) {
        if (selected == null || selected.isEmpty()) {
            this.selectedShardCopyIds = Set.of();
        } else {
            this.selectedShardCopyIds = Set.copyOf(new HashSet<>(selected));
        }
        logger.debug("index-digest updated selected shard copies={}", selectedShardCopyIds);
    }

    public List<ShardCopyKey> resolveLocalShardCopies() {
        final String[] patterns = this.allowedIndexPatterns;
        if (patterns.length == 0) {
            return List.of();
        }

        final List<ShardCopyKey> out = new ArrayList<>();
        final ScanMode mode = this.scanMode;
        final Set<String> selected = this.selectedShardCopyIds;

        for (IndexService indexService : indicesService) {
            final String indexName = indexService.index().getName();

            if (IndexDigestSystemIndex.INDEX_NAME.equals(indexName)) {
                continue;
            }

            if (Regex.simpleMatch(patterns, indexName) == false) {
                continue;
            }

            for (IndexShard shard : indexService) {
                if (shard == null) continue;
                if (shard.state() != IndexShardState.STARTED) continue;
                if (shard.routingEntry() == null || shard.routingEntry().started() == false) continue;
                if (shard.routingEntry().allocationId() == null) continue;

                final String indexUuid = shard.shardId().getIndex().getUUID();
                final int shardId = shard.shardId().id();
                final String allocationId = shard.routingEntry().allocationId().getId();
                final boolean primary = shard.routingEntry().primary();
                final String nodeId = shard.routingEntry().currentNodeId();

                ShardCopyKey key = new ShardCopyKey(
                        indexUuid,
                        shardId,
                        allocationId,
                        primary,
                        nodeId,
                        mode
                );

                if (shouldInclude(key, mode, selected) == false) {
                    continue;
                }

                out.add(key);
            }
        }

        logger.trace(
                "index-digest resolved {} shard copies for patterns={} scan_mode={}",
                out.size(),
                List.of(patterns),
                mode.settingValue()
        );
        return out;
    }

    private static boolean shouldInclude(ShardCopyKey key, ScanMode mode, Set<String> selected) {
        return switch (mode) {
            case PRIMARY_ONLY -> key.primary;
            case ALL_COPIES -> true;
            case SELECTED_COPIES -> selected.contains(key.identityString());
        };
    }
}
