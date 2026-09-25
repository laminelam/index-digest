/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indexdigest.IndexDigestService;
import org.opensearch.indexdigest.tree.IndexDigestCompareRequest;
import org.opensearch.indexdigest.tree.IndexDigestCompareResult;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public final class RestIndexDigestCompareAction extends BaseRestHandler {
    private static final int DEFAULT_MAX_MISMATCHES = 100;
    private static final int MAX_MISMATCHES = 1000;

    private final Supplier<IndexDigestService> serviceSupplier;

    public RestIndexDigestCompareAction(Supplier<IndexDigestService> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public String getName() {
        return "index_digest_compare_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/_index_digest/_compare"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String leftIndex = requireNonEmptyParam(request, "left_index");
        requireNonEmptyParam(request, "left_shard_id");
        String leftAllocationId = requireNonEmptyParam(request, "left_allocation_id");
        String rightIndex = requireNonEmptyParam(request, "right_index");
        requireNonEmptyParam(request, "right_shard_id");
        String rightAllocationId = requireNonEmptyParam(request, "right_allocation_id");

        int maxMismatches = request.paramAsInt("max_mismatches", DEFAULT_MAX_MISMATCHES);
        if (maxMismatches < 1) {
            throw new IllegalArgumentException("[max_mismatches] must be >= 1");
        }
        if (maxMismatches > MAX_MISMATCHES) {
            throw new IllegalArgumentException("[max_mismatches] must be <= " + MAX_MISMATCHES);
        }

        int rootLevel = request.hasParam("root_level")
                ? request.paramAsInt("root_level", IndexDigestCompareRequest.DEFAULT_ROOT_LEVEL)
                : IndexDigestCompareRequest.DEFAULT_ROOT_LEVEL;
        int leftShardId = request.paramAsInt("left_shard_id", -1);
        int rightShardId = request.paramAsInt("right_shard_id", -1);
        if (leftShardId < 0) {
            throw new IllegalArgumentException("[left_shard_id] must be >= 0");
        }
        if (rightShardId < 0) {
            throw new IllegalArgumentException("[right_shard_id] must be >= 0");
        }

        IndexDigestCompareRequest compareRequest = new IndexDigestCompareRequest(
                leftIndex,
                leftShardId,
                leftAllocationId,
                rightIndex,
                rightShardId,
                rightAllocationId,
                rootLevel,
                maxMismatches,
                request.paramAsBoolean("include_digest", true)
        );

        return channel -> {
            IndexDigestService svc = serviceSupplier.get();
            if (svc == null) {
                channel.sendResponse(new BytesRestResponse(
                        RestStatus.SERVICE_UNAVAILABLE,
                        "index-digest service not initialized"
                ));
                return;
            }

            svc.compareAsync(
                    compareRequest,
                    result -> {
                        try (XContentBuilder b = jsonBuilder()) {
                            writeResponse(b, result, compareRequest.includeDigest);
                            channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    e -> {
                        try {
                            channel.sendResponse(new BytesRestResponse(channel, e));
                        } catch (Exception sendError) {
                            throw new RuntimeException(sendError);
                        }
                    }
            );
        };
    }

    private static String requireNonEmptyParam(RestRequest request, String name) {
        if (request.hasParam(name) == false) {
            throw new IllegalArgumentException("Missing required parameter [" + name + "]");
        }
        String value = request.param(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Parameter [" + name + "] must not be empty");
        }
        return value;
    }

    private static void writeResponse(
            XContentBuilder b,
            IndexDigestCompareResult result,
            boolean includeDigest
    ) throws IOException {
        b.startObject();

        b.field("comparable", result.comparable);
        b.field("equal", result.equal);
        b.field("truncated", result.truncated);
        if (result.reason != null) {
            b.field("reason", result.reason);
        }

        b.field("min_level", result.minLevel);
        b.field("root_level", result.rootLevel);
        b.field("max_mismatches", result.maxMismatches);
        b.field("common_last_sealed_leaf_bucket", result.commonLastSealedLeafBucketId);
        b.startObject("root_range");
        b.field("start_bucket", result.startRootBucket);
        b.field("end_bucket", result.endRootBucket);
        b.endObject();

        b.field("left");
        writeSide(b, result.left);
        b.field("right");
        writeSide(b, result.right);

        b.startArray("reseal_requested_windows");
        for (Long window : result.resealRequestedWindows) {
            b.value(window);
        }
        b.endArray();
        b.field("mismatch_count", result.mismatches.size());
        b.startArray("mismatches");
        for (IndexDigestCompareResult.Mismatch mismatch : result.mismatches) {
            writeMismatch(b, mismatch, includeDigest);
        }
        b.endArray();

        b.endObject();
    }

    private static void writeSide(XContentBuilder b, IndexDigestCompareResult.Side side) throws IOException {
        b.startObject();
        b.field("index", side.index);
        b.field("index_uuid", side.indexUuid);
        b.field("shard_id", side.shardId);
        b.field("allocation_id", side.allocationId);
        b.field("progress_found", side.progressFound);
        b.field("last_sealed_leaf_bucket", side.lastSealedLeafBucketId);
        b.field("last_seen_epoch_millis", side.lastSeenEpochMillis);
        b.field("min_level", side.minLevel);
        b.field("max_level", side.maxLevel);
        b.field("format_version", side.formatVersion);
        b.endObject();
    }

    private static void writeMismatch(
            XContentBuilder b,
            IndexDigestCompareResult.Mismatch mismatch,
            boolean includeDigest
    ) throws IOException {
        b.startObject();
        b.field("level", mismatch.level);
        b.field("bucket_id", mismatch.bucketId);
        b.startObject("range");
        b.field("start", mismatch.bucketStartMillis);
        b.field("end", mismatch.bucketEndMillis);
        b.endObject();
        b.field("leaf", mismatch.leaf);
        b.field("reason", mismatch.reason);
        b.field("left");
        writeBucket(b, mismatch.left, includeDigest);
        b.field("right");
        writeBucket(b, mismatch.right, includeDigest);
        b.endObject();
    }

    private static void writeBucket(
            XContentBuilder b,
            IndexDigestCompareResult.Bucket bucket,
            boolean includeDigest
    ) throws IOException {
        b.startObject();
        b.field("present", bucket.present);
        b.field("level", bucket.level);
        b.field("bucket_id", bucket.bucketId);
        b.field("leaf", bucket.leaf);
        b.field("doc_count", bucket.docCount);
        b.field("child_count", bucket.childCount);
        b.field("delete_event_count", bucket.deleteEventCount);
        b.field("sealed_by_max_external_version", bucket.sealedByMaxExternalVersion);
        if (includeDigest) {
            b.field("digest", bucket.digest);
        }
        b.endObject();
    }
}
