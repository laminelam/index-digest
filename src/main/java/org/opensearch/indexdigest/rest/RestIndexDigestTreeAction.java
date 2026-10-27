/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indexdigest.IndexDigestService;
import org.opensearch.indexdigest.tree.TreeRequest;
import org.opensearch.indexdigest.tree.TreeView;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public final class RestIndexDigestTreeAction extends BaseRestHandler {

    private static final int DEFAULT_DEPTH = 2;
    private static final int MAX_DEPTH = 8;

    private final Supplier<IndexDigestService> serviceSupplier;

    public RestIndexDigestTreeAction(Supplier<IndexDigestService> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public String getName() {
        return "index_digest_tree_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/_index_digest/{index}/_tree"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String index = requireNonEmptyParam(request, "index");
        requireNonEmptyParam(request, "shard_id");
        String allocationId = requireNonEmptyParam(request, "allocation_id");
        requireNonEmptyParam(request, "root_level");
        requireNonEmptyParam(request, "root_bucket");

        int shardId = request.paramAsInt("shard_id", -1);
        int rootLevel = request.paramAsInt("root_level", -1);
        long rootBucket = Long.parseLong(request.param("root_bucket"));
        if (shardId < 0) {
            throw new IllegalArgumentException("[shard_id] must be >= 0");
        }
        if (rootLevel < 0) {
            throw new IllegalArgumentException("[root_level] must be >= 0");
        }
        if (rootBucket < 0L) {
            throw new IllegalArgumentException("[root_bucket] must be >= 0");
        }

        int depth = request.paramAsInt("depth", DEFAULT_DEPTH);
        if (depth < 0) {
            throw new IllegalArgumentException("[depth] must be >= 0");
        }
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException("[depth] must be <= " + MAX_DEPTH);
        }

        boolean includeDigest = request.paramAsBoolean("include_digest", true);

        TreeRequest treeRequest = new TreeRequest(
                index,
                shardId,
                allocationId,
                rootLevel,
                rootBucket,
                depth,
                includeDigest
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

            svc.treeViewAsync(
                    treeRequest,
                    view -> {
                        try (XContentBuilder b = jsonBuilder()) {
                            writeResponse(b, view);
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

    private static void writeResponse(XContentBuilder b, TreeView view) throws IOException {
        b.startObject();

        b.field("index", view.index);
        b.field("index_uuid", view.indexUuid);
        b.field("shard_id", view.shardId);
        b.field("allocation_id", view.allocationId);
        b.field("root_level", view.rootLevel);
        b.field("root_bucket", view.rootBucket);
        b.field("depth", view.depth);
        b.field("truncated", view.truncated);

        if (view.root == null) {
            b.nullField("root");
        } else {
            b.field("root");
            writeNode(b, view.root, view.includeDigest);
        }

        b.endObject();
    }

    private static void writeNode(
            XContentBuilder b,
            TreeView.Node node,
            boolean includeDigest
    ) throws IOException {
        b.startObject();

        b.field("level", node.level);
        b.field("bucket_id", node.bucketId);

        b.startObject("range");
        b.field("start", node.bucketStartMillis);
        b.field("end", node.bucketEndMillis);
        b.endObject();

        b.field("leaf", node.leaf);
        b.field("doc_count", node.docCount);
        b.field("child_count", node.childCount);
        b.field("delete_event_count", node.deleteEventCount);
        b.field("sealed_by_max_external_version", node.sealedByMaxExternalVersion);

        if (includeDigest) {
            b.field("digest_algo", node.digestAlgo);
            b.field("digest", node.digest);
        }

        b.startArray("children");
        for (TreeView.Node child : node.children) {
            writeNode(b, child, includeDigest);
        }
        b.endArray();

        b.endObject();
    }
}
