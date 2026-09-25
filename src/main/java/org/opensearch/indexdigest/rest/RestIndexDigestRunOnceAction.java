/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indexdigest.IndexDigestService;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public final class RestIndexDigestRunOnceAction extends BaseRestHandler {

    private final Supplier<IndexDigestService> serviceSupplier;

    public RestIndexDigestRunOnceAction(Supplier<IndexDigestService> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public String getName() {
        return "index_digest_run_once_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.POST, "/_plugins/_index_digest/_run_once"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            IndexDigestService svc = serviceSupplier.get();
            if (svc == null) {
                channel.sendResponse(new BytesRestResponse(
                        RestStatus.SERVICE_UNAVAILABLE,
                        "index-digest service not initialized"
                ));
                return;
            }

            svc.triggerScanOnceAsync(
                    result -> {
                        try (XContentBuilder b = jsonBuilder()) {
                            b.startObject();
                            b.field("triggered", true);
                            b.field("attempted_shard_copies", result.attemptedShardCopies);
                            b.field("skipped_in_flight_shard_copies", result.skippedInFlightShardCopies);
                            b.endObject();
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
}
