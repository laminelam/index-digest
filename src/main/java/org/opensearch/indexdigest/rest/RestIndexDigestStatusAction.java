/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.indexdigest.IndexDigestService;
import org.opensearch.indexdigest.IndexDigestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

public final class RestIndexDigestStatusAction extends BaseRestHandler {

    private final Supplier<IndexDigestService> serviceSupplier;

    public RestIndexDigestStatusAction(Supplier<IndexDigestService> serviceSupplier) {
        this.serviceSupplier = serviceSupplier;
    }

    @Override
    public String getName() {
        return "index_digest_status_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(RestRequest.Method.GET, "/_plugins/_index_digest/_status"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        return channel -> {
            IndexDigestService svc = serviceSupplier.get();
            if (svc == null) {
                channel.sendResponse(new BytesRestResponse(RestStatus.SERVICE_UNAVAILABLE, "index-digest service not initialized"));
                return;
            }

            IndexDigestStatus st = svc.status();
            try (XContentBuilder b = jsonBuilder()) {
                b.startObject();
                b.field("running", st.running);
                b.field("last_run_epoch_millis", st.lastRunEpochMillis);
                b.field("last_skipped_in_flight_shard_copies", st.lastSkippedInFlightShardCopies);
                if (st.lastError != null) {
                    b.field("last_error", st.lastError);
                }
                b.endObject();
                channel.sendResponse(new BytesRestResponse(RestStatus.OK, b));
            } catch (IOException e) {
                channel.sendResponse(new BytesRestResponse(channel, e));
            }
        };
    }
}
