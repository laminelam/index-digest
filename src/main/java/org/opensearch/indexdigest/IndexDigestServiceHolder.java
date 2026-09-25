/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Singleton;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Small bridge exposing the Guice-created IndexDigestService to REST handlers.
 * <p>
 * Singleton: Guice constructs an unscoped class afresh on every resolution,
 * and Node resolves this holder independently of IndexDigestService.
 */
@Singleton
public final class IndexDigestServiceHolder extends AbstractLifecycleComponent {

    private static final ConcurrentMap<String, IndexDigestService> SERVICES_BY_NODE_NAME = new ConcurrentHashMap<>();
    private static volatile IndexDigestService fallbackService;

    private final IndexDigestService instance;
    private final String nodeName;

    @Inject
    public IndexDigestServiceHolder(IndexDigestService service, ClusterService clusterService) {
        this.instance = service;
        this.nodeName = clusterService.getNodeName();
        fallbackService = service;
        SERVICES_BY_NODE_NAME.put(nodeName, service);
    }

    public static IndexDigestService getServiceForNodeName(String nodeName) {
        if (nodeName == null) {
            return fallbackService;
        }
        IndexDigestService service = SERVICES_BY_NODE_NAME.get(nodeName);
        return service == null ? fallbackService : service;
    }

    @Override
    protected void doStart() {}

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        SERVICES_BY_NODE_NAME.remove(nodeName, instance);
        if (fallbackService == instance) {
            fallbackService = null;
        }
    }
}
