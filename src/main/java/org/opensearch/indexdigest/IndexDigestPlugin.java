/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.indexdigest;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.lifecycle.LifecycleComponent;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.indexdigest.client.PluginClient;
import org.opensearch.indexdigest.persistence.IndexDigestSystemIndex;
import org.opensearch.indexdigest.rest.RestIndexDigestCompareAction;
import org.opensearch.indexdigest.rest.RestIndexDigestRunOnceAction;
import org.opensearch.indexdigest.rest.RestIndexDigestStatusAction;
import org.opensearch.indexdigest.rest.RestIndexDigestTreeAction;
import org.opensearch.identity.PluginSubject;
import org.opensearch.indices.SystemIndexDescriptor;
import org.opensearch.node.Node;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.IdentityAwarePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SystemIndexPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Hierarchical Index Digest plugin entry point.
 */
public class IndexDigestPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin, IdentityAwarePlugin {

    private volatile PluginClient pluginClient;

    @Override
    public void assignSubject(PluginSubject pluginSubject) {
        PluginClient pc = this.pluginClient;
        if (pc != null) {
            pc.setSubject(pluginSubject);
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return IndexDigestSettings.ALL_SETTINGS;
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings nodeSettings) {
        return List.of(IndexDigestSystemIndex.descriptor());
    }

    /**
     * Both classes are {@code @Singleton}: Node resolves each entry here with
     * its own {@code injector.getInstance} call and the holder injects the
     * service, so without the scope every node would build - and start - two
     * services. Node's lifecycle starts the single service; nothing else may.
     */
    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return List.of(IndexDigestService.class, IndexDigestServiceHolder.class);
    }

    @Override
    public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.pluginClient = new PluginClient(client);
        return List.of(pluginClient);
    }

    @Override
    public List<RestHandler> getRestHandlers(
            Settings settings,
            RestController restController,
            ClusterSettings clusterSettings,
            IndexScopedSettings indexScopedSettings,
            SettingsFilter settingsFilter,
            IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        String nodeName = Node.NODE_NAME_SETTING.get(settings);
        Supplier<IndexDigestService> serviceSupplier = () -> IndexDigestServiceHolder.getServiceForNodeName(nodeName);

        return List.of(
                new RestIndexDigestStatusAction(serviceSupplier),
                new RestIndexDigestRunOnceAction(serviceSupplier),
                new RestIndexDigestCompareAction(serviceSupplier),
                new RestIndexDigestTreeAction(serviceSupplier)

        );
    }
}
