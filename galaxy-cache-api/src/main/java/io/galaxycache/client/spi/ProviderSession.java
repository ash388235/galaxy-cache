package io.galaxycache.client.spi;

import io.galaxycache.client.Capability;
import io.galaxycache.common.DatasetId;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.storage.DatasetMetadata;

import java.util.List;
import java.util.Map;
import java.util.Set;

/** One open connection of a provider: everything one {@code GalaxyClient} uses. Thread-safe. */
public interface ProviderSession extends AutoCloseable {

    String providerName();

    /** Data capabilities (cluster capabilities come from {@link ProviderCluster#capabilities()}). */
    Set<Capability> capabilities();

    /**
     * Opens a dataset, creating it when missing; existing metadata wins.
     *
     * @param indexedTypes {@code @Indexed} field name to its Java type, for providers that need column types
     */
    ProviderCache open(DatasetId dataset, DatasetMetadata wanted, Map<String, Class<?>> indexedTypes);

    /** Datasets of one namespace, or of all namespaces when null, sorted. */
    List<DatasetId> listDatasets(String namespace);

    void dropDataset(DatasetId dataset);

    /** Creates (or confirms) a namespace. Providers without placement ignore {@code storage}/{@code indexNode}. */
    void createNamespace(String namespace, String storage, String indexNode);

    Map<String, GalaxyConfig.NamespaceConfig> listNamespaces();

    /** The cluster named {@code name}, or the default one when null. Never null: may have no capabilities. */
    ProviderCluster cluster(String name);

    /** Provider internals for tools and tests (e.g. the Ignite client); null when not available. */
    default <T> T unwrap(Class<T> type) {
        return type.isInstance(this) ? type.cast(this) : null;
    }

    @Override
    void close();
}
