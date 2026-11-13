package io.galaxycache.client.galaxy;

import java.util.EnumSet;
import io.galaxycache.client.Capability;
import io.galaxycache.client.spi.ClusterSupport;
import io.galaxycache.client.spi.ProviderCache;
import io.galaxycache.client.spi.ProviderCluster;
import io.galaxycache.client.spi.ProviderSession;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.config.NamespaceCatalog;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.index.api.IndexingStore;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.StorageProviders;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The galaxy provider's session: storages opened by type ({@code file}, {@code grid}), index pods over HTTP, and
 * namespace placement from the catalog. A grid storage that holds its own index is used instead of an index pod.
 */
public final class GalaxyProviderSession implements ProviderSession {

    private final GalaxyConfig config;
    private final NamespaceCatalog catalog;
    private final Map<String, DataStore> stores = new ConcurrentHashMap<>();
    private final Map<String, IndexClient> indexClients = new ConcurrentHashMap<>();

    GalaxyProviderSession(GalaxyConfig config) {
        this.config = config;
        this.catalog = new NamespaceCatalog(config);
    }

    @Override
    public String providerName() {
        return GalaxyCacheProvider.NAME;
    }

    /** No optional data operation: the file store and the grid have no atomic ops, expiry, transactions or events. */
    @Override
    public Set<Capability> capabilities() {
        return Set.of();
    }

    @Override
    public ProviderCache open(DatasetId dataset, DatasetMetadata wanted, Map<String, Class<?>> indexedTypes) {
        GalaxyConfig.NamespaceConfig nc = catalog.resolve(dataset.namespace());
        DataStore store = store(nc.storage());
        return new GalaxyProviderCache(dataset, store.ensureDataset(wanted), store, indexFor(nc));
    }

    @Override
    public List<DatasetId> listDatasets(String namespace) {
        if (namespace != null) {
            return store(catalog.resolve(namespace).storage()).listDatasets(namespace);
        }
        List<DatasetId> out = new ArrayList<>();
        catalog.all().forEach((ns, nc) -> out.addAll(store(nc.storage()).listDatasets(ns)));
        out.sort(null);
        return out;
    }

    @Override
    public void dropDataset(DatasetId dataset) {
        GalaxyConfig.NamespaceConfig nc = catalog.resolve(dataset.namespace());
        indexFor(nc).drop(dataset);
        store(nc.storage()).dropDataset(dataset);
    }

    @Override
    public void createNamespace(String namespace, String storage, String indexNode) {
        catalog.create(namespace, storage, indexNode);
    }

    @Override
    public Map<String, GalaxyConfig.NamespaceConfig> listNamespaces() {
        return catalog.all();
    }

    /**
     * {@code name} is a storage name; null means the only grid storage in the configuration. Storages that are not
     * clusters (the file store), or no grid at all, give a cluster with no capabilities.
     */
    @Override
    public ProviderCluster cluster(String name) {
        if (name == null) {
            List<String> grids = config.storages().entrySet().stream()
                    .filter(e -> "grid".equalsIgnoreCase(e.getValue().type())).map(Map.Entry::getKey).sorted().toList();
            if (grids.isEmpty()) {
                return ProviderCluster.none("none");
            }
            if (grids.size() > 1) {
                throw new GalaxyException("the configuration has several grids " + grids + "; use cluster(name)");
            }
            name = grids.get(0);
        }
        String storage = name;
        GalaxyConfig.StorageConfig sc = config.storage(storage);
        if (stores.containsKey(storage)) {
            return clusterOf(storage);
        }
        // not connected yet: report capabilities without connecting, connect on the first real call
        Set<Capability> caps = EnumSet.noneOf(Capability.class);
        StorageProviders.find(sc.type()).clusterCapabilities().forEach(c -> caps.add(Capability.valueOf(c)));
        return new LazyCluster(storage, Set.copyOf(caps), () -> clusterOf(storage));
    }

    private ProviderCluster clusterOf(String storage) {
        DataStore s = store(storage);
        return s instanceof ClusterSupport cs ? cs.cluster() : ProviderCluster.none(storage);
    }

    // ------------------------------------------------------------------ plumbing (public for tools and tests)

    public DataStore store(String name) {
        return stores.computeIfAbsent(name, n -> {
            GalaxyConfig.StorageConfig sc = config.storage(n);
            return StorageProviders.open(sc.type(), n, sc.settings());
        });
    }

    /** The namespace's index: the storage's own (grid) if it has one, otherwise its index pod. */
    public IndexClient indexFor(GalaxyConfig.NamespaceConfig nc) {
        return store(nc.storage()) instanceof IndexingStore is ? is.indexClient() : index(nc.indexNode());
    }

    public IndexClient index(String node) {
        return indexClients.computeIfAbsent(node, n -> {
            GalaxyConfig.IndexNodeConfig nc = config.indexNode(n);
            return new HttpIndexClient(n, nc.url(), config.client().indexTimeoutMs(), config.client().indexRetries());
        });
    }

    @Override
    public void close() {
        stores.values().forEach(DataStore::close);
        indexClients.values().forEach(IndexClient::close);
    }
}
