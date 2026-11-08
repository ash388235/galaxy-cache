package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.codec.Codec;
import io.galaxycache.common.codec.JsonCodec;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.config.NamespaceCatalog;
import io.galaxycache.index.api.IndexClient;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.StorageProviders;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point of the client-cache-api. One instance per process is enough; it is thread-safe.
 *
 * <pre>{@code
 * try (GalaxyClient client = GalaxyClient.fromConfig(Path.of("galaxy.yaml"))) {
 *     GalaxyCache<Trade> trades = client.cache(DatasetId.of("trading", "equities", "us", LocalDate.now()), Trade.class);
 *     trades.put("T-1", trade);
 * }
 * }</pre>
 */
public final class GalaxyClient implements AutoCloseable {

    private final GalaxyConfig config;
    private final NamespaceCatalog catalog;
    private final Codec codec;
    private final Map<String, DataStore> stores = new ConcurrentHashMap<>();
    private final Map<String, IndexClient> indexClients = new ConcurrentHashMap<>();
    private final Map<DatasetId, GalaxyCache<?>> caches = new ConcurrentHashMap<>();
    private final ExecutorService readPool;
    private final QueryLogger queryLogger;

    public GalaxyClient(GalaxyConfig config) {
        this(config, new JsonCodec());
    }

    public GalaxyClient(GalaxyConfig config, Codec codec) {
        this.config = config.validate();
        this.catalog = new NamespaceCatalog(config);
        this.codec = codec;
        AtomicInteger n = new AtomicInteger();
        this.readPool = Executors.newFixedThreadPool(config.client().readParallelism(), r -> {
            Thread t = new Thread(r, "galaxy-read-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        this.queryLogger = new QueryLogger(config.logging().queryLogDir(), config.logging().systemLogger());
    }

    public static GalaxyClient fromConfig(Path yaml) {
        return new GalaxyClient(GalaxyConfig.load(yaml));
    }

    public GalaxyConfig config() {
        return config;
    }

    // ------------------------------------------------------------------ namespaces

    /** Creates (or confirms) a namespace mapped to a storage and an index node, visible to all clients. */
    public void createNamespace(String namespace, String storage, String indexNode) {
        catalog.create(namespace, storage, indexNode);
    }

    public Map<String, GalaxyConfig.NamespaceConfig> listNamespaces() {
        return catalog.all();
    }

    // ------------------------------------------------------------------ datasets

    /** Opens (creating if needed) the cache of one dataset for one domain type. */
    @SuppressWarnings("unchecked")
    public <T> GalaxyCache<T> cache(DatasetId dataset, Class<T> type) {
        GalaxyCache<?> c = caches.computeIfAbsent(dataset, ds -> {
            GalaxyConfig.NamespaceConfig nc = catalog.resolve(ds.namespace());
            return new GalaxyCache<>(ds, type, store(nc.storage()), indexFor(nc), codec, readPool,
                    config.client(), queryLogger);
        });
        if (c.type() != type) {
            throw new GalaxyException("Dataset " + dataset + " is already open for " + c.type().getName());
        }
        return (GalaxyCache<T>) c;
    }

    /** Datasets that exist in storage for a namespace (or for all namespaces when null). */
    public List<DatasetId> listDatasets(String namespace) {
        if (namespace != null) {
            return store(catalog.resolve(namespace).storage()).listDatasets(namespace);
        }
        List<DatasetId> out = new ArrayList<>();
        catalog.all().forEach((ns, nc) -> out.addAll(store(nc.storage()).listDatasets(ns)));
        out.sort(null);
        return out;
    }

    /** Deletes a whole dataset from its index pod and its storage. */
    public void dropDataset(DatasetId dataset) {
        GalaxyConfig.NamespaceConfig nc = catalog.resolve(dataset.namespace());
        caches.remove(dataset);
        indexFor(nc).drop(dataset);
        store(nc.storage()).dropDataset(dataset);
    }

    // ------------------------------------------------------------------ plumbing

    DataStore store(String name) {
        return stores.computeIfAbsent(name, n -> {
            GalaxyConfig.StorageConfig sc = config.storage(n);
            return StorageProviders.open(sc.type(), n, sc.settings());
        });
    }

    /** The namespace's index: the storage's own (grid) if it has one, otherwise its index pod. */
    IndexClient indexFor(GalaxyConfig.NamespaceConfig nc) {
        return store(nc.storage()) instanceof io.galaxycache.index.api.IndexingStore is ? is.indexClient() : index(nc.indexNode());
    }

    IndexClient index(String node) {
        return indexClients.computeIfAbsent(node, n -> {
            GalaxyConfig.IndexNodeConfig nc = config.indexNode(n);
            return new HttpIndexClient(n, nc.url(), config.client().indexTimeoutMs(), config.client().indexRetries());
        });
    }

    @Override
    public void close() {
        readPool.shutdown();
        stores.values().forEach(DataStore::close);
        indexClients.values().forEach(IndexClient::close);
        queryLogger.close();
    }
}
