package io.galaxycache.client;

import io.galaxycache.client.spi.CacheProvider;
import io.galaxycache.client.spi.ProviderSession;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.codec.Codec;
import io.galaxycache.common.codec.JsonCodec;
import io.galaxycache.config.GalaxyConfig;

import java.nio.file.Path;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point of the client-cache-api. One instance per process is enough; it is thread-safe.
 * {@code provider:} in the configuration picks the implementation ({@code galaxy}, the default, or {@code ignite});
 * the code below is the same for both.
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
    private final ProviderSession session;
    private final Codec codec;
    private final Map<DatasetId, GalaxyCache<?>> caches = new ConcurrentHashMap<>();
    private final ExecutorService readPool;
    private final QueryLogger queryLogger;

    public GalaxyClient(GalaxyConfig config) {
        this(config, new JsonCodec());
    }

    public GalaxyClient(GalaxyConfig config, Codec codec) {
        this.config = config.validate();
        this.codec = codec;
        this.session = findProvider(config.provider()).open(config);
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

    static CacheProvider findProvider(String name) {
        Set<String> available = new TreeSet<>();
        for (CacheProvider p : ServiceLoader.load(CacheProvider.class)) {
            if (p.name().equalsIgnoreCase(name)) {
                return p;
            }
            available.add(p.name());
        }
        String hint = "ignite".equalsIgnoreCase(name) ? " Add the galaxy-ignite jar to the classpath." : "";
        throw new GalaxyException("No cache provider '" + name + "' on the classpath. Available: " + available + "." + hint);
    }

    public GalaxyConfig config() {
        return config;
    }

    // ------------------------------------------------------------------ provider and capabilities

    /** The provider in use: {@code galaxy} or {@code ignite}. */
    public String provider() {
        return session.providerName();
    }

    /** Data capabilities plus those of the default {@link #cluster()}. */
    public Set<Capability> capabilities() {
        EnumSet<Capability> all = EnumSet.noneOf(Capability.class);
        all.addAll(session.capabilities());
        all.addAll(session.cluster(null).capabilities());
        return all;
    }

    public boolean supports(Capability capability) {
        return capabilities().contains(capability);
    }

    /** The default cluster: the Ignite cluster, or the only grid of a galaxy configuration. */
    public GalaxyCluster cluster() {
        return new GalaxyCluster(session.cluster(null), provider());
    }

    /** A named cluster: for galaxy, a grid storage name; for Ignite, its only cluster. */
    public GalaxyCluster cluster(String name) {
        return new GalaxyCluster(session.cluster(name), provider());
    }

    /** Provider internals for tools and tests, e.g. {@code unwrap(IgniteClient.class)}; null when not available. */
    public <X> X unwrap(Class<X> type) {
        return session.unwrap(type);
    }

    // ------------------------------------------------------------------ namespaces

    /**
     * Creates (or confirms) a namespace, visible to all clients. For galaxy it maps to a storage and an index
     * node; the Ignite provider ignores both (pass null).
     */
    public void createNamespace(String namespace, String storage, String indexNode) {
        session.createNamespace(namespace, storage, indexNode);
    }

    public Map<String, GalaxyConfig.NamespaceConfig> listNamespaces() {
        return session.listNamespaces();
    }

    // ------------------------------------------------------------------ datasets

    /** Opens (creating if needed) the cache of one dataset for one domain type. */
    @SuppressWarnings("unchecked")
    public <T> GalaxyCache<T> cache(DatasetId dataset, Class<T> type) {
        GalaxyCache<?> c = caches.computeIfAbsent(dataset, ds ->
                new GalaxyCache<>(ds, type, session, provider(), codec, readPool, config.client(), queryLogger));
        if (c.type() != type) {
            throw new GalaxyException("Dataset " + dataset + " is already open for " + c.type().getName());
        }
        return (GalaxyCache<T>) c;
    }

    /** Datasets that exist for a namespace (or for all namespaces when null). */
    public List<DatasetId> listDatasets(String namespace) {
        return session.listDatasets(namespace);
    }

    /** Deletes a whole dataset: its data and its index. */
    public void dropDataset(DatasetId dataset) {
        caches.remove(dataset);
        session.dropDataset(dataset);
    }

    @Override
    public void close() {
        readPool.shutdown();
        session.close();
        queryLogger.close();
    }
}
