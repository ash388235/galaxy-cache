package io.galaxycache.ignite;

import io.galaxycache.client.Capability;
import io.galaxycache.client.spi.ProviderCache;
import io.galaxycache.client.spi.ProviderCluster;
import io.galaxycache.client.spi.ProviderSession;
import io.galaxycache.common.ClusterUnavailableException;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.storage.DatasetMetadata;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.SslMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.ClientConfiguration;

import javax.cache.Cache;
import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A thin-client connection to an Ignite cluster started with {@code IgniteNodeMain}. Opening waits until the
 * expected servers are up, the cluster is active and its layout fingerprint matches this configuration. Under
 * {@code failureMode: failstop} a heartbeat watches the servers: if one leaves (or the cluster stops answering for
 * {@code failAfterMs}) every call fails with {@link ClusterUnavailableException}, and with {@code onFailure: exit}
 * the process exits with status 75, exactly like a galaxy grid client.
 */
public final class IgniteProviderSession implements ProviderSession {

    private static final System.Logger LOG = System.getLogger(IgniteProviderSession.class.getName());

    /** Dataset metadata plus its table layout, as stored in {@link IgniteNames#DATASETS}. */
    public record DatasetEntry(DatasetMetadata metadata, IgniteTable table) {
    }

    record NamespaceEntry(String name, Instant createdAt) {
    }

    private final GalaxyConfig config;
    private final IgniteSettings settings;
    private final IgniteClient client;
    private final ClientCache<String, String> datasets;
    private final ClientCache<String, String> namespaces;
    private final IgniteProviderCluster cluster;
    private final List<Consumer<String>> failureListeners = new CopyOnWriteArrayList<>();
    private final ScheduledExecutorService monitor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "galaxy-ignite-heartbeat");
        t.setDaemon(true);
        return t;
    });
    private volatile String failure;
    private volatile boolean closed;
    private Set<UUID> knownServers = Set.of();
    private long lastOk;

    IgniteProviderSession(GalaxyConfig config, IgniteSettings settings) {
        this.config = config;
        this.settings = settings;
        this.client = Ignition.startClient(clientConfiguration(settings));
        try {
            awaitReady(settings.startupWaitMs());
            this.datasets = client.cache(IgniteNames.DATASETS); // replicated, FULL_SYNC: defined by the servers
            this.namespaces = client.cache(IgniteNames.NAMESPACES);
        } catch (RuntimeException e) {
            client.close();
            throw e;
        }
        this.cluster = new IgniteProviderCluster(this);
        if (settings.failStop()) {
            knownServers = serverIds();
            lastOk = System.currentTimeMillis();
            monitor.scheduleWithFixedDelay(this::heartbeat, settings.heartbeatMs(), settings.heartbeatMs(), TimeUnit.MILLISECONDS);
        }
        LOG.log(System.Logger.Level.INFO, "ignite cluster ready: {0} servers, {1}", knownServers.size(), settings);
    }

    static ClientConfiguration clientConfiguration(IgniteSettings s) {
        ClientConfiguration c = new ClientConfiguration()
                .setAddresses(s.addresses().toArray(String[]::new))
                .setTimeout(s.timeoutMs())
                .setPartitionAwarenessEnabled(true)
                .setHeartbeatEnabled(true);
        if (s.username() != null) {
            c.setUserName(s.username()).setUserPassword(s.password());
        }
        if (s.tls()) {
            c.setSslMode(SslMode.REQUIRED);
            if (s.tlsKeyStore() != null) {
                c.setSslClientCertificateKeyStorePath(s.tlsKeyStore())
                        .setSslClientCertificateKeyStorePassword(s.tlsKeyStorePassword())
                        .setSslClientCertificateKeyStoreType("PKCS12");
            }
            if (s.tlsTrustStore() != null) {
                c.setSslTrustCertificateKeyStorePath(s.tlsTrustStore())
                        .setSslTrustCertificateKeyStorePassword(s.tlsTrustStorePassword())
                        .setSslTrustCertificateKeyStoreType("PKCS12");
            }
        }
        return c;
    }

    // ------------------------------------------------------------------ readiness and fail-stop

    /** Waits until the expected servers are up, the cluster is active and its fingerprint matches ours. */
    void awaitReady(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String problem;
        while (true) {
            try {
                problem = readinessProblem();
                if (problem == null) {
                    return;
                }
            } catch (GalaxyException e) {
                throw e; // a wrong configuration: waiting cannot help
            } catch (RuntimeException e) {
                problem = "not reachable: " + e.getMessage();
            }
            if (System.currentTimeMillis() > deadline) {
                throw new ClusterUnavailableException("ignite cluster not ready after " + timeoutMs + " ms: " + problem);
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new GalaxyException("interrupted", e);
            }
        }
    }

    /** Null when ready; otherwise why not. Throws {@link GalaxyException} for a layout mismatch. */
    String readinessProblem() {
        int servers = client.cluster().forServers().nodes().size();
        if (servers < settings.expectedNodes()) {
            return servers + " of " + settings.expectedNodes() + " servers up";
        }
        org.apache.ignite.cluster.ClusterState st = client.cluster().state();
        if (st != org.apache.ignite.cluster.ClusterState.ACTIVE) {
            return "cluster is " + st;
        }
        if (!client.cacheNames().containsAll(java.util.List.of(IgniteNames.META, IgniteNames.DATASETS, IgniteNames.NAMESPACES))) {
            return "servers have not published their configuration yet";
        }
        Object fp = client.cache(IgniteNames.META).get(IgniteNames.FINGERPRINT_KEY);
        if (fp == null) {
            return "servers have not published their configuration yet";
        }
        if (!settings.fingerprint().equals(fp)) {
            throw new GalaxyException("the ignite cluster runs a different configuration (" + fp + " vs "
                    + settings.fingerprint() + "); servers, partitions, backups, atomicity and failureMode must match");
        }
        return null;
    }

    Set<UUID> serverIds() {
        Set<UUID> ids = new HashSet<>();
        for (ClusterNode n : client.cluster().forServers().nodes()) {
            ids.add(n.id());
        }
        return ids;
    }

    private void heartbeat() {
        if (failure != null || closed) {
            return;
        }
        long now = System.currentTimeMillis();
        try {
            Set<UUID> ids = serverIds();
            if (!ids.containsAll(knownServers)) {
                Set<UUID> gone = new HashSet<>(knownServers);
                gone.removeAll(ids);
                fail("ignite server(s) " + gone + " left the cluster (fail-stop)");
                return;
            }
            lastOk = now;
        } catch (RuntimeException e) {
            if (now - lastOk >= settings.failAfterMs()) {
                fail("ignite cluster unreachable for " + (now - lastOk) + " ms: " + e.getMessage());
            }
        }
    }

    /** Ends the cluster for this process: every later call throws; listeners run; with onFailure: exit, exits 75. */
    void fail(String reason) {
        synchronized (this) {
            if (failure != null) {
                return;
            }
            failure = reason;
        }
        LOG.log(System.Logger.Level.ERROR, "IGNITE CLUSTER FAILURE: {0}", reason);
        monitor.shutdown();
        for (Consumer<String> l : failureListeners) {
            try {
                l.accept(reason);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.ERROR, "failure listener threw", e);
            }
        }
        if (settings.exitOnFailure()) {
            System.err.println("[galaxy] ignite cluster failed: " + reason + " -> exiting with status "
                    + IgniteSettings.EXIT_STATUS + " (onFailure: exit)");
            new Thread(() -> System.exit(IgniteSettings.EXIT_STATUS), "galaxy-ignite-exit").start();
        }
    }

    void onFailure(Consumer<String> listener) {
        String already;
        synchronized (this) {
            already = failure;
            if (already == null) {
                failureListeners.add(listener);
            }
        }
        if (already != null) {
            listener.accept(already);
        }
    }

    String failure() {
        return failure;
    }

    /** Throws when the cluster has failed for this process. */
    void ensureUp() {
        String f = failure;
        if (f != null) {
            throw new ClusterUnavailableException("ignite cluster unavailable: " + f + " (restart this process after the cluster is back)");
        }
    }

    // ------------------------------------------------------------------ SPI

    @Override
    public String providerName() {
        return IgniteCacheProvider.NAME;
    }

    @Override
    public Set<Capability> capabilities() {
        EnumSet<Capability> c = EnumSet.of(Capability.ATOMIC_OPS, Capability.EXPIRY, Capability.CHANGE_EVENTS);
        if (settings.transactional()) {
            c.add(Capability.TRANSACTIONS);
        }
        return c;
    }

    @Override
    public ProviderCache open(DatasetId dataset, DatasetMetadata wanted, Map<String, Class<?>> indexedTypes) {
        ensureUp();
        requireNamespace(dataset.namespace());
        DatasetEntry entry = entry(dataset);
        if (entry == null) {
            IgniteTable table = IgniteTable.of(dataset, indexedTypes);
            sql(table.createTableSql(IgniteNames.TEMPLATE, settings.atomicity(), settings.backups()));
            for (String idx : table.createIndexSql()) {
                sql(idx);
            }
            datasets.putIfAbsent(dataset.key(), Json.write(new DatasetEntry(wanted, table)));
            entry = entry(dataset);
        }
        return new IgniteProviderCache(this, dataset, entry);
    }

    DatasetEntry entry(DatasetId dataset) {
        String json = datasets.get(dataset.key());
        return json == null ? null : Json.read(json, DatasetEntry.class);
    }

    @Override
    public List<DatasetId> listDatasets(String namespace) {
        ensureUp();
        List<DatasetId> out = new ArrayList<>();
        try (QueryCursor<Cache.Entry<String, String>> cur = datasets.query(new ScanQuery<>())) {
            for (Cache.Entry<String, String> e : cur) {
                DatasetId id = DatasetId.parse(e.getKey());
                if (namespace == null || namespace.equals(id.namespace())) {
                    out.add(id);
                }
            }
        }
        out.sort(null);
        return out;
    }

    @Override
    public void dropDataset(DatasetId dataset) {
        ensureUp();
        DatasetEntry e = entry(dataset);
        if (e != null) {
            sql("DROP TABLE IF EXISTS " + e.table().qualified());
            datasets.remove(dataset.key());
        }
    }

    @Override
    public void createNamespace(String namespace, String storage, String indexNode) {
        ensureUp();
        if (!DatasetId.validSegment(namespace)) {
            throw new GalaxyException("invalid namespace name '" + namespace + "'");
        }
        namespaces.putIfAbsent(namespace, Json.write(new NamespaceEntry(namespace, Instant.now())));
    }

    @Override
    public Map<String, GalaxyConfig.NamespaceConfig> listNamespaces() {
        ensureUp();
        Map<String, GalaxyConfig.NamespaceConfig> out = new TreeMap<>();
        config.namespaces().keySet().forEach(ns -> out.put(ns, new GalaxyConfig.NamespaceConfig(null, null)));
        try (QueryCursor<Cache.Entry<String, String>> cur = namespaces.query(new ScanQuery<>())) {
            cur.forEach(e -> out.put(e.getKey(), new GalaxyConfig.NamespaceConfig(null, null)));
        }
        return out;
    }

    private void requireNamespace(String ns) {
        if (!config.namespaces().containsKey(ns) && namespaces.get(ns) == null) {
            throw new GalaxyException("Unknown namespace '" + ns + "'. Add it to 'namespaces' in the config or call createNamespace.");
        }
    }

    @Override
    public ProviderCluster cluster(String name) {
        if (name != null && !name.equals(cluster.name())) {
            throw new GalaxyException("unknown cluster '" + name + "'; this client is connected to '" + cluster.name() + "'");
        }
        return cluster;
    }

    @Override
    public <T> T unwrap(Class<T> type) {
        if (type.isInstance(client)) {
            return type.cast(client);
        }
        return ProviderSession.super.unwrap(type);
    }

    // ------------------------------------------------------------------ plumbing

    IgniteClient client() {
        return client;
    }

    IgniteSettings settings() {
        return settings;
    }

    /** Runs one SQL statement on the PUBLIC schema with bound parameters. */
    List<List<?>> sql(String sql, Object... args) {
        ensureUp();
        try (FieldsQueryCursor<List<?>> cur = client.query(new SqlFieldsQuery(sql).setArgs(args)
                .setSchema(IgniteTable.SCHEMA).setTimeout(settings.timeoutMs(), TimeUnit.MILLISECONDS))) {
            return cur.getAll();
        }
    }

    @Override
    public void close() {
        closed = true;
        monitor.shutdownNow();
        client.close();
    }
}
