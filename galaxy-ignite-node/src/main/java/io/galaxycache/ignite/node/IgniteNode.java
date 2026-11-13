package io.galaxycache.ignite.node;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.ignite.IgniteNames;
import io.galaxycache.ignite.IgniteSettings;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.PartitionLossPolicy;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.ssl.SslContextFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * One Ignite server node of a galaxy cluster ({@code provider: ignite}), configured from {@code galaxy.yaml}.
 * <ul>
 *   <li>Static discovery over the configured servers; thin clients on {@code clientPort}.</li>
 *   <li>Dataset tables are created by clients from the {@code gx_template} cache template (partitions, backups,
 *       atomicity, partition-loss policy); SQL runs on the Calcite engine.</li>
 *   <li>First start with persistence: the cluster is activated once {@code expectedNodes} servers have joined,
 *       which fixes the baseline; later restarts reactivate when the whole baseline is back.</li>
 *   <li>Once formed, the node publishes the layout fingerprint; a node with another layout stops.</li>
 *   <li>{@code failureMode: failstop}: after the cluster formed, any server leaving calls {@code onFailStop}
 *       ({@link IgniteNodeMain} then stops this node and exits), so all servers stop together.</li>
 * </ul>
 */
public final class IgniteNode implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(IgniteNode.class.getName());
    public static final String DATA_REGION = "gx";

    private final String name;
    private final IgniteSettings settings;
    private final Path workDir;
    private final Consumer<String> onFailStop;
    private final ScheduledExecutorService worker = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "galaxy-ignite-node");
        t.setDaemon(true);
        return t;
    });
    private volatile Ignite ignite;
    private volatile boolean formed;
    private volatile boolean closing;
    private volatile boolean failed;

    public IgniteNode(String name, IgniteSettings settings, Path workDir, Consumer<String> onFailStop) {
        this.name = name;
        this.settings = settings;
        this.workDir = workDir;
        this.onFailStop = onFailStop;
        settings.server(name); // must be a configured server
        if (settings.username() != null) {
            throw new GalaxyException("ignite.username/password: Ignite authentication is not built yet for galaxy server "
                    + "nodes; remove them (use TLS and a private network)");
        }
    }

    public IgniteNode start() {
        ignite = Ignition.start(configuration());
        ignite.events().localListen(evt -> {
            ClusterNode n = ((DiscoveryEvent) evt).eventNode();
            if (!n.isClient()) {
                if (evt.type() == EventType.EVT_NODE_JOINED) {
                    worker.execute(this::checkCluster);
                } else {
                    serverLost(n);
                }
            }
            return true;
        }, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_JOINED);
        // poll as well: activation after a restart happens inside Ignite, not on a join we see
        worker.scheduleWithFixedDelay(this::checkCluster, 0, 500, TimeUnit.MILLISECONDS);
        LOG.log(System.Logger.Level.INFO, "ignite node {0} started ({1}), {2} of {3} servers up",
                name, settings, ignite.cluster().forServers().nodes().size(), settings.expectedNodes());
        return this;
    }

    IgniteConfiguration configuration() {
        IgniteSettings.Server me = settings.server(name);
        TcpDiscoveryVmIpFinder finder = new TcpDiscoveryVmIpFinder().setAddresses(settings.servers().values().stream()
                .map(s -> s.host() + ":" + s.discoveryPort()).toList());
        TcpDiscoverySpi disco = new TcpDiscoverySpi().setIpFinder(finder);
        disco.setLocalAddress(me.host());
        disco.setLocalPort(me.discoveryPort());
        disco.setLocalPortRange(0);
        TcpCommunicationSpi comm = new TcpCommunicationSpi();
        comm.setLocalAddress(me.host());
        comm.setLocalPort(me.communicationPort());
        comm.setLocalPortRange(0);

        DataRegionConfiguration region = new DataRegionConfiguration().setName(DATA_REGION)
                .setMaxSize(settings.dataRegionMb() << 20)
                .setInitialSize(Math.min(settings.dataRegionMb(), 256) << 20)
                .setPersistenceEnabled(settings.persistence())
                .setMetricsEnabled(true);
        DataStorageConfiguration storage = new DataStorageConfiguration().setDefaultDataRegionConfiguration(region)
                .setMetricsEnabled(true);
        if (settings.persistence()) {
            int walMb = (int) Math.max(1, Math.min(1024, Long.getLong("galaxy.ignite.walSegmentMb", 64)));
            storage.setWalSegmentSize(walMb << 20);
        }

        IgniteConfiguration cfg = new IgniteConfiguration()
                .setIgniteInstanceName(name)
                .setConsistentId(name)
                .setDiscoverySpi(disco)
                .setCommunicationSpi(comm)
                .setFailureDetectionTimeout(Math.max(2_000, settings.failAfterMs()))
                .setClientConnectorConfiguration(new ClientConnectorConfiguration()
                        .setHost(me.host()).setPort(me.clientPort()).setPortRange(0)
                        .setSslEnabled(settings.tls()).setUseIgniteSslContextFactory(settings.tls())
                        .setThinClientConfiguration(new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(64)))
                .setDataStorageConfiguration(storage)
                .setSqlConfiguration(new SqlConfiguration().setQueryEnginesConfiguration(
                        new CalciteQueryEngineConfiguration().setDefault(true)))
                .setIncludeEventTypes(EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_JOINED)
                .setMetricsLogFrequency(0)
                .setPeerClassLoadingEnabled(false)
                .setCacheConfiguration(template(), affinityCache(), replicated(IgniteNames.META),
                        replicated(IgniteNames.DATASETS), replicated(IgniteNames.NAMESPACES));
        if (workDir != null) {
            cfg.setWorkDirectory(workDir.toAbsolutePath().toString());
        }
        if (settings.tls()) {
            SslContextFactory ssl = new SslContextFactory();
            ssl.setKeyStoreFilePath(settings.tlsKeyStore());
            ssl.setKeyStorePassword(chars(settings.tlsKeyStorePassword()));
            ssl.setKeyStoreType("PKCS12");
            ssl.setTrustStoreFilePath(settings.tlsTrustStore());
            ssl.setTrustStorePassword(chars(settings.tlsTrustStorePassword()));
            ssl.setTrustStoreType("PKCS12");
            cfg.setSslContextFactory(ssl);
        }
        return cfg;
    }

    private static char[] chars(String s) {
        return s == null ? null : s.toCharArray();
    }

    /** Template for dataset tables (matched by name prefix: {@code TEMPLATE=gx_template}). */
    private CacheConfiguration<Object, Object> template() {
        return partitioned(IgniteNames.TEMPLATE + "*");
    }

    /** An empty cache laid out like every dataset table, used to answer "which node owns this key". */
    private CacheConfiguration<Object, Object> affinityCache() {
        return partitioned(IgniteNames.AFFINITY);
    }

    private CacheConfiguration<Object, Object> partitioned(String cacheName) {
        return new CacheConfiguration<>(cacheName)
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(settings.backups())
                .setAtomicityMode(CacheAtomicityMode.valueOf(settings.atomicity()))
                .setAffinity(new RendezvousAffinityFunction(false, settings.partitions()))
                .setPartitionLossPolicy(PartitionLossPolicy.READ_WRITE_SAFE)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /** Small cluster-wide registries; FULL_SYNC so a read on any node sees the last write. */
    private static CacheConfiguration<Object, Object> replicated(String cacheName) {
        return new CacheConfiguration<>(cacheName).setCacheMode(CacheMode.REPLICATED)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
    }

    /** Activates on first start, publishes / checks the layout fingerprint, and arms fail-stop once formed. */
    private void checkCluster() {
        Ignite ig = ignite;
        if (ig == null || closing || failed) {
            return;
        }
        try {
            int servers = ig.cluster().forServers().nodes().size();
            ClusterState state = ig.cluster().state();
            if (state == ClusterState.INACTIVE && servers >= settings.expectedNodes()
                    && ig.cluster().currentBaselineTopology() == null) {
                LOG.log(System.Logger.Level.INFO, "ignite node {0}: {1} servers up, activating the cluster", name, servers);
                ig.cluster().state(ClusterState.ACTIVE);
                state = ClusterState.ACTIVE;
            }
            if (state != ClusterState.ACTIVE) {
                return;
            }
            if (settings.persistence() && ig.cluster().isBaselineAutoAdjustEnabled()) {
                ig.cluster().baselineAutoAdjustEnabled(false); // membership changes only through setBaseline / rebalance
            }
            IgniteCache<String, String> meta = ig.cache(IgniteNames.META);
            meta.putIfAbsent(IgniteNames.FINGERPRINT_KEY, settings.fingerprint());
            String fp = meta.get(IgniteNames.FINGERPRINT_KEY);
            if (fp == null) {
                return; // partitions still moving right after (re)activation: check again shortly
            }
            if (!settings.fingerprint().equals(fp)) {
                fail("this node runs a different cluster configuration (" + settings.fingerprint() + ") than the cluster ("
                        + fp + "); servers, partitions, backups, atomicity and failureMode must match");
                return;
            }
            if (!formed && servers >= settings.expectedNodes()) {
                formed = true;
                LOG.log(System.Logger.Level.INFO, "ignite node {0}: cluster READY with {1} servers ({2})", name, servers,
                        settings.failStop() ? "fail-stop armed" : "failover mode");
            }
        } catch (RuntimeException e) {
            if (!closing) {
                LOG.log(System.Logger.Level.DEBUG, "ignite node " + name + ": cluster check failed, retrying", e);
            }
        }
    }

    private void serverLost(ClusterNode n) {
        if (closing || !formed || !settings.failStop()) {
            return;
        }
        fail("ignite server " + n.consistentId() + " left the cluster");
    }

    private void fail(String reason) {
        if (failed) {
            return;
        }
        failed = true;
        LOG.log(System.Logger.Level.ERROR, "ignite node {0} FAIL-STOP: {1}", name, reason);
        new Thread(() -> onFailStop.accept(reason), "galaxy-ignite-failstop").start();
    }

    public Ignite ignite() {
        return ignite;
    }

    public String name() {
        return name;
    }

    /** True once the expected servers joined and the cluster is active. */
    public boolean formed() {
        return formed;
    }

    public boolean failed() {
        return failed;
    }

    /** Waits until this node sees the cluster formed. */
    public void awaitFormed(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!formed) {
            if (failed || System.currentTimeMillis() > deadline) {
                throw new GalaxyException("ignite node " + name + " did not see the cluster form within " + timeoutMs + " ms");
            }
            sleep(100);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() {
        closing = true;
        worker.shutdownNow();
        Ignite ig = ignite;
        if (ig != null) {
            try {
                Ignition.stop(name, true);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.WARNING, "stopping ignite node " + name, e);
            }
        }
        try {
            worker.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Stops this node immediately without a graceful leave (as a crash would), for tests. */
    void kill() {
        closing = true;
        Ignition.stop(name, true);
    }

    static List<String> names(java.util.Collection<ClusterNode> nodes) {
        return nodes.stream().map(n -> String.valueOf(n.consistentId())).sorted().toList();
    }
}
