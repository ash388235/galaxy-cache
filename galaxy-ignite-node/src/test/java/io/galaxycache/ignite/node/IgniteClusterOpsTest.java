package io.galaxycache.ignite.node;

import io.galaxycache.client.ClusterState;
import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.client.GalaxyCluster;
import io.galaxycache.client.NodeInfo;
import io.galaxycache.client.ProviderContractTest.Trade;
import io.galaxycache.common.ClusterUnavailableException;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.query.Filters;
import io.galaxycache.config.GalaxyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** Ignite-only behaviour: fail-stop, persistence across a full restart, baseline / rebalance, activation. */
class IgniteClusterOpsTest {

    static final DatasetId DS = DatasetId.of("trading", "equities", "us", "2026-11-13");

    IgniteTestCluster cluster;

    @AfterEach
    void stop() {
        if (cluster != null) {
            cluster.close();
        }
    }

    private static void await(String what, java.util.function.BooleanSupplier ok) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 20_000;
        while (!ok.getAsBoolean()) {
            if (System.currentTimeMillis() > deadline) {
                fail("timed out waiting for: " + what);
            }
            Thread.sleep(100);
        }
    }

    @Test
    void failStopStopsEveryServerAndFailsTheClients() throws Exception {
        cluster = new IgniteTestCluster(3, null).start();
        try (GalaxyClient client = cluster.client()) {
            GalaxyCache<Trade> c = client.cache(DS, Trade.class);
            c.put("a", new Trade(1));
            AtomicReference<String> reason = new AtomicReference<>();
            client.cluster().onFailure(reason::set);
            assertEquals(ClusterState.READY, client.cluster().state());

            cluster.node("node-2").close(); // a server goes away

            await("node-1 and node-3 to stop themselves",
                    () -> cluster.failStops.containsKey("node-1") && cluster.failStops.containsKey("node-3"));
            assertTrue(cluster.failStops.get("node-1").contains("node-2"), cluster.failStops.toString());
            await("the client to fail", () -> reason.get() != null);
            assertEquals(ClusterState.FAILED, client.cluster().state());
            ClusterUnavailableException e = assertThrows(ClusterUnavailableException.class, () -> c.get("a"));
            assertTrue(e.getMessage().contains("restart"), e.getMessage());
        }
    }

    @Test
    void dataSurvivesAFullRestartWithPersistence() throws Exception {
        System.setProperty("galaxy.ignite.walSegmentMb", "4");
        try {
            cluster = new IgniteTestCluster(2, "  persistence: true\n").start();
            try (GalaxyClient client = cluster.client()) {
                Map<String, Trade> batch = new HashMap<>();
                for (int i = 0; i < 200; i++) {
                    batch.put("T" + i, new Trade(i));
                }
                client.cache(DS, Trade.class).putAll(batch);
                assertEquals(List.of("node-1", "node-2"), client.cluster().baseline());
            }
            cluster.close();
            cluster.nodes.clear();
            cluster.failStops.clear();
            cluster.start(); // same work dir: the baseline comes back and the cluster reactivates
            try (GalaxyClient client = cluster.client()) {
                GalaxyCache<Trade> c = client.cache(DS, Trade.class);
                assertEquals(new Trade(42), c.get("T42").orElseThrow());
                assertEquals(50, c.count(Filters.eq("symbol", "AAPL")));
                assertEquals(200, c.stats().liveRecords());
            }
        } finally {
            System.clearProperty("galaxy.ignite.walSegmentMb");
        }
    }

    @Test
    void aNodeAddedWhileRunningJoinsThroughTheBaseline() throws Exception {
        cluster = new IgniteTestCluster(3, "  expectedNodes: 2\n  backups: 1\n  failureMode: failover\n");
        cluster.nodes.add(cluster.startNode("node-1"));
        cluster.nodes.add(cluster.startNode("node-2"));
        cluster.nodes.forEach(n -> n.awaitFormed(60_000));
        try (GalaxyClient client = cluster.client()) {
            GalaxyCluster cl = client.cluster();
            GalaxyCache<Trade> c = client.cache(DS, Trade.class);
            for (int i = 0; i < 300; i++) {
                c.put("T" + i, new Trade(i));
            }
            assertEquals(2, cl.nodes().size());

            cluster.nodes.add(cluster.startNode("node-3"));
            await("node-3 to join", () -> cl.nodes().size() == 3);
            Map<String, Long> owned = cl.rebalance();
            assertEquals(List.of("node-1", "node-2", "node-3"), List.copyOf(owned.keySet()));
            assertEquals(List.of("node-1", "node-2", "node-3"), cl.baseline());
            await("partitions to spread over 3 nodes",
                    () -> cl.nodes().stream().allMatch(n -> n.partitions() > 200));
            assertEquals(1024, cl.nodes().stream().mapToInt(NodeInfo::partitions).sum());
            assertTrue(List.of("node-1", "node-2", "node-3").contains(cl.ownerOf("T1")));

            // failover mode: losing a node keeps the data (backups: 1)
            cluster.node("node-2").close();
            await("node-2 to leave", () -> cl.nodes().size() == 2);
            assertTrue(cluster.failStops.isEmpty(), "no fail-stop in failover mode");
            assertEquals(new Trade(7), c.get("T7").orElseThrow());
            assertEquals(300, c.count(Filters.all()));
        }
    }

    @Test
    void inMemoryClustersRefuseDeactivation() throws Exception {
        cluster = new IgniteTestCluster(2, null).start();
        try (GalaxyClient client = cluster.client()) {
            GalaxyException e = assertThrows(GalaxyException.class, () -> client.cluster().deactivate());
            assertTrue(e.getMessage().contains("in-memory"), e.getMessage());
            assertEquals(ClusterState.READY, client.cluster().state());
        }
    }

    @Test
    void deactivateAndActivateWithPersistence() throws Exception {
        System.setProperty("galaxy.ignite.walSegmentMb", "4");
        cluster = new IgniteTestCluster(2, "  persistence: true\n").start();
        try (GalaxyClient client = cluster.client()) {
            GalaxyCluster cl = client.cluster();
            client.cache(DS, Trade.class).put("a", new Trade(1));
            cl.deactivate();
            assertEquals(ClusterState.INACTIVE, cl.state());
            cl.activate();
            cl.awaitReady(Duration.ofSeconds(20));
            assertEquals(ClusterState.READY, cl.state());
            assertEquals(new Trade(1), client.cache(DS, Trade.class).get("a").orElseThrow());
        } finally {
            System.clearProperty("galaxy.ignite.walSegmentMb");
        }
    }

    @Test
    void aClientWithAnotherLayoutIsRefused() throws Exception {
        cluster = new IgniteTestCluster(2, null).start();
        String yaml = Files.readString(cluster.dir.resolve("galaxy.yaml")).replace("  persistence: false",
                "  persistence: false\n  partitions: 512");
        Files.writeString(cluster.dir.resolve("other.yaml"), yaml);
        GalaxyConfig other = GalaxyConfig.load(cluster.dir.resolve("other.yaml"));
        GalaxyException e = assertThrows(GalaxyException.class, () -> new GalaxyClient(other));
        assertTrue(e.getMessage().contains("different configuration"), e.getMessage());
    }
}
