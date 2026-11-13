package io.galaxycache.ignite.node;

import io.galaxycache.client.Capability;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.client.ProviderContractTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The provider contract on a 3-server Ignite cluster with TRANSACTIONAL tables, so every optional data operation
 * is exercised (galaxy refuses them; Ignite runs them).
 */
class IgniteProviderContractTest extends ProviderContractTest {

    static IgniteTestCluster cluster;

    @BeforeAll
    static void startCluster() throws IOException {
        cluster = new IgniteTestCluster(3, "  atomicity: TRANSACTIONAL\n").start();
    }

    @AfterAll
    static void stopCluster() {
        cluster.close();
    }

    @Override
    protected GalaxyClient openClient() {
        return cluster.client();
    }

    @Override
    protected String namespace() {
        return "contract";
    }

    @Override
    protected String expectedProvider() {
        return "ignite";
    }

    @Test
    void offersEveryDataCapabilityButCompaction() {
        for (Capability c : new Capability[]{Capability.ATOMIC_OPS, Capability.EXPIRY, Capability.TRANSACTIONS,
                Capability.CHANGE_EVENTS, Capability.CLUSTER_VIEW, Capability.PARTITION_MAP, Capability.NODE_STATS,
                Capability.REBALANCE, Capability.ACTIVATION, Capability.BASELINE, Capability.ONLINE_SCALE}) {
            assertTrue(client.supports(c), c.name());
        }
        assertFalse(client.supports(Capability.COMPACT));
        assertFalse(client.supports(Capability.FAILOVER), "fail-stop with backups 0");
    }
}
