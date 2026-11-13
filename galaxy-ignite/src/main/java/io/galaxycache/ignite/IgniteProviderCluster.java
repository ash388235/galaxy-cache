package io.galaxycache.ignite;

import com.fasterxml.jackson.core.type.TypeReference;
import io.galaxycache.client.Capability;
import io.galaxycache.client.ClusterState;
import io.galaxycache.client.NodeInfo;
import io.galaxycache.client.NodeStats;
import io.galaxycache.client.spi.ProviderCluster;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import org.apache.ignite.client.ClientException;

import java.time.Duration;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Cluster operations on Ignite. State and activation use the thin client directly; everything that needs a
 * server's view (partition owners, per-node sizes, baseline) runs as {@link IgniteNames#ADMIN_TASK} on the
 * servers. Compaction is not offered: Ignite reuses freed pages itself.
 */
final class IgniteProviderCluster implements ProviderCluster {

    private final IgniteProviderSession session;

    IgniteProviderCluster(IgniteProviderSession session) {
        this.session = session;
    }

    @Override
    public String name() {
        return session.settings().clusterName();
    }

    @Override
    public Set<Capability> capabilities() {
        EnumSet<Capability> c = EnumSet.of(Capability.CLUSTER_VIEW, Capability.PARTITION_MAP, Capability.NODE_STATS,
                Capability.REBALANCE, Capability.ACTIVATION, Capability.BASELINE, Capability.ONLINE_SCALE);
        if (!session.settings().failStop() && session.settings().backups() > 0) {
            c.add(Capability.FAILOVER);
        }
        return c;
    }

    /** Runs one admin command on the servers; the answer is JSON. */
    String admin(String command, String arg) {
        session.ensureUp();
        try {
            return session.client().compute(session.client().cluster().forServers())
                    .withTimeout(Math.max(60_000, session.settings().timeoutMs()))
                    .execute(IgniteNames.ADMIN_TASK, command + "\n" + (arg == null ? "" : arg));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GalaxyException("interrupted", e);
        } catch (ClientException e) {
            throw new GalaxyException("ignite admin command '" + command + "' failed: " + e.getMessage(), e);
        }
    }

    @Override
    public List<NodeInfo> nodes() {
        return Json.MAPPER.convertValue(readTree(admin("nodes", null)), new TypeReference<List<NodeInfo>>() { });
    }

    @Override
    public ClusterState state() {
        if (session.failure() != null) {
            return ClusterState.FAILED;
        }
        try {
            String problem = session.readinessProblem();
            if (problem == null) {
                return ClusterState.READY;
            }
            return session.client().cluster().state() == org.apache.ignite.cluster.ClusterState.INACTIVE
                    && session.client().cluster().forServers().nodes().size() >= session.settings().expectedNodes()
                    ? ClusterState.INACTIVE : ClusterState.FORMING;
        } catch (GalaxyException e) {
            return ClusterState.FAILED;
        } catch (RuntimeException e) {
            return ClusterState.FAILED;
        }
    }

    @Override
    public void awaitReady(Duration timeout) {
        session.ensureUp();
        session.awaitReady(timeout.toMillis());
    }

    @Override
    public void onFailure(Consumer<String> listener) {
        session.onFailure(listener);
    }

    @Override
    public int partitions() {
        return session.settings().partitions();
    }

    @Override
    public String ownerOf(String key) {
        return Json.read(admin("ownerOf", key), String.class);
    }

    @Override
    public List<NodeStats> nodeStats() {
        return Json.MAPPER.convertValue(readTree(admin("nodeStats", null)), new TypeReference<List<NodeStats>>() { });
    }

    /** Makes every server that is up part of the baseline; Ignite then moves partitions in the background. */
    @Override
    public Map<String, Long> rebalance() {
        return Json.MAPPER.convertValue(readTree(admin("rebalance", null)), new TypeReference<Map<String, Long>>() { });
    }

    @Override
    public void activate() {
        session.ensureUp();
        session.client().cluster().state(org.apache.ignite.cluster.ClusterState.ACTIVE);
    }

    /** Refused without persistence: deactivating an in-memory Ignite cluster deletes all its data. */
    @Override
    public void deactivate() {
        session.ensureUp();
        if (!session.settings().persistence()) {
            throw new GalaxyException("deactivate() would delete every record of this in-memory Ignite cluster "
                    + "(persistence: false); stop the servers instead");
        }
        session.client().cluster().state(org.apache.ignite.cluster.ClusterState.INACTIVE);
    }

    @Override
    public List<String> baseline() {
        return Json.MAPPER.convertValue(readTree(admin("baseline", null)), new TypeReference<List<String>>() { });
    }

    @Override
    public void setBaseline(Collection<String> nodeNames) {
        if (nodeNames.isEmpty()) {
            throw new IllegalArgumentException("the baseline needs at least one node");
        }
        admin("setBaseline", String.join(",", nodeNames));
    }

    private static com.fasterxml.jackson.databind.JsonNode readTree(String json) {
        try {
            return Json.MAPPER.readTree(json);
        } catch (java.io.IOException e) {
            throw new GalaxyException("bad answer from the ignite admin task: " + json, e);
        }
    }
}
