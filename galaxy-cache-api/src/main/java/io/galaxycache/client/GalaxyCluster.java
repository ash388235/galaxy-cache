package io.galaxycache.client;

import io.galaxycache.client.spi.ProviderCluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Node and cluster operations, the same for every provider. An operation whose {@link Capability} this cluster
 * lacks throws {@link NotSupportedException}; check with {@link #supports(Capability)}.
 * Get one with {@link GalaxyClient#cluster()}.
 */
public final class GalaxyCluster {

    private final ProviderCluster cluster;
    private final String provider;

    GalaxyCluster(ProviderCluster cluster, String provider) {
        this.cluster = cluster;
        this.provider = provider;
    }

    public String name() {
        return cluster.name();
    }

    public Set<Capability> capabilities() {
        return Set.copyOf(cluster.capabilities());
    }

    public boolean supports(Capability c) {
        return cluster.capabilities().contains(c);
    }

    private void require(Capability c) {
        if (!supports(c)) {
            throw new NotSupportedException(c, provider, "cluster '" + cluster.name() + "'");
        }
    }

    public List<NodeInfo> nodes() {
        require(Capability.CLUSTER_VIEW);
        return cluster.nodes();
    }

    public ClusterState state() {
        require(Capability.CLUSTER_VIEW);
        return cluster.state();
    }

    /** Waits until every node is up and the cluster accepts data operations. */
    public void awaitReady(Duration timeout) {
        require(Capability.CLUSTER_VIEW);
        cluster.awaitReady(timeout);
    }

    /** {@code listener} receives the reason once if the cluster fails for this process (fail-stop). */
    public void onFailure(Consumer<String> listener) {
        require(Capability.CLUSTER_VIEW);
        cluster.onFailure(listener);
    }

    public int partitions() {
        require(Capability.PARTITION_MAP);
        return cluster.partitions();
    }

    /** Name of the node that owns {@code key}'s partition (primary copy). */
    public String ownerOf(String key) {
        require(Capability.PARTITION_MAP);
        return cluster.ownerOf(key);
    }

    public List<NodeStats> nodeStats() {
        require(Capability.NODE_STATS);
        return cluster.nodeStats();
    }

    /** Moves data to the nodes that own it after a membership change; result per node. Run with writers stopped. */
    public Map<String, Long> rebalance() {
        require(Capability.REBALANCE);
        return cluster.rebalance();
    }

    /** Reclaims disk on every node; bytes reclaimed per node. Run with writers stopped. */
    public Map<String, Long> compact() {
        require(Capability.COMPACT);
        return cluster.compact();
    }

    public void activate() {
        require(Capability.ACTIVATION);
        cluster.activate();
    }

    public void deactivate() {
        require(Capability.ACTIVATION);
        cluster.deactivate();
    }

    /** Names of the nodes that hold data (Ignite baseline topology). */
    public List<String> baseline() {
        require(Capability.BASELINE);
        return cluster.baseline();
    }

    public void setBaseline(Collection<String> nodeNames) {
        require(Capability.BASELINE);
        cluster.setBaseline(nodeNames);
    }

    @Override
    public String toString() {
        return "GalaxyCluster[" + provider + "/" + cluster.name() + "]";
    }
}
