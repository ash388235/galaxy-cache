package io.galaxycache.client.spi;

import io.galaxycache.client.Capability;
import io.galaxycache.client.ClusterState;
import io.galaxycache.client.NodeInfo;
import io.galaxycache.client.NodeStats;
import io.galaxycache.client.NotSupportedException;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Node and cluster operations of one provider cluster. Every operation defaults to {@link NotSupportedException};
 * an implementation overrides what it offers and lists it in {@link #capabilities()}.
 */
public interface ProviderCluster {

    /** Name of this cluster (grid storage name, Ignite cluster name, or "none"). */
    String name();

    Set<Capability> capabilities();

    default List<NodeInfo> nodes() {
        throw unsupported(Capability.CLUSTER_VIEW);
    }

    default ClusterState state() {
        throw unsupported(Capability.CLUSTER_VIEW);
    }

    default void awaitReady(Duration timeout) {
        throw unsupported(Capability.CLUSTER_VIEW);
    }

    /** {@code listener} gets the failure reason once, when the cluster fails for this process. */
    default void onFailure(Consumer<String> listener) {
        throw unsupported(Capability.CLUSTER_VIEW);
    }

    default int partitions() {
        throw unsupported(Capability.PARTITION_MAP);
    }

    default String ownerOf(String key) {
        throw unsupported(Capability.PARTITION_MAP);
    }

    default List<NodeStats> nodeStats() {
        throw unsupported(Capability.NODE_STATS);
    }

    /** Returns records moved (or partitions scheduled) per node. */
    default Map<String, Long> rebalance() {
        throw unsupported(Capability.REBALANCE);
    }

    /** Returns bytes reclaimed per node. */
    default Map<String, Long> compact() {
        throw unsupported(Capability.COMPACT);
    }

    default void activate() {
        throw unsupported(Capability.ACTIVATION);
    }

    default void deactivate() {
        throw unsupported(Capability.ACTIVATION);
    }

    default List<String> baseline() {
        throw unsupported(Capability.BASELINE);
    }

    default void setBaseline(Collection<String> nodeNames) {
        throw unsupported(Capability.BASELINE);
    }

    private NotSupportedException unsupported(Capability c) {
        return new NotSupportedException(c, name());
    }

    /** A cluster with no capabilities, for providers or configurations that have no cluster. */
    static ProviderCluster none(String name) {
        return new ProviderCluster() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public Set<Capability> capabilities() {
                return Set.of();
            }
        };
    }
}
