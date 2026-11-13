package io.galaxycache.client.galaxy;

import io.galaxycache.client.Capability;
import io.galaxycache.client.ClusterState;
import io.galaxycache.client.NodeInfo;
import io.galaxycache.client.NodeStats;
import io.galaxycache.client.spi.ProviderCluster;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * A cluster whose name and capabilities are known from the configuration; it connects on the first operation. This
 * keeps {@code client.capabilities()} from connecting to a grid the application may not use.
 */
final class LazyCluster implements ProviderCluster {

    private final String name;
    private final Set<Capability> capabilities;
    private final Supplier<ProviderCluster> open;
    private volatile ProviderCluster real;

    LazyCluster(String name, Set<Capability> capabilities, Supplier<ProviderCluster> open) {
        this.name = name;
        this.capabilities = capabilities;
        this.open = open;
    }

    private ProviderCluster real() {
        ProviderCluster r = real;
        if (r == null) {
            synchronized (this) {
                if (real == null) {
                    real = open.get();
                }
                r = real;
            }
        }
        return r;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Set<Capability> capabilities() {
        return capabilities;
    }

    @Override
    public List<NodeInfo> nodes() {
        return real().nodes();
    }

    @Override
    public ClusterState state() {
        return real().state();
    }

    @Override
    public void awaitReady(Duration timeout) {
        real().awaitReady(timeout);
    }

    @Override
    public void onFailure(Consumer<String> listener) {
        real().onFailure(listener);
    }

    @Override
    public int partitions() {
        return real().partitions();
    }

    @Override
    public String ownerOf(String key) {
        return real().ownerOf(key);
    }

    @Override
    public List<NodeStats> nodeStats() {
        return real().nodeStats();
    }

    @Override
    public Map<String, Long> rebalance() {
        return real().rebalance();
    }

    @Override
    public Map<String, Long> compact() {
        return real().compact();
    }

    @Override
    public void activate() {
        real().activate();
    }

    @Override
    public void deactivate() {
        real().deactivate();
    }

    @Override
    public List<String> baseline() {
        return real().baseline();
    }

    @Override
    public void setBaseline(Collection<String> nodeNames) {
        real().setBaseline(nodeNames);
    }
}
