package io.galaxycache.grid.client;

import io.galaxycache.client.Capability;
import io.galaxycache.client.ClusterState;
import io.galaxycache.client.NodeInfo;
import io.galaxycache.client.NodeStats;
import io.galaxycache.client.spi.ProviderCluster;
import io.galaxycache.common.Json;
import io.galaxycache.grid.api.ClusterMonitor;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.grid.api.NodeStatus;
import io.galaxycache.index.api.Messages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Cluster operations of a galaxy grid. The member list is static configuration, so there is no activation,
 * baseline or online scaling; rebalance and compact are the grid's own REBALANCE / COMPACT operations.
 */
final class GridProviderCluster implements ProviderCluster {

    static final Set<Capability> CAPABILITIES = EnumSet.of(Capability.CLUSTER_VIEW, Capability.PARTITION_MAP,
            Capability.NODE_STATS, Capability.REBALANCE, Capability.COMPACT);
    /** Rebalance and compaction rewrite whole nodes; they may take hours. */
    private static final long LONG_OP_MS = 6L * 3600 * 1000;
    private static final long PING_MS = 5_000;

    private final String name;
    private final GridClient grid;

    GridProviderCluster(String name, GridClient grid) {
        this.name = name;
        this.grid = grid;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Set<Capability> capabilities() {
        return CAPABILITIES;
    }

    @Override
    public List<NodeInfo> nodes() {
        Map<String, Integer> owned = grid.topology().partitionCounts();
        List<NodeInfo> out = new ArrayList<>();
        for (GridTopology.Member m : grid.topology().members()) {
            String state;
            try {
                state = ClusterMonitor.ping(grid, m, PING_MS).state();
            } catch (RuntimeException e) {
                state = "UNREACHABLE";
            }
            out.add(new NodeInfo(m.name(), m.host() + ":" + m.port(), state, owned.getOrDefault(m.name(), 0)));
        }
        return out;
    }

    @Override
    public ClusterState state() {
        if (grid.failure() != null) {
            return ClusterState.FAILED;
        }
        boolean forming = false;
        for (GridTopology.Member m : grid.topology().members()) {
            try {
                NodeStatus s = ClusterMonitor.ping(grid, m, PING_MS);
                if (NodeStatus.FAILED.equals(s.state())) {
                    return ClusterState.FAILED;
                }
                forming |= !NodeStatus.READY.equals(s.state());
            } catch (RuntimeException e) {
                return ClusterState.FAILED;
            }
        }
        return forming ? ClusterState.FORMING : ClusterState.READY;
    }

    @Override
    public void awaitReady(Duration timeout) {
        GridTopology t = grid.topology();
        new ClusterMonitor(grid, t.members(), t.fingerprint(), 500, Long.MAX_VALUE, true, r -> { })
                .awaitFormed(timeout.toMillis());
    }

    @Override
    public void onFailure(Consumer<String> listener) {
        grid.onFailure(listener);
    }

    @Override
    public int partitions() {
        return grid.topology().partitions();
    }

    @Override
    public String ownerOf(String key) {
        return grid.topology().ownerOf(key).name();
    }

    @Override
    public List<NodeStats> nodeStats() {
        List<NodeStats> out = new ArrayList<>();
        grid.callAll(GridProtocol.NODE_STATS, new byte[0]).forEach((m, frames) -> {
            Messages.StatsResponse s = Json.read(frames.get(0), Messages.StatsResponse.class);
            long records = s.datasets() == null ? 0 : s.datasets().stream().mapToLong(Messages.DatasetIndexStats::documents).sum();
            out.add(new NodeStats(m.name(), records, -1, s.counters()));
        });
        return out;
    }

    @Override
    public Map<String, Long> rebalance() {
        return longOp(GridProtocol.REBALANCE);
    }

    @Override
    public Map<String, Long> compact() {
        return longOp(GridProtocol.COMPACT);
    }

    private Map<String, Long> longOp(byte op) {
        Map<GridTopology.Member, CompletableFuture<List<byte[]>>> fs = new LinkedHashMap<>();
        grid.topology().members().forEach(m -> fs.put(m, grid.send(m, op, new byte[0])));
        Map<String, Long> out = new LinkedHashMap<>();
        fs.forEach((m, f) -> out.put(m.name(), new GridProtocol.In(grid.await(m, f, LONG_OP_MS).get(0)).getLong()));
        return out;
    }
}
