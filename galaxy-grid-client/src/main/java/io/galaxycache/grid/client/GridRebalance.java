package io.galaxycache.grid.client;

import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.ClusterMonitor;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.api.GridTopology;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Moves records to their owners after the grid's member list changed (nodes added or removed while the grid
 * was stopped). Every node moves what it holds but no longer owns straight to the owner, in parallel.
 * <pre>
 * java -cp galaxy-examples.jar io.galaxycache.grid.client.GridRebalance --config config/galaxy.yaml --grid grid-1
 * </pre>
 * Run it with no other clients writing. It is safe to run again.
 */
public final class GridRebalance {

    private GridRebalance() {
    }

    public static void main(String[] args) {
        String configPath = "config/galaxy.yaml";
        String grid = "grid-1";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--grid" -> grid = args[++i];
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        Map<String, Long> moved = run(GalaxyConfig.load(Path.of(configPath)).storage(grid).settings());
        moved.forEach((node, n) -> System.out.printf("%-10s moved %,d records to their owners%n", node, n));
    }

    /** Rebalances the grid described by {@code settings}; returns records moved per node. */
    public static Map<String, Long> run(Map<String, Object> settings) {
        GridSettings s = GridSettings.from(settings);
        GridTopology topo = s.topology();
        try (GridClient c = new GridClient(topo, 1, 6 * 3600 * 1000, null, s.security())) {
            new ClusterMonitor(c, topo.members(), topo.fingerprint(), s.heartbeatMs(), s.failAfterMs(), true, r -> { })
                    .awaitFormed(s.startupWaitMs());
            long t0 = System.nanoTime();
            Map<String, Long> out = new LinkedHashMap<>();
            c.callAll(GridProtocol.REBALANCE, new byte[0]).forEach((m, frames) ->
                    out.put(m.name(), new GridProtocol.In(frames.get(0)).getLong()));
            System.out.printf("rebalance of %s finished in %.1f s%n", List.copyOf(out.keySet()), (System.nanoTime() - t0) / 1e9);
            return out;
        }
    }
}
