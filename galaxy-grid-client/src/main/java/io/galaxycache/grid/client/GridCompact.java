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
 * Reclaims the disk held by old record versions, deleted records and rebalancing: every node rewrites its live
 * data into new segments and removes the old ones, in parallel. Run it with no clients writing.
 * <pre>
 * java -cp galaxy-examples.jar io.galaxycache.grid.client.GridCompact --config config/galaxy.yaml --grid grid-1
 * </pre>
 */
public final class GridCompact {

    private GridCompact() {
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
        Map<String, Long> reclaimed = run(GalaxyConfig.load(Path.of(configPath)).storage(grid).settings());
        reclaimed.forEach((node, n) -> System.out.printf("%-10s reclaimed %,.1f GiB%n", node, n / (double) (1L << 30)));
    }

    /** Compacts the grid described by {@code settings}; returns bytes reclaimed per node. */
    public static Map<String, Long> run(Map<String, Object> settings) {
        GridSettings s = GridSettings.from(settings);
        GridTopology topo = s.topology();
        try (GridClient c = new GridClient(topo, 1, 6 * 3600 * 1000, null, s.security())) {
            new ClusterMonitor(c, topo.members(), topo.fingerprint(), s.heartbeatMs(), s.failAfterMs(), true, r -> { })
                    .awaitFormed(s.startupWaitMs());
            long t0 = System.nanoTime();
            Map<String, Long> out = new LinkedHashMap<>();
            c.callAll(GridProtocol.COMPACT, new byte[0]).forEach((m, frames) ->
                    out.put(m.name(), new GridProtocol.In(frames.get(0)).getLong()));
            System.out.printf("compaction of %s finished in %.1f s%n", List.copyOf(out.keySet()), (System.nanoTime() - t0) / 1e9);
            return out;
        }
    }
}
