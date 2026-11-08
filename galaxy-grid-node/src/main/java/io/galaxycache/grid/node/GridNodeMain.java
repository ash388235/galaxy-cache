package io.galaxycache.grid.node;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.api.GridTopology;

import java.nio.file.Path;
import java.util.Map;

/**
 * Starts one grid node from galaxy.yaml:
 * <pre>
 * java -jar galaxy-grid-node.jar --config config/galaxy.yaml --grid grid-1 --node node-1
 * </pre>
 * The grid is a storage of {@code type: grid}; its settings list the nodes ({@code nodes}), the data folder
 * ({@code path}, each node uses {@code path/<node>}) and optionally {@code adminPortOffset} (default 100: the
 * HTTP stats port is the protocol port + 100).
 */
public final class GridNodeMain {

    private GridNodeMain() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy.yaml";
        String grid = null;
        String node = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--grid" -> grid = args[++i];
                case "--node" -> node = args[++i];
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        if (grid == null || node == null) {
            throw new IllegalArgumentException("usage: --config galaxy.yaml --grid <storage name> --node <node name>");
        }
        GridNode n = start(GalaxyConfig.load(Path.of(configPath)), grid, node);
        Runtime.getRuntime().addShutdownHook(new Thread(n::close));
        System.out.printf("grid node %s of %s on port %d (stats: http://localhost:%d/v1/stats)%n", node, grid,
                n.port(), n.port() + adminOffset(GalaxyConfig.load(Path.of(configPath)).storage(grid).settings()));
        Thread.currentThread().join();
    }

    public static GridNode start(GalaxyConfig config, String grid, String node) {
        return start(config, grid, node, reason -> {
            // fail-stop: exit so the supervisor/operator sees it; give the log a moment to flush
            System.err.println("grid node " + node + " stopped: " + reason);
            Thread t = new Thread(() -> System.exit(2));
            t.start();
        });
    }

    public static GridNode start(GalaxyConfig config, String grid, String node,
                                 java.util.function.Consumer<String> onFailStop) {
        GalaxyConfig.StorageConfig sc = config.storage(grid);
        if (!"grid".equals(sc.type())) {
            throw new GalaxyException("storage '" + grid + "' is of type " + sc.type() + ", not grid");
        }
        Map<String, Object> s = sc.settings();
        GridSettings settings = GridSettings.from(s);
        GridTopology.Member me = settings.topology().member(node);
        Object path = s.get("path");
        if (path == null) {
            throw new GalaxyException("grid storage '" + grid + "' needs a 'path' for node data");
        }
        return new GridNode(node, settings, Path.of(path.toString()).resolve(node), me.port() + adminOffset(s),
                onFailStop).start();
    }

    private static int adminOffset(Map<String, Object> s) {
        Object o = s.get("adminPortOffset");
        return o == null ? 100 : Integer.parseInt(o.toString());
    }
}