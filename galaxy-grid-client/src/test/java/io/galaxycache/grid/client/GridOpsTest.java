package io.galaxycache.grid.client;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.node.GridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GridOpsTest {

    @TempDir
    Path tmp;
    final List<GridNode> nodes = new ArrayList<>();

    @AfterEach
    void stop() {
        nodes.forEach(GridNode::close);
    }

    private GalaxyConfig config(String nodesYaml) throws IOException {
        Path file = tmp.resolve("galaxy.yaml");
        Files.writeString(file, """
                cluster: ops
                catalog: { path: catalog }
                storages:
                  grid-1:
                    type: grid
                    path: grid
                    fsync: false
                    heartbeatMs: 200
                    failAfterMs: 1000
                    startupWaitMs: 5000
                    nodes: { %s }
                indexNodes:
                  grid-1: { url: "http://localhost:1" }
                namespaces:
                  orders: { storage: grid-1, indexNode: grid-1 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(nodesYaml));
        return GalaxyConfig.load(file);
    }

    private static HttpResponse<String> get(int port, String path) throws Exception {
        return HttpClient.newHttpClient().send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path)).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    void healthIsNotOkUntilTheGridHasFormed() throws Exception {
        int admin = GridEndToEndTest.freePort();
        GalaxyConfig c = config("node-1: \"localhost:%d\", node-2: \"localhost:%d\""
                .formatted(GridEndToEndTest.freePort(), GridEndToEndTest.freePort()));
        nodes.add(new GridNode("node-1", GridSettings.from(c.storage("grid-1").settings()), tmp.resolve("n1"), admin, r -> { }).start());
        HttpResponse<String> r = get(admin, "/health");
        assertEquals(503, r.statusCode());
        assertTrue(r.body().contains("FORMING"), r.body());
    }

    @Test
    void metricsDescribeTheNodeAndItsDatasets() throws Exception {
        int admin = GridEndToEndTest.freePort();
        GalaxyConfig c = config("solo: \"localhost:%d\"".formatted(GridEndToEndTest.freePort()));
        GridNode node = new GridNode("solo", GridSettings.from(c.storage("grid-1").settings()), tmp.resolve("n1"), admin, r -> { }).start();
        nodes.add(node);
        long deadline = System.currentTimeMillis() + 5_000;
        while (!node.state().equals("READY") && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(200, get(admin, "/health").statusCode());
        DatasetId ds = DatasetId.of("orders", "eq", "us", "2026-11-08");
        try (GalaxyClient client = new GalaxyClient(c)) {
            GalaxyCache<GridEndToEndTest.Order> orders = client.cache(ds, GridEndToEndTest.Order.class);
            for (int i = 0; i < 25; i++) {
                orders.put(new GridEndToEndTest.Order(i));
            }
        }
        String m = get(admin, "/metrics").body();
        assertTrue(m.contains("galaxy_grid_node_ready{node=\"solo\"} 1"), m);
        assertTrue(m.contains("galaxy_grid_partitions_owned{node=\"solo\"} 1024"), m);
        assertTrue(m.contains("galaxy_grid_dataset_records{node=\"solo\",dataset=\"" + ds.key() + "\"} 25"), m);
        assertTrue(m.contains("galaxy_grid_dataset_index_documents{node=\"solo\",dataset=\"" + ds.key() + "\"} 25"), m);
        assertTrue(m.contains("galaxy_grid_dataset_unindexed_records{node=\"solo\",dataset=\"" + ds.key() + "\"} 0"), m);
        assertEquals(1, m.lines().filter(l -> l.equals("# TYPE galaxy_grid_node_ready gauge")).count());
        assertTrue(m.lines().allMatch(l -> l.startsWith("#") || l.matches("[a-z_]+\\{.*} -?\\d+")), m);
    }
}
