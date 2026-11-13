package io.galaxycache.grid.client;

import io.galaxycache.client.GalaxyClient;
import io.galaxycache.client.GalaxyCluster;
import io.galaxycache.client.ProviderContractTest;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.node.GridNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/** The provider contract on galaxy with a 3-node grid, plus the grid's own cluster operations. */
class GridProviderContractTest extends ProviderContractTest {

    static Path dir;
    static GalaxyConfig config;
    static final List<GridNode> nodes = new ArrayList<>();

    @BeforeAll
    static void startGrid() throws IOException {
        dir = Files.createTempDirectory("grid-contract");
        Path file = dir.resolve("galaxy.yaml");
        Files.writeString(file, """
                cluster: contract
                catalog: { path: catalog }
                storages:
                  grid-1:
                    type: grid
                    path: grid
                    fsync: false
                    heartbeatMs: 200
                    failAfterMs: 3000
                    startupWaitMs: 10000
                    onFailure: throw
                    nodes: { node-1: "localhost:%d", node-2: "localhost:%d", node-3: "localhost:%d" }
                indexNodes:
                  grid-1: { url: "http://localhost:1" }
                namespaces:
                  contract: { storage: grid-1, indexNode: grid-1 }
                client: { readParallelism: 4, readBatchSize: 16 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(GridEndToEndTest.freePort(), GridEndToEndTest.freePort(), GridEndToEndTest.freePort()));
        config = GalaxyConfig.load(file);
        GridSettings s = GridSettings.from(config.storage("grid-1").settings());
        for (String n : List.of("node-1", "node-2", "node-3")) {
            nodes.add(new GridNode(n, s, dir.resolve(n), -1, r -> { }));
        }
        nodes.forEach(GridNode::start);
    }

    @AfterAll
    static void stopGrid() {
        nodes.forEach(GridNode::close);
        nodes.clear();
    }

    @Override
    protected GalaxyClient openClient() {
        return new GalaxyClient(config);
    }

    @Override
    protected String namespace() {
        return "contract";
    }

    @Override
    protected String expectedProvider() {
        return "galaxy";
    }

    @Test
    void rebalanceAndCompactRunOnEveryNode() {
        client.cache(ds, Trade.class).put("x", new Trade(1));
        GalaxyCluster cl = client.cluster();
        assertEquals("grid-1", cl.name());
        Map<String, Long> moved = cl.rebalance();
        assertEquals(List.of("node-1", "node-2", "node-3"), List.copyOf(moved.keySet()));
        assertTrue(moved.values().stream().allMatch(v -> v == 0), "nothing misplaced: " + moved);
        assertEquals(3, cl.compact().size());
        assertEquals(new Trade(1), client.cache(ds, Trade.class).get("x").orElseThrow());
        assertEquals(3, client.cluster("grid-1").nodes().size());
    }
}
