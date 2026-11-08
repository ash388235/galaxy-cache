package io.galaxycache.grid.client;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.client.GalaxyClientAccess;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.GalaxyKey;
import io.galaxycache.common.domain.Indexed;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridRecords;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.grid.node.GridNode;
import io.galaxycache.storage.StoredRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static io.galaxycache.common.query.Filters.*;
import static org.junit.jupiter.api.Assertions.*;

class GridEndToEndTest {

    @GalaxyDomain(name = "Order", version = 1, codec = "binary")
    public static class Order {
        @GalaxyKey String orderId;
        @Indexed String symbol;
        @Indexed long quantity;
        @Indexed LocalDate tradeDate;
        List<String> notes = new ArrayList<>();

        public Order() {
        }

        Order(int i) {
            orderId = String.format("ORD-%06d", i);
            symbol = new String[]{"AAPL", "MSFT", "NVDA", "TSLA"}[i % 4];
            quantity = i;
            tradeDate = LocalDate.of(2026, 11, 1 + i % 7);
            notes.add("n" + i);
        }
    }

    @TempDir
    Path tmp;
    final List<GridNode> nodes = new ArrayList<>();
    GalaxyConfig config;
    GridTopology topology;

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @BeforeEach
    void start() throws IOException {
        int[] ports = {freePort(), freePort(), freePort()};
        String yaml = """
                cluster: grid-test
                catalog: { path: catalog }
                storages:
                  grid-1:
                    type: grid
                    path: grid
                    fsync: false
                    nodes: { node-1: "localhost:%d", node-2: "localhost:%d", node-3: "localhost:%d" }
                indexNodes:
                  grid-1: { url: "http://localhost:1" }
                namespaces:
                  orders: { storage: grid-1, indexNode: grid-1 }
                client: { readParallelism: 4, readBatchSize: 100 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(ports[0], ports[1], ports[2]);
        Path file = tmp.resolve("galaxy.yaml");
        Files.writeString(file, yaml);
        config = GalaxyConfig.load(file);
        var settings = config.storage("grid-1").settings();
        topology = GridTopology.fromSettings(settings);
        for (GridTopology.Member m : topology.members()) {
            nodes.add(new GridNode(m.name(), topology, tmp.resolve("grid").resolve(m.name()), settings, m.port(), -1).start());
        }
    }

    @AfterEach
    void stop() {
        nodes.forEach(GridNode::close);
    }

    @Test
    void partitionsSpreadEvenlyAndAreStable() {
        Map<String, Integer> counts = topology.partitionCounts();
        assertEquals(1024, counts.values().stream().mapToInt(Integer::intValue).sum());
        counts.values().forEach(c -> assertTrue(c > 250 && c < 450, "uneven partition split " + counts));
        GridTopology again = GridTopology.fromSettings(config.storage("grid-1").settings());
        for (int i = 0; i < 1000; i++) {
            assertEquals(topology.ownerOf("K" + i), again.ownerOf("K" + i));
        }
    }

    @Test
    void cacheApiWorksUnchangedOnTheGrid() {
        DatasetId ds = DatasetId.of("orders", "eq", "us", "2026-11-08");
        List<Order> all = new ArrayList<>();
        for (int i = 0; i < 3_000; i++) {
            all.add(new Order(i));
        }
        try (GalaxyClient client = new GalaxyClient(config)) {
            GalaxyCache<Order> orders = client.cache(ds, Order.class);
            orders.putAll(all);
            assertEquals(3_000, orders.stats().liveRecords());

            // every node holds a share of records and index documents, and only keys it owns
            GridDataStore store = (GridDataStore) GalaxyClientAccess.store(client, "grid-1");
            store.statsPerNode(ds).forEach((node, st) -> assertTrue(st.liveRecords() > 700, node + " has " + st));
            nodes.forEach(n -> assertTrue(n.index().datasetStats().get(0).documents() > 700));

            // key-routed reads
            Order o = orders.get("ORD-000042").orElseThrow();
            assertEquals(42, o.quantity);
            assertEquals(List.of("n42"), o.notes);
            Map<String, Order> some = orders.getAll(List.of("ORD-000001", "ORD-002999", "NOPE", "ORD-001500"));
            assertEquals(List.of("ORD-000001", "ORD-002999", "ORD-001500"), List.copyOf(some.keySet()));

            // fan-out queries give exactly what one index would give, including paging
            checkQuery(orders, all, eq("symbol", "NVDA"), x -> x.symbol.equals("NVDA"));
            checkQuery(orders, all, and(eq("symbol", "AAPL"), gte("quantity", 1000), lt("quantity", 1200)),
                    x -> x.symbol.equals("AAPL") && x.quantity >= 1000 && x.quantity < 1200);
            checkQuery(orders, all, or(eq("tradeDate", LocalDate.of(2026, 11, 3)), prefix("symbol", "TS")),
                    x -> x.tradeDate.equals(LocalDate.of(2026, 11, 3)) || x.symbol.startsWith("TS"));
            List<String> expectedAll = all.stream().map(x -> x.orderId).sorted().toList();
            assertEquals(expectedAll.subList(1234, 1284), orders.queryIds(Query.where(all()).offset(1234).limit(50)));
            assertEquals(3_000, orders.count(all()));

            // delete and update
            assertTrue(orders.deleteByKey(o));
            assertTrue(orders.get("ORD-000042").isEmpty());
            assertEquals(2_999, orders.count(all()));
            Order moved = orders.get("ORD-000007").orElseThrow();
            moved.symbol = "IBM";
            orders.put(moved);
            assertEquals(List.of("ORD-000007"), orders.queryIds(Query.where(eq("symbol", "IBM"))));

            // index lost on all nodes (no persistence in phase 1) -> reindex from the nodes' records
            store.indexClient().drop(ds);
            assertEquals(0, orders.count(all()));
            assertEquals(2_999, orders.reindex());
            assertEquals(2_999, orders.count(all()));
            assertEquals(749, orders.count(eq("symbol", "NVDA"))); // 750 NVDA orders, ORD-000042 deleted

            client.dropDataset(ds);
            assertTrue(client.listDatasets("orders").isEmpty());
        }
    }

    @Test
    void nodeRefusesKeysItDoesNotOwn() {
        String key = "ORD-000001";
        GridTopology.Member owner = topology.ownerOf(key);
        GridTopology.Member other = topology.members().stream().filter(m -> !m.equals(owner)).findFirst().orElseThrow();
        try (GridClient raw = new GridClient(topology, 1, 5_000)) {
            byte[] body = GridRecords.records("orders/eq/us/2026-11-08", List.of(new StoredRecord(key, 1, new byte[]{1})));
            GalaxyException e = assertThrows(GalaxyException.class, () -> raw.call(other, GridProtocol.PUT_RECORDS, body));
            assertTrue(e.getMessage().contains("belongs to " + owner.name()), e.getMessage());
            raw.call(owner, GridProtocol.ENSURE_DATASET, io.galaxycache.common.Json.writeBytes(new io.galaxycache.storage.DatasetMetadata(
                    DatasetId.parse("orders/eq/us/2026-11-08"), "Order", Order.class.getName(), 1, "binary", java.time.Instant.now())));
            assertEquals(1, new GridProtocol.In(raw.call(owner, GridProtocol.PUT_RECORDS, body).get(0)).getInt());
        }
    }

    @Test
    void mergeKeepsGlobalOrderAndPaging() {
        List<List<String>> lists = List.of(List.of("a", "d", "g"), List.of("b", "e"), List.of("c", "f", "h"));
        assertEquals(List.of("a", "b", "c", "d", "e", "f", "g", "h"), GridIndexClient.mergeSorted(lists, 0, Query.NO_LIMIT));
        assertEquals(List.of("c", "d", "e"), GridIndexClient.mergeSorted(lists, 2, 3));
        assertEquals(List.of(), GridIndexClient.mergeSorted(lists, 8, 3));
    }

    private static void checkQuery(GalaxyCache<Order> cache, List<Order> all, Filter f, Predicate<Order> p) {
        List<String> expected = all.stream().filter(p).map(x -> x.orderId).sorted(Comparator.naturalOrder()).toList();
        assertEquals(expected, cache.queryIds(Query.where(f)), f.toString());
        assertEquals(expected.size(), cache.count(f));
        assertEquals(expected.stream().skip(5).limit(7).toList(), cache.queryIds(Query.where(f).offset(5).limit(7)));
        List<Order> objs = cache.query(Query.where(f).limit(20));
        assertEquals(expected.stream().limit(20).toList(), objs.stream().map(x -> x.orderId).toList());
    }
}
