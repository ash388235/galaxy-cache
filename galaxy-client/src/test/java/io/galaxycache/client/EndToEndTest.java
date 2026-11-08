package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.Indexed;
import io.galaxycache.common.log.QueryLogEntry;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.index.server.IndexServer;
import io.galaxycache.index.server.IndexServerMain;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static io.galaxycache.common.query.Filters.*;
import static org.junit.jupiter.api.Assertions.*;

class EndToEndTest {

    @GalaxyDomain(name = "Trade", version = 1)
    public static class Trade {
        @Indexed String symbol;
        @Indexed long quantity;
        @Indexed String desk;
        @Indexed LocalDate tradeDate;
        double price;

        public Trade() {
        }

        Trade(String symbol, long quantity, String desk, double price) {
            this.symbol = symbol;
            this.quantity = quantity;
            this.desk = desk;
            this.price = price;
            this.tradeDate = LocalDate.of(2026, 11, 7);
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Trade t && t.symbol.equals(symbol) && t.quantity == quantity && t.desk.equals(desk)
                    && t.price == price && Objects.equals(t.tradeDate, tradeDate);
        }

        @Override
        public int hashCode() {
            return Objects.hash(symbol, quantity);
        }
    }

    /** Binary-coded domain whose key is the storage and index id. */
    @GalaxyDomain(name = "Order", version = 1, codec = "binary")
    public static class Order {
        @io.galaxycache.common.domain.GalaxyKey String orderId;
        @Indexed String symbol;
        @Indexed long quantity;
        List<String> tags = new ArrayList<>();
        Map<String, Double> fees = new java.util.LinkedHashMap<>();

        public Order() {
        }

        Order(String orderId, String symbol, long quantity) {
            this.orderId = orderId;
            this.symbol = symbol;
            this.quantity = quantity;
            tags.add("t-" + quantity);
            fees.put("commission", quantity * 0.01);
        }
    }

    @Test
    void binaryDomainUsesItsKeyAsStorageAndIndexId() {
        DatasetId ds = DatasetId.of("trading", "orders", "us", "2026-11-07");
        try (GalaxyClient client = new GalaxyClient(config)) {
            GalaxyCache<Order> orders = client.cache(ds, Order.class);
            assertEquals("binary", orders.metadata().codec());
            List<Order> batch = new ArrayList<>();
            for (int i = 0; i < 300; i++) {
                batch.add(new Order("ORD-" + i, i % 3 == 0 ? "AAPL" : "MSFT", i));
            }
            orders.putAll(batch);
            orders.put(new Order("ORD-999", "TSLA", 7));

            Order got = orders.get("ORD-42").orElseThrow();
            assertEquals("ORD-42", got.orderId);
            assertEquals(List.of("t-42"), got.tags);
            assertEquals(0.42, got.fees.get("commission"), 1e-9);

            // the index pod returns the keys themselves as ids
            assertEquals(List.of("ORD-999"), orders.queryIds(Query.where(eq("symbol", "TSLA"))));
            assertEquals(100, orders.query(Query.where(eq("symbol", "AAPL"))).size());

            GalaxyException mismatch = assertThrows(GalaxyException.class,
                    () -> orders.put("WRONG-ID", new Order("ORD-1", "AAPL", 1)));
            assertTrue(mismatch.getMessage().contains("@GalaxyKey"));

            assertTrue(orders.deleteByKey(got));
            assertTrue(orders.get("ORD-42").isEmpty());
            assertEquals(0, orders.count(eq("quantity", 42)));
        }
    }

    // ---- a domain as it was written, the same class renamed to read old data, and the changed class

    @GalaxyDomain(name = "Tx", version = 1, codec = "binary")
    public static class TxOriginal {
        @io.galaxycache.common.domain.GalaxyKey String id;
        @Indexed String sym;
        Detail detail = new Detail();

        public static class Detail {
            int lots;
        }
    }

    @GalaxyDomain(name = "Tx", version = 1, codec = "binary",
            originalClass = "io.galaxycache.client.EndToEndTest$TxOriginal")
    public static class TxV1 {
        @io.galaxycache.common.domain.GalaxyKey String id;
        @Indexed String sym;
        Detail detail = new Detail();

        public static class Detail {
            int lots;
        }
    }

    /** Same shape and name as TxV1 but no originalClass: must be refused rather than misread. */
    @GalaxyDomain(name = "Tx", version = 1, codec = "binary")
    public static class TxRenamedWithoutAlias {
        @io.galaxycache.common.domain.GalaxyKey String id;
        @Indexed String sym;
        Detail detail = new Detail();

        public static class Detail {
            int lots;
        }
    }

    @GalaxyDomain(name = "Tx", version = 2, codec = "binary")
    public static class TxV2 {
        @io.galaxycache.common.domain.GalaxyKey String id;
        @Indexed String symbol; // renamed
        @Indexed String currency; // new
        long lots; // moved up from the nested class
    }

    @Test
    void changingADomainClassMigratesToANewDataset() {
        DatasetId oldDs = DatasetId.of("trading", "tx", "us", "2026-11-07");
        DatasetId newDs = DatasetId.of("trading", "tx", "us", "2026-11-08");
        try (GalaxyClient writer = new GalaxyClient(config)) {
            GalaxyCache<TxOriginal> c = writer.cache(oldDs, TxOriginal.class);
            List<TxOriginal> list = new ArrayList<>();
            for (int i = 0; i < 2_500; i++) {
                TxOriginal t = new TxOriginal();
                t.id = String.format("X%05d", i);
                t.sym = i % 2 == 0 ? "AAPL" : "MSFT";
                t.detail.lots = i;
                list.add(t);
            }
            c.putAll(list);
        }
        try (GalaxyClient renamed = new GalaxyClient(config)) {
            GalaxyException e = assertThrows(GalaxyException.class,
                    () -> renamed.cache(oldDs, TxRenamedWithoutAlias.class).get("X00001"));
            assertTrue(e.getMessage().contains("different class shape"), e.getMessage());
        }
        try (GalaxyClient client = new GalaxyClient(config)) {
            assertEquals(7, client.cache(oldDs, TxV1.class).get("X00007").orElseThrow().detail.lots);

            java.util.function.Function<TxV1, TxV2> convert = o -> {
                if (o.detail.lots == 13) {
                    return null; // conversions may drop objects
                }
                TxV2 n = new TxV2();
                n.id = o.id;
                n.symbol = o.sym;
                n.currency = "USD";
                n.lots = o.detail.lots;
                return n;
            };
            DatasetMigration.Result r = DatasetMigration.run(client, oldDs, TxV1.class, newDs, TxV2.class, convert);
            assertEquals(2_500, r.source());
            assertEquals(2_499, r.copied());
            assertEquals(1, r.skipped());

            GalaxyCache<TxV2> v2 = client.cache(newDs, TxV2.class);
            assertEquals(2_499, v2.count(all()));
            assertEquals(2_499, v2.count(eq("currency", "USD")));
            assertEquals(1_250, v2.count(eq("symbol", "AAPL")));
            assertEquals(1234, v2.get("X01234").orElseThrow().lots);
            assertTrue(v2.get("X00013").isEmpty());
            assertEquals(2, v2.metadata().domainVersion());

            // the old data is untouched, and running it again is harmless
            assertEquals(2_500, client.cache(oldDs, TxV1.class).count(all()));
            assertEquals(2_499, DatasetMigration.run(client, oldDs, TxV1.class, newDs, TxV2.class, convert).copied());
            assertEquals(2_499, v2.count(all()));

            assertThrows(GalaxyException.class, () -> DatasetMigration.run(client, oldDs, TxV1.class, oldDs, TxV1.class, o -> o));
        }
    }

    @TempDir
    Path tmp;
    GalaxyConfig config;
    IndexServer idxA;
    IndexServer idxB;

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @BeforeEach
    void start() throws IOException {
        io.galaxycache.index.server.persistence.InMemoryIndexPersistence.reset("idx-b");
        int pa = freePort();
        int pb = freePort();
        String yaml = """
                cluster: test
                catalog: { path: catalog }
                storages:
                  fs1: { type: file, path: fs1, pageSize: 8192, fsync: false }
                  fs2: { type: file, path: fs2 }
                indexNodes:
                  idx-a: { url: "http://localhost:%d", persistence: noop }
                  idx-b: { url: "http://localhost:%d", persistence: memory }
                namespaces:
                  trading: { storage: fs1, indexNode: idx-a }
                  risk: { storage: fs2, indexNode: idx-b }
                client: { readParallelism: 4, readBatchSize: 100 }
                logging: { queryLogDir: logs }
                """.formatted(pa, pb);
        Path file = tmp.resolve("galaxy.yaml");
        Files.writeString(file, yaml);
        config = GalaxyConfig.load(file);
        idxA = IndexServerMain.start(config, "idx-a", -1);
        idxB = IndexServerMain.start(config, "idx-b", -1);
    }

    @AfterEach
    void stop() {
        idxA.close();
        idxB.close();
    }

    @Test
    void crudAndQueriesAcrossTwoNamespaces() throws IOException {
        DatasetId ds = DatasetId.of("trading", "equities", "us", LocalDate.of(2026, 11, 7));
        try (GalaxyClient client = new GalaxyClient(config)) {
            GalaxyCache<Trade> trades = client.cache(ds, Trade.class);
            String[] syms = {"AAPL", "MSFT", "AMZN", "GOOG"};
            Map<String, Trade> batch = new HashMap<>();
            for (int i = 0; i < 2000; i++) {
                batch.put(String.format("T%05d", i), new Trade(syms[i % 4], i, i % 2 == 0 ? "D1" : "D2", 100 + i));
            }
            trades.putAll(batch);

            assertEquals(batch.get("T00042"), trades.get("T00042").orElseThrow());
            List<Trade> aapl = trades.query(Query.where(and(eq("symbol", "AAPL"), gte("quantity", 1000))));
            assertEquals(250, aapl.size());
            assertTrue(aapl.stream().allMatch(t -> t.symbol.equals("AAPL") && t.quantity >= 1000));
            assertEquals(500, trades.count(eq("symbol", "MSFT")));
            assertEquals(2000, trades.count(eq("tradeDate", LocalDate.of(2026, 11, 7))));
            assertEquals(List.of("T00010", "T00011"), trades.queryIds(Query.where(between("quantity", 10, 11))));

            trades.put("T00000", new Trade("TSLA", 1, "D9", 1));
            assertEquals(List.of("T00000"), trades.queryIds(Query.where(eq("symbol", "TSLA"))));
            assertTrue(trades.delete("T00000"));
            assertTrue(trades.query(Query.where(eq("symbol", "TSLA"))).isEmpty());
            assertTrue(trades.get("T00000").isEmpty());
            assertEquals(1999, trades.stats().liveRecords());

            // the second namespace lives on another file system and another index pod
            DatasetId riskDs = DatasetId.of("risk", "var", "eu", "2026-11-07");
            GalaxyCache<Trade> risk = client.cache(riskDs, Trade.class);
            risk.put("R1", new Trade("SAP", 5, "R", 1));
            assertEquals(1, risk.count(all()));
            assertEquals(1, idxB.engine().datasetStats().size());
            assertEquals(1, idxA.engine().datasetStats().size());
            assertTrue(Files.exists(tmp.resolve("fs2/risk/var/eu/2026-11-07/_dataset.json")));

            assertEquals(List.of(riskDs, ds), client.listDatasets(null));
        }

        List<QueryLogEntry> entries = new ArrayList<>();
        try (Stream<Path> files = Files.list(tmp.resolve("logs"))) {
            for (Path f : files.toList()) {
                Files.readAllLines(f).stream().map(QueryLogEntry::parse).filter(Objects::nonNull).forEach(entries::add);
            }
        }
        QueryLogEntry q = entries.stream().filter(e -> e.op().equals("QUERY")).findFirst().orElseThrow();
        assertEquals(250, q.idsReturned());
        assertEquals("AND(symbol EQ AAPL, quantity GTE 1000)", q.filter());
        assertNotNull(q.indexMs());
    }

    @Test
    void indexPodsRejectNamespacesTheyDoNotOwn() {
        DatasetId ds = DatasetId.of("risk", "var", "eu", "2026-11-07");
        try (GalaxyClient client = new GalaxyClient(config)) {
            // talk to the wrong pod on purpose
            GalaxyException e = assertThrows(GalaxyException.class,
                    () -> client.index("idx-a").query(ds, Query.all()));
            assertTrue(e.getMessage().contains("not served by index node 'idx-a'"), e.getMessage());
        }
    }

    @Test
    void runtimeNamespacesAreSharedThroughTheCatalogAndReindexRecoversAnEmptyPod() throws Exception {
        DatasetId ds = DatasetId.of("pricing", "fx", "spot", "2026-11-07");
        try (GalaxyClient writer = new GalaxyClient(config); GalaxyClient reader = new GalaxyClient(config)) {
            writer.createNamespace("pricing", "fs1", "idx-a");
            assertTrue(reader.listNamespaces().containsKey("pricing"));
            writer.cache(ds, Trade.class).put("P1", new Trade("EURUSD", 1, "FX", 1.08));
            assertEquals("EURUSD", reader.cache(ds, Trade.class).query(Query.all()).get(0).symbol);

            // idx-a uses noop persistence: a restart loses its memory, reindex() rebuilds it from storage
            int port = idxA.port();
            idxA.close();
            idxA = IndexServerMain.start(config, "idx-a", port);
            assertEquals(0, reader.cache(ds, Trade.class).count(all()));
            assertEquals(1, reader.cache(ds, Trade.class).reindex());
            assertEquals(1, reader.cache(ds, Trade.class).count(eq("symbol", "EURUSD")));

            assertThrows(GalaxyException.class, () -> writer.createNamespace("pricing", "fs2", "idx-b"));
        }
    }

    @Test
    void memoryPersistenceRestoresAPodAfterRestart() {
        DatasetId ds = DatasetId.of("risk", "var", "eu", "2026-11-08");
        try (GalaxyClient client = new GalaxyClient(config)) {
            client.cache(ds, Trade.class).put("R9", new Trade("SAP", 5, "R", 1));
            int port = idxB.port();
            idxB.close(); // flushes async persistence
            idxB = IndexServerMain.start(config, "idx-b", port);
            assertEquals(1, client.cache(ds, Trade.class).count(eq("symbol", "SAP")));
        }
    }
}
