package io.galaxycache.client;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.GalaxyKey;
import io.galaxycache.common.domain.Indexed;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;

import static io.galaxycache.common.query.Filters.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The cache API contract every provider must meet. A provider's test module extends this class and says how to
 * open a client; the same assertions then run against galaxy (file store, grid) and Ignite. Each row of the
 * support matrix in docs/PROVIDERS.md is covered here: either the operation works, or it throws
 * {@link NotSupportedException} and {@code supports()} says false.
 */
public abstract class ProviderContractTest {

    /** A client on a running cluster with namespace {@link #namespace()} available. */
    protected abstract GalaxyClient openClient();

    protected abstract String namespace();

    protected abstract String expectedProvider();

    protected GalaxyClient client;
    protected DatasetId ds;

    @BeforeEach
    void openContractClient(TestInfo info) {
        client = openClient();
        String name = info.getTestMethod().map(m -> m.getName()).orElse("t");
        ds = DatasetId.of(namespace(), "contract", name.length() > 60 ? name.substring(0, 60) : name, "2026-11-13");
    }

    @AfterEach
    void closeContractClient() {
        if (client != null) {
            client.close();
        }
    }

    // ------------------------------------------------------------------ domains

    public enum Side { BUY, SELL }

    @GalaxyDomain(name = "ContractTrade", version = 1)
    public static class Trade {
        @Indexed public String symbol;
        @Indexed public long qty;
        @Indexed public double price;
        @Indexed public boolean active;
        @Indexed public Side side;
        @Indexed public LocalDate tradeDate;
        @Indexed public String note;
        public String comment;

        public Trade() {
        }

        public Trade(int i) {
            String[] syms = {"AAPL", "MSFT", "AMZN", "GOOG"};
            symbol = syms[i % 4];
            qty = i * 10L;
            price = i + 0.5;
            active = i % 3 == 0;
            side = i % 2 == 0 ? Side.BUY : Side.SELL;
            tradeDate = LocalDate.of(2026, 11, 1).plusDays(i % 5);
            note = i % 4 == 0 ? null : "n" + i;
            comment = "trade " + i;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Trade t && Objects.equals(t.symbol, symbol) && t.qty == qty && t.price == price
                    && t.active == active && t.side == side && Objects.equals(t.tradeDate, tradeDate)
                    && Objects.equals(t.note, note) && Objects.equals(t.comment, comment);
        }

        @Override
        public int hashCode() {
            return Objects.hash(symbol, qty);
        }

        @Override
        public String toString() {
            return symbol + "/" + qty;
        }
    }

    @GalaxyDomain(name = "ContractOrder", version = 1, codec = "binary")
    public static class Order {
        @GalaxyKey public String orderId;
        @Indexed public String symbol;
        @Indexed public long quantity;
        public List<String> tags = new ArrayList<>();

        public Order() {
        }

        public Order(String orderId, String symbol, long quantity) {
            this.orderId = orderId;
            this.symbol = symbol;
            this.quantity = quantity;
            tags.add("t-" + quantity);
        }
    }

    /** Same domain name as {@link Trade} but version 2: must be refused on a v1 dataset. */
    @GalaxyDomain(name = "ContractTrade", version = 2)
    public static class TradeV2 {
        @Indexed public String symbol;

        public TradeV2() {
        }
    }

    private static String id(int i) {
        return String.format("t%02d", i);
    }

    private Map<String, Trade> load(GalaxyCache<Trade> c, int n) {
        Map<String, Trade> all = new LinkedHashMap<>();
        for (int i = 0; i < n; i++) {
            all.put(id(i), new Trade(i));
        }
        c.putAll(all);
        return all;
    }

    // ------------------------------------------------------------------ core

    @Test
    void reportsItsProvider() {
        assertEquals(expectedProvider(), client.provider());
        assertEquals(expectedProvider(), client.cache(ds, Trade.class).provider());
    }

    @Test
    void putGetDelete() {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        c.put("a", new Trade(1));
        assertEquals(new Trade(1), c.get("a").orElseThrow());
        assertTrue(c.get("missing").isEmpty());

        c.put("a", new Trade(2)); // overwrite
        assertEquals(new Trade(2), c.get("a").orElseThrow());

        Map<String, Trade> all = load(c, 10);
        Map<String, Trade> got = c.getAll(List.of("t05", "nope", "t01", "t09"));
        assertEquals(List.of("t05", "t01", "t09"), List.copyOf(got.keySet()), "order of the request, missing skipped");
        assertEquals(all.get("t05"), got.get("t05"));

        assertTrue(c.delete("a"));
        assertFalse(c.delete("a"));
        assertEquals(2, c.deleteAll(List.of("t01", "t02", "nope")));
        assertTrue(c.get("t01").isEmpty());
        assertEquals(0, c.count(eq("qty", 10L)));
        assertEquals(8, c.stats().liveRecords());
    }

    @Test
    void keyedDomainWithBinaryCodec() {
        GalaxyCache<Order> c = client.cache(ds, Order.class);
        assertEquals("binary", c.metadata().codec());
        for (int i = 0; i < 30; i++) {
            c.put(new Order("ORD-" + i, i % 3 == 0 ? "AAPL" : "MSFT", i));
        }
        Order got = c.get("ORD-12").orElseThrow();
        assertEquals("ORD-12", got.orderId);
        assertEquals(List.of("t-12"), got.tags);
        assertEquals(List.of("ORD-0", "ORD-12"), c.queryIds(Query.where(and(eq("symbol", "AAPL"), lte("quantity", 12), in("quantity", 0, 12)))));
        GalaxyException e = assertThrows(GalaxyException.class, () -> c.put("WRONG", new Order("ORD-1", "X", 1)));
        assertTrue(e.getMessage().contains("@GalaxyKey"));
        assertTrue(c.deleteByKey(got));
        assertTrue(c.get("ORD-12").isEmpty());
    }

    @Test
    void everyFilterOperatorMatchesItsDefinition() {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        Map<String, Trade> all = load(c, 20);
        check(c, all, eq("symbol", "AAPL"), t -> t.symbol.equals("AAPL"));
        check(c, all, ne("symbol", "AAPL"), t -> !t.symbol.equals("AAPL"));
        check(c, all, in("symbol", "MSFT", "GOOG", "NONE"), t -> Set.of("MSFT", "GOOG").contains(t.symbol));
        check(c, all, gt("qty", 100), t -> t.qty > 100);
        check(c, all, gte("qty", 100), t -> t.qty >= 100);
        check(c, all, lt("qty", 50), t -> t.qty < 50);
        check(c, all, lte("qty", 50), t -> t.qty <= 50);
        check(c, all, between("qty", 30, 70), t -> t.qty >= 30 && t.qty <= 70);
        check(c, all, between("qty", 70, 30), t -> false);
        check(c, all, gt("price", 9.5), t -> t.price > 9.5);
        check(c, all, gt("price", 9), t -> t.price > 9);
        check(c, all, eq("qty", 40.0), t -> false); // 40L and 40.0 are different index values
        check(c, all, eq("active", true), t -> t.active);
        check(c, all, eq("side", "SELL"), t -> t.side == Side.SELL);
        check(c, all, between("tradeDate", "2026-11-02", "2026-11-03"),
                t -> t.tradeDate.toString().compareTo("2026-11-02") >= 0 && t.tradeDate.toString().compareTo("2026-11-03") <= 0);
        check(c, all, prefix("note", "n1"), t -> t.note != null && t.note.startsWith("n1"));
        check(c, all, exists("note"), t -> t.note != null);
        check(c, all, not(exists("note")), t -> t.note == null);
        check(c, all, ne("note", "n1"), t -> t.note != null && !t.note.equals("n1"));
        check(c, all, not(eq("note", "n1")), t -> !"n1".equals(t.note));
        check(c, all, and(eq("symbol", "MSFT"), gte("qty", 50)), t -> t.symbol.equals("MSFT") && t.qty >= 50);
        check(c, all, or(eq("symbol", "MSFT"), lt("qty", 20)), t -> t.symbol.equals("MSFT") || t.qty < 20);
        check(c, all, not(or(eq("symbol", "MSFT"), eq("active", true))), t -> !(t.symbol.equals("MSFT") || t.active));
        check(c, all, all(), t -> true);
        // values of another type than the field: booleans < numbers < strings, as the galaxy index orders them
        check(c, all, gt("qty", "a"), t -> false);
        check(c, all, lt("qty", "a"), t -> true);
        check(c, all, eq("symbol", 5), t -> false);
        check(c, all, gt("symbol", 5), t -> true);
        // a field that is not indexed matches nothing; NOT of it matches everything
        check(c, all, eq("comment", "trade 1"), t -> false);
        check(c, all, not(eq("comment", "trade 1")), t -> true);
    }

    private void check(GalaxyCache<Trade> c, Map<String, Trade> all, Filter f, Predicate<Trade> expected) {
        List<String> want = all.entrySet().stream().filter(e -> expected.test(e.getValue())).map(Map.Entry::getKey)
                .sorted().toList();
        assertEquals(want, c.queryIds(Query.where(f)), "ids for " + f);
        assertEquals(want.size(), c.count(f), "count for " + f);
        List<Trade> objects = c.query(Query.where(f));
        assertEquals(want.stream().map(all::get).toList(), objects, "objects for " + f);
    }

    @Test
    void pagingIsOrderedById() {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        load(c, 20);
        assertEquals(List.of("t05", "t06", "t07"), c.queryIds(Query.all().offset(5).limit(3)));
        assertEquals(List.of("t18", "t19"), c.queryIds(Query.all().offset(18).limit(10)));
        assertEquals(List.of(), c.queryIds(Query.all().offset(25)));
        assertEquals(List.of("t02", "t06"), c.queryIds(Query.where(eq("symbol", "AMZN")).limit(2)));
    }

    @Test
    void indexFollowsOverwritesAndDeletes() {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        load(c, 8);
        Trade t = new Trade(1);
        t.symbol = "TSLA";
        c.put("t00", t);
        assertEquals(List.of("t00"), c.queryIds(Query.where(eq("symbol", "TSLA"))));
        assertEquals(List.of("t04"), c.queryIds(Query.where(eq("symbol", "AAPL"))));
        c.delete("t00");
        assertEquals(0, c.count(eq("symbol", "TSLA")));
        assertEquals(7, c.reindex());
        assertEquals(7, c.count(all()));
    }

    @Test
    void datasetsCanBeListedAndDropped() {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        load(c, 3);
        assertTrue(client.listDatasets(namespace()).contains(ds));
        assertTrue(client.listDatasets(null).contains(ds));
        assertTrue(client.listNamespaces().containsKey(namespace()));
        client.dropDataset(ds);
        assertFalse(client.listDatasets(namespace()).contains(ds));
        GalaxyCache<Trade> again = client.cache(ds, Trade.class);
        assertTrue(again.get("t01").isEmpty());
        assertEquals(0, again.count(all()));
    }

    @Test
    void anotherDomainVersionIsRefused() {
        client.cache(ds, Trade.class).put("x", new Trade(1));
        try (GalaxyClient other = openClient()) {
            GalaxyException e = assertThrows(GalaxyException.class, () -> other.cache(ds, TradeV2.class));
            assertTrue(e.getMessage().contains("v1"), e.getMessage());
        }
    }

    @Test
    void secondClientSeesTheSameData() {
        client.cache(ds, Trade.class).put("x", new Trade(3));
        try (GalaxyClient other = openClient()) {
            assertEquals(new Trade(3), other.cache(ds, Trade.class).get("x").orElseThrow());
        }
    }

    // ------------------------------------------------------------------ optional data operations

    private void supportedOrRefused(Capability cap, Executable whenSupported, Executable call) throws Throwable {
        if (client.supports(cap)) {
            whenSupported.execute();
        } else {
            NotSupportedException e = assertThrows(NotSupportedException.class, call, cap + " must be refused");
            assertInstanceOf(UnsupportedOperationException.class, e);
            assertEquals(cap, e.capability());
            assertEquals(expectedProvider(), e.provider());
            assertTrue(e.getMessage().contains("not supported"), e.getMessage());
        }
    }

    @Test
    void atomicOperations() throws Throwable {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        supportedOrRefused(Capability.ATOMIC_OPS, () -> {
            assertTrue(c.putIfAbsent("a", new Trade(1)));
            assertFalse(c.putIfAbsent("a", new Trade(2)));
            assertEquals(new Trade(1), c.get("a").orElseThrow());
            assertFalse(c.replace("b", new Trade(2)));
            assertTrue(c.replace("a", new Trade(2)));
            assertEquals(List.of("a"), c.queryIds(Query.where(eq("symbol", "AMZN"))), "index follows replace");
            assertEquals(new Trade(2), c.getAndPut("a", new Trade(3)).orElseThrow());
            assertTrue(c.getAndPut("c", new Trade(4)).isEmpty());
        }, () -> c.putIfAbsent("a", new Trade(1)));
        if (!client.supports(Capability.ATOMIC_OPS)) {
            assertThrows(NotSupportedException.class, () -> c.replace("a", new Trade(1)));
            assertThrows(NotSupportedException.class, () -> c.getAndPut("a", new Trade(1)));
        }
    }

    @Test
    void expiry() throws Throwable {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        supportedOrRefused(Capability.EXPIRY, () -> {
            c.put("short", new Trade(1), Duration.ofMillis(300));
            c.put("long", new Trade(2), Duration.ofMinutes(10));
            long deadline = System.currentTimeMillis() + 10_000;
            while (c.get("short").isPresent() && System.currentTimeMillis() < deadline) {
                Thread.sleep(100);
            }
            assertTrue(c.get("short").isEmpty(), "expired entry is gone");
            assertTrue(c.get("long").isPresent());
        }, () -> c.put("short", new Trade(1), Duration.ofSeconds(1)));
    }

    @Test
    void transactions() throws Throwable {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        supportedOrRefused(Capability.TRANSACTIONS, () -> {
            c.inTransaction(tx -> {
                tx.put("a", new Trade(1));
                tx.put("b", new Trade(2));
                return null;
            });
            assertEquals(2, c.getAll(List.of("a", "b")).size());
            assertThrows(IllegalStateException.class, () -> c.inTransaction(tx -> {
                tx.put("c", new Trade(3));
                tx.delete("a");
                throw new IllegalStateException("abort");
            }));
            assertTrue(c.get("c").isEmpty(), "rolled back");
            assertTrue(c.get("a").isPresent(), "rolled back");
            Integer seen = c.inTransaction(tx -> tx.getAll(List.of("a", "b")).size());
            assertEquals(2, seen);
        }, () -> c.inTransaction(tx -> null));
    }

    @Test
    void changeEvents() throws Throwable {
        GalaxyCache<Trade> c = client.cache(ds, Trade.class);
        List<CacheEvent<Trade>> events = new CopyOnWriteArrayList<>();
        supportedOrRefused(Capability.CHANGE_EVENTS, () -> {
            try (AutoCloseable h = c.listen(events::add)) {
                c.put("a", new Trade(1));
                c.put("a", new Trade(2));
                c.delete("a");
                long deadline = System.currentTimeMillis() + 10_000;
                while (events.size() < 3 && System.currentTimeMillis() < deadline) {
                    Thread.sleep(50);
                }
            }
            assertEquals(List.of(CacheEvent.Type.CREATED, CacheEvent.Type.UPDATED, CacheEvent.Type.REMOVED),
                    events.stream().map(CacheEvent::type).toList());
            assertEquals(new Trade(1), events.get(0).value());
            assertEquals("a", events.get(2).id());
        }, () -> c.listen(events::add));
    }

    // ------------------------------------------------------------------ cluster operations

    @Test
    void clusterOperationsWorkOrAreRefused() throws Throwable {
        GalaxyCluster cl = client.cluster();
        assertEquals(cl.capabilities().stream().filter(x -> true).count(), cl.capabilities().size());
        for (Capability cap : cl.capabilities()) {
            assertTrue(client.supports(cap), "client capabilities include the default cluster's: " + cap);
        }
        supportedOrRefusedOn(cl, Capability.CLUSTER_VIEW, () -> {
            List<NodeInfo> nodes = cl.nodes();
            assertFalse(nodes.isEmpty());
            assertEquals(ClusterState.READY, cl.state());
            cl.awaitReady(Duration.ofSeconds(10));
            cl.onFailure(reason -> { });
        }, cl::nodes);
        supportedOrRefusedOn(cl, Capability.PARTITION_MAP, () -> {
            assertTrue(cl.partitions() > 0);
            String owner = cl.ownerOf("some-key");
            assertTrue(cl.nodes().stream().anyMatch(n -> n.name().equals(owner)), owner);
            assertEquals(owner, cl.ownerOf("some-key"), "stable");
        }, cl::partitions);
        supportedOrRefusedOn(cl, Capability.NODE_STATS, () -> {
            GalaxyCache<Trade> c = client.cache(ds, Trade.class);
            load(c, 12);
            List<NodeStats> stats = cl.nodeStats();
            assertEquals(cl.nodes().size(), stats.size());
            assertTrue(stats.stream().mapToLong(NodeStats::records).sum() >= 12);
        }, cl::nodeStats);
        supportedOrRefusedOn(cl, Capability.BASELINE, () -> {
            assertEquals(cl.nodes().stream().map(NodeInfo::name).sorted().toList(),
                    cl.baseline().stream().sorted().toList());
        }, cl::baseline);
        // operations that change the cluster are exercised by each provider's own tests; here only refusals
        if (!cl.supports(Capability.REBALANCE)) {
            assertThrows(NotSupportedException.class, cl::rebalance);
        }
        if (!cl.supports(Capability.COMPACT)) {
            assertThrows(NotSupportedException.class, cl::compact);
        }
        if (!cl.supports(Capability.ACTIVATION)) {
            assertThrows(NotSupportedException.class, cl::activate);
            assertThrows(NotSupportedException.class, cl::deactivate);
        }
        if (!cl.supports(Capability.BASELINE)) {
            assertThrows(NotSupportedException.class, () -> cl.setBaseline(List.of("x")));
        }
    }

    private void supportedOrRefusedOn(GalaxyCluster cl, Capability cap, Executable whenSupported, Executable call)
            throws Throwable {
        if (cl.supports(cap)) {
            whenSupported.execute();
        } else {
            NotSupportedException e = assertThrows(NotSupportedException.class, call, cap + " must be refused");
            assertEquals(cap, e.capability());
        }
    }

    /** Sorted copy, for comparisons that ignore order. */
    protected static List<String> sorted(List<String> in) {
        List<String> out = new ArrayList<>(in);
        out.sort(Comparator.naturalOrder());
        return out;
    }
}
