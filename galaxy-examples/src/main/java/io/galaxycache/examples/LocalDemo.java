package io.galaxycache.examples;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.dashboard.DashboardServer;
import io.galaxycache.index.server.IndexServer;
import io.galaxycache.index.server.IndexServerMain;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.galaxycache.common.query.Filters.*;

/**
 * End-to-end walkthrough on the local file system.
 *
 * <pre>
 * java -jar galaxy-examples.jar --config config/galaxy.yaml [--embedded] [--records 200000] [--keep-running]
 * </pre>
 * {@code --embedded} starts both index pods and the dashboard inside this JVM; without it they must
 * already be running (see scripts/start-index.sh and scripts/start-dashboard.sh).
 */
public final class LocalDemo {

    private static final String[] SYMBOLS = {"AAPL", "MSFT", "AMZN", "GOOG", "NVDA", "META", "TSLA", "JPM", "V", "XOM"};
    private static final String[] DESKS = {"EQ-FLOW", "EQ-DELTA1", "EQ-PROG", "EQ-ARB"};
    private static final String[] CPTYS = {"ACME-BANK", "BLUE-FUND", "NORTH-AM", "ORION-CAP", "ZEN-PARTNERS"};

    private LocalDemo() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy.yaml";
        boolean embedded = false;
        boolean keepRunning = false;
        int records = 100_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--embedded" -> embedded = true;
                case "--keep-running" -> keepRunning = true;
                case "--records" -> records = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        GalaxyConfig config = GalaxyConfig.load(Path.of(configPath));
        List<AutoCloseable> embeddedServices = new ArrayList<>();
        if (embedded) {
            for (String node : config.indexNodes().keySet()) {
                IndexServer s = IndexServerMain.start(config, node, -1);
                embeddedServices.add(s);
                say("started index pod %s on port %d", node, s.port());
            }
            DashboardServer d = new DashboardServer(config, 8080).start();
            embeddedServices.add(d);
            say("dashboard on http://localhost:%d", d.port());
        }

        try (GalaxyClient client = new GalaxyClient(config)) {
            say("namespaces from config: %s", client.listNamespaces().keySet());
            // a namespace created at runtime lands in the shared catalog folder and is visible to every client
            client.createNamespace("reference", "local-fs-1", "idx-a");
            say("namespaces now: %s", client.listNamespaces());

            DatasetId tradesId = DatasetId.of("trading", "equities", "us", LocalDate.now());
            GalaxyCache<Trade> trades = client.cache(tradesId, Trade.class);
            loadTrades(trades, records);

            section("Filtered queries (index pod returns ids, client reads storage in parallel)");
            runQuery(trades, eq("symbol", "AAPL"));
            runQuery(trades, and(eq("symbol", "NVDA"), gt("quantity", 9_000)));
            runQuery(trades, and(in("desk", "EQ-FLOW", "EQ-ARB"), eq("side", "BUY"), between("quantity", 100, 200)));
            runQuery(trades, or(eq("counterparty", "ZEN-PARTNERS"), prefix("symbol", "TS")));
            runQuery(trades, not(eq("tradeDate", LocalDate.now())));

            section("Paging, counts and point reads");
            List<String> page = trades.queryIds(Query.where(eq("symbol", "MSFT")).offset(0).limit(5));
            say("first 5 MSFT ids: %s", page);
            say("count(symbol = MSFT) = %,d", trades.count(eq("symbol", "MSFT")));
            say("get(%s) = %s", page.get(0), trades.get(page.get(0)).orElseThrow());
            Map<String, Trade> some = trades.getAll(page);
            say("getAll(5 ids) returned %d trades", some.size());

            section("Update and delete");
            Trade t = trades.get(page.get(0)).orElseThrow();
            t.symbol = "IBM";
            trades.put(page.get(0), t);
            say("after update, count(symbol = IBM) = %d", trades.count(eq("symbol", "IBM")));
            trades.delete(page.get(0));
            say("after delete, count(symbol = IBM) = %d, get = %s", trades.count(eq("symbol", "IBM")),
                    trades.get(page.get(0)));

            section("Second namespace on another file system and index pod");
            DatasetId riskId = DatasetId.of("risk", "var", "eod", LocalDate.now());
            GalaxyCache<RiskMeasure> risk = client.cache(riskId, RiskMeasure.class);
            Map<String, RiskMeasure> rm = new LinkedHashMap<>();
            Random r = new Random(7);
            for (int i = 0; i < 5_000; i++) {
                rm.put("RM-" + i, new RiskMeasure("BOOK-" + (i % 50), i % 2 == 0 ? "VaR99" : "ES97.5",
                        Math.round(r.nextGaussian() * 1_000_000) / 100.0, "USD"));
            }
            risk.putAll(rm);
            runQuery(risk, and(eq("measure", "VaR99"), gt("value", 15_000.0)));

            DatasetId refId = DatasetId.of("reference", "instruments", "us", LocalDate.now());
            GalaxyCache<Trade> ref = client.cache(refId, Trade.class);
            ref.put("REF-1", new Trade("AAPL", "REF", Trade.Side.BUY, 1, LocalDate.now(), "NONE", 0, "", "reference row"));

            section("Datasets");
            for (DatasetId ds : client.listDatasets(null)) {
                String ns = ds.namespace();
                var nc = client.listNamespaces().get(ns);
                var stats = ns.equals("trading") ? trades.stats() : ns.equals("risk") ? risk.stats() : ref.stats();
                say("%-40s storage=%-11s pod=%-6s records=%,9d  size=%,12d bytes  segments=%d", ds, nc.storage(),
                        nc.indexNode(), stats.liveRecords(), stats.bytesOnDisk(), stats.files());
            }
            say("query log lines: %s", config.logging().queryLogDir());
        }

        if (embedded && keepRunning) {
            say("services keep running; open http://localhost:8080 and press Ctrl+C to stop");
            Thread.currentThread().join();
        }
        for (AutoCloseable c : embeddedServices) {
            c.close();
        }
    }

    private static void loadTrades(GalaxyCache<Trade> trades, int records) {
        section("Bulk load of " + String.format("%,d", records) + " trades into " + trades.dataset());
        Random r = new Random(42);
        LocalDate today = LocalDate.now();
        long start = System.nanoTime();
        int batchSize = 10_000;
        for (int from = 0; from < records; from += batchSize) {
            Map<String, Trade> batch = new LinkedHashMap<>();
            for (int i = from; i < Math.min(records, from + batchSize); i++) {
                batch.put(String.format("T%08d", i), new Trade(
                        SYMBOLS[r.nextInt(SYMBOLS.length)], DESKS[r.nextInt(DESKS.length)],
                        r.nextBoolean() ? Trade.Side.BUY : Trade.Side.SELL, 1 + r.nextInt(10_000),
                        r.nextInt(10) == 0 ? today.minusDays(1) : today, CPTYS[r.nextInt(CPTYS.length)],
                        Math.round((50 + r.nextDouble() * 450) * 100) / 100.0, "trader" + r.nextInt(40),
                        "booked by demo loader"));
            }
            trades.putAll(batch);
        }
        double secs = (System.nanoTime() - start) / 1e9;
        say("wrote %,d trades in %.2f s (%,.0f records/s)", records, secs, records / secs);
    }

    private static <T> void runQuery(GalaxyCache<T> cache, Filter filter) {
        long t0 = System.nanoTime();
        List<T> result = cache.query(Query.where(filter));
        double ms = (System.nanoTime() - t0) / 1e6;
        say("%-75s -> %,7d objects in %8.2f ms", filter, result.size(), ms);
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("== " + title);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
