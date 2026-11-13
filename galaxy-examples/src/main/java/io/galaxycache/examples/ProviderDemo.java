package io.galaxycache.examples;

import io.galaxycache.client.Capability;
import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.client.GalaxyCluster;
import io.galaxycache.client.NotSupportedException;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;

import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.galaxycache.common.query.Filters.*;

/**
 * The same application code on any provider: only the config file changes.
 *
 * <pre>
 * java -cp galaxy-examples.jar io.galaxycache.examples.ProviderDemo --config config/galaxy-ignite.yaml
 * java -cp galaxy-examples.jar io.galaxycache.examples.ProviderDemo --config config/galaxy.yaml --namespace trading
 * </pre>
 * Operations the provider lacks are reported as "not supported" instead of failing the demo.
 */
public final class ProviderDemo {

    private static final String[] SYMBOLS = {"AAPL", "MSFT", "AMZN", "GOOG", "NVDA", "META", "TSLA", "JPM", "V", "XOM"};

    private ProviderDemo() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy-ignite.yaml";
        String namespace = "trading";
        int records = 20_000;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--namespace" -> namespace = args[++i];
                case "--records" -> records = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        try (GalaxyClient client = GalaxyClient.fromConfig(Path.of(configPath))) {
            section("Provider");
            say("provider = %s", client.provider());
            EnumSet<Capability> missing = EnumSet.allOf(Capability.class);
            missing.removeAll(client.capabilities());
            say("supported     : %s", client.capabilities());
            say("not supported : %s", missing);

            GalaxyCluster cluster = client.cluster();
            if (cluster.supports(Capability.CLUSTER_VIEW)) {
                say("cluster %s is %s, nodes:", cluster.name(), cluster.state());
                cluster.nodes().forEach(n -> say("  %s", n));
            } else {
                say("no cluster view (single process or file storage)");
            }

            DatasetId id = DatasetId.of(namespace, "demo", "provider", LocalDate.now());
            GalaxyCache<Trade> trades = client.cache(id, Trade.class);
            section("Load " + String.format("%,d", records) + " trades into " + id);
            Random r = new Random(42);
            long t0 = System.nanoTime();
            Map<String, Trade> batch = new LinkedHashMap<>();
            for (int i = 0; i < records; i++) {
                batch.put(String.format("T%08d", i), new Trade(SYMBOLS[r.nextInt(SYMBOLS.length)], "EQ-FLOW",
                        r.nextBoolean() ? Trade.Side.BUY : Trade.Side.SELL, 1 + r.nextInt(10_000), LocalDate.now(),
                        "ACME-BANK", Math.round((50 + r.nextDouble() * 450) * 100) / 100.0, "trader" + r.nextInt(40),
                        "provider demo"));
                if (batch.size() == 5_000) {
                    trades.putAll(batch);
                    batch.clear();
                }
            }
            trades.putAll(batch);
            say("wrote %,d trades in %.2f s", records, (System.nanoTime() - t0) / 1e9);

            section("Queries (same filters on every provider)");
            runQuery(trades, eq("symbol", "AAPL"));
            runQuery(trades, and(eq("symbol", "NVDA"), gt("quantity", 9_000)));
            runQuery(trades, or(eq("side", "SELL"), prefix("symbol", "TS")));
            say("count(symbol = MSFT) = %,d", trades.count(eq("symbol", "MSFT")));
            say("first 3 ids: %s", trades.queryIds(Query.where(eq("symbol", "MSFT")).limit(3)));

            section("Optional operations");
            Trade t = trades.get("T00000001").orElseThrow();
            attempt("putIfAbsent on an existing id", () -> "inserted = " + trades.putIfAbsent("T00000001", t));
            attempt("put with a 1 minute TTL", () -> {
                trades.put("TTL-1", t, Duration.ofMinutes(1));
                return "ok";
            });
            attempt("transaction moving quantity between two trades", () -> trades.inTransaction(tx -> {
                Trade a = tx.get("T00000002").orElseThrow();
                Trade b = tx.get("T00000003").orElseThrow();
                a.quantity -= 1;
                b.quantity += 1;
                tx.put("T00000002", a);
                tx.put("T00000003", b);
                return "committed";
            }));
            attempt("node stats", () -> cluster.nodeStats().toString());
            attempt("compact", () -> cluster.compact().toString());

            client.dropDataset(id);
            say("dropped %s", id);
        }
    }

    private interface Step {
        Object run() throws Exception;
    }

    private static void attempt(String what, Step step) {
        try {
            say("%-50s -> %s", what, step.run());
        } catch (NotSupportedException e) {
            say("%-50s -> not supported (%s)", what, e.getMessage());
        } catch (Exception e) {
            say("%-50s -> FAILED: %s", what, e);
        }
    }

    private static void runQuery(GalaxyCache<Trade> cache, Filter filter) {
        long t0 = System.nanoTime();
        List<Trade> result = cache.query(Query.where(filter));
        say("%-60s -> %,6d trades in %7.2f ms", filter, result.size(), (System.nanoTime() - t0) / 1e6);
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("== " + title);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
