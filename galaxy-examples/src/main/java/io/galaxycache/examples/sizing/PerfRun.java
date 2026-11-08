package io.galaxycache.examples.sizing;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static io.galaxycache.common.query.Filters.*;

/**
 * Loads N BaseTransaction objects (key = transactionId, 6 indexed attributes, codec from the domain) and then
 * measures queries, getAll and get-by-id. Reads use the client's readParallelism threads (config: 8).
 * <pre>
 * java -Xmx4g -cp galaxy-examples.jar io.galaxycache.examples.sizing.PerfRun --config config/galaxy.yaml
 *      [--records 1000000] [--writers 8] [--fetch-limit 5000] [--skip-load]
 * </pre>
 */
public final class PerfRun {

    private PerfRun() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy.yaml";
        int records = 1_000_000;
        int writers = 8;
        int fetchLimit = 5_000;
        boolean skipLoad = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--records" -> records = Integer.parseInt(args[++i]);
                case "--writers" -> writers = Integer.parseInt(args[++i]);
                case "--fetch-limit" -> fetchLimit = Integer.parseInt(args[++i]);
                case "--skip-load" -> skipLoad = true;
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        GalaxyConfig config = GalaxyConfig.load(Path.of(configPath));
        DatasetId ds = DatasetId.of("sizing", "txn", "perf1m", LocalDate.now());
        try (GalaxyClient client = new GalaxyClient(config)) {
            if (!client.listNamespaces().containsKey("sizing")) {
                client.createNamespace("sizing", "local-fs-1", "idx-a");
            }
            if (!skipLoad && client.listDatasets("sizing").contains(ds)) {
                client.dropDataset(ds);
            }
            GalaxyCache<BaseTransaction> cache = client.cache(ds, BaseTransaction.class);
            say("dataset %s  codec=%s  key=transactionId  indexed=%s", ds, cache.metadata().codec(),
                    io.galaxycache.common.domain.DomainDescriptor.of(BaseTransaction.class).indexedFieldNames());
            say("client read threads (readParallelism) = %s, readBatchSize = %s, JVM max heap = %,d MB",
                    config.client().readParallelism(), config.client().readBatchSize(),
                    Runtime.getRuntime().maxMemory() >> 20);
            if (!skipLoad) {
                load(cache, records, writers);
            }
            var st = cache.stats();
            say("stored: %,d objects, %,.1f GiB on disk in %d segment files", st.liveRecords(),
                    st.bytesOnDisk() / (double) (1L << 30), st.files());
            queries(cache, fetchLimit);
            reads(cache, (int) st.liveRecords());
        }
    }

    // ------------------------------------------------------------------ load

    private static void load(GalaxyCache<BaseTransaction> cache, int records, int writers) throws Exception {
        section(String.format("Loading %,d objects with %d writer threads (generate + serialize in parallel, "
                + "storage appends serialised by the dataset lock)", records, writers));
        int batch = 250;
        AtomicLong next = new AtomicLong();
        AtomicLong done = new AtomicLong();
        long t0 = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        List<Future<?>> fs = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
            int seed = w;
            fs.add(pool.submit(() -> {
                SizingRun gen = new SizingRun(1000 + seed);
                long from;
                while ((from = next.getAndAdd(batch)) < records) {
                    List<BaseTransaction> chunk = new ArrayList<>(batch);
                    for (long i = from; i < Math.min(records, from + batch); i++) {
                        chunk.add(gen.build(String.format("TX%010d", i)));
                    }
                    cache.putAll(chunk);
                    long d = done.addAndGet(chunk.size());
                    if (d % 50_000 < batch) {
                        double s = (System.nanoTime() - t0) / 1e9;
                        say("  %,9d written  %6.0f s  %,6.0f objects/s", d, s, d / s);
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        pool.shutdown();
        double secs = (System.nanoTime() - t0) / 1e9;
        say("loaded %,d objects in %.0f s = %,.0f objects/s", records, secs, records / secs);
    }

    // ------------------------------------------------------------------ queries

    private static void queries(GalaxyCache<BaseTransaction> cache, int fetchLimit) {
        LocalDate today = LocalDate.now();
        section("Queries: count (index only), ids (index only), fetch (index + 8-thread storage read + "
                + "deserialize, first " + String.format("%,d", fetchLimit) + " matches). 3 runs each, ms");
        System.out.printf("   %-78s %9s %8s %8s %24s %10s%n", "filter", "matches", "count", "ids",
                "fetch run1/run2/run3", "obj/s");
        List<Filter> filters = List.of(
                eq("bookId", "BOOK-42"),
                and(eq("currency", "EUR"), eq("lifecycleStatus", "SETTLED"), eq("deskCode", "RATES-NY")),
                and(eq("transactionType", "IRS"), eq("tradeDate", today.minusDays(3))),
                in("bookId", "BOOK-1", "BOOK-2", "BOOK-3"),
                and(between("tradeDate", today.minusDays(6), today.minusDays(5)), eq("currency", "USD")),
                prefix("bookId", "BOOK-42"),
                and(eq("deskCode", "CREDIT"), not(eq("lifecycleStatus", "CANCELLED")), gte("tradeDate", today.minusDays(7))),
                or(eq("transactionType", "REPO"), eq("currency", "CHF")),
                not(eq("currency", "USD")),
                all());
        for (Filter f : filters) {
            long[] count = new long[1];
            double countMs = ms(() -> count[0] = cache.count(f));
            double idsMs = ms(() -> cache.queryIds(Query.where(f).limit(Integer.MAX_VALUE)));
            double[] fetch = new double[3];
            int[] got = new int[1];
            for (int r = 0; r < 3; r++) {
                fetch[r] = ms(() -> got[0] = cache.query(Query.where(f).limit(fetchLimit)).size());
            }
            double best = Arrays.stream(fetch).min().orElseThrow();
            String fl = f.toString();
            System.out.printf("   %-78s %,9d %8.1f %8.1f %8.0f/%7.0f/%7.0f %,10.0f%n",
                    fl.length() > 78 ? fl.substring(0, 75) + "..." : fl, count[0], countMs, idsMs,
                    fetch[0], fetch[1], fetch[2], got[0] / (best / 1000));
        }
    }

    // ------------------------------------------------------------------ point and bulk reads

    private static void reads(GalaxyCache<BaseTransaction> cache, int n) {
        Random r = new Random(99);
        section("getAll by ids (random ids across the whole dataset, 8 read threads)");
        for (int size : new int[]{100, 1_000, 5_000}) {
            List<String> ids = randomIds(r, n, size);
            double cold = ms(() -> cache.getAll(ids));
            double warm = ms(() -> cache.getAll(ids));
            say("getAll(%,5d ids): first %,8.0f ms (%,6.0f obj/s)   repeat %,8.0f ms (%,6.0f obj/s)", size, cold,
                    size / (cold / 1000), warm, size / (warm / 1000));
        }

        section("get by id (single-object reads, one after another, random ids)");
        int count = 2_000;
        List<String> ids = randomIds(r, n, count);
        double[] lat = new double[count];
        long t0 = System.nanoTime();
        for (int i = 0; i < count; i++) {
            String id = ids.get(i);
            lat[i] = ms(() -> cache.get(id).orElseThrow());
        }
        double total = (System.nanoTime() - t0) / 1e6;
        Arrays.sort(lat);
        say("%,d gets in %,.0f ms (%,.0f gets/s)  latency ms: p50 %.2f  p90 %.2f  p99 %.2f  max %.2f", count,
                total, count / (total / 1000), lat[count / 2], lat[count * 9 / 10], lat[count * 99 / 100],
                lat[count - 1]);

        section("get by id from 8 client threads at once");
        List<String> many = randomIds(r, n, 8_000);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        long p0 = System.nanoTime();
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            List<String> part = many.subList(t * 1_000, (t + 1) * 1_000);
            fs.add(pool.submit(() -> part.forEach(id -> cache.get(id).orElseThrow())));
        }
        for (Future<?> f : fs) {
            try {
                f.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        pool.shutdown();
        double pms = (System.nanoTime() - p0) / 1e6;
        say("8 threads x 1,000 gets in %,.0f ms = %,.0f gets/s", pms, 8_000 / (pms / 1000));
    }

    private static List<String> randomIds(Random r, int n, int size) {
        List<String> ids = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            ids.add(String.format("TX%010d", r.nextInt(n)));
        }
        return ids;
    }

    private interface Action {
        void run() throws Exception;
    }

    private static double ms(Action a) {
        long t0 = System.nanoTime();
        try {
            a.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return (System.nanoTime() - t0) / 1e6;
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("== " + title);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
