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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static io.galaxycache.common.query.Filters.*;

/**
 * Imports the 1M BaseTransaction dataset (direct file storage, namespace "sizing") into the 3-node grid
 * (namespace "grid") through the normal cache API, then measures the same operations on both paths.
 * <pre>
 * java -Xmx3g -cp galaxy-examples.jar io.galaxycache.examples.sizing.GridBench --config config/galaxy.yaml
 *      [--import] [--bench] [--read-all]
 * </pre>
 */
public final class GridBench {

    private GridBench() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy.yaml";
        boolean doImport = false;
        boolean bench = false;
        boolean readAll = false;
        boolean reindex = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--import" -> doImport = true;
                case "--bench" -> bench = true;
                case "--read-all" -> readAll = true;
                case "--reindex" -> reindex = true;
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        GalaxyConfig config = GalaxyConfig.load(Path.of(configPath));
        DatasetId srcId = DatasetId.of("sizing", "txn", "perf1m", LocalDate.of(2026, 11, 7));
        DatasetId gridId = DatasetId.of("grid", "txn", "perf1m", LocalDate.of(2026, 11, 7));
        try (GalaxyClient client = new GalaxyClient(config)) {
            // the direct copy may be gone (deleted once the grid held its own); then only the grid is measured
            GalaxyCache<BaseTransaction> direct = client.listDatasets("sizing").contains(srcId)
                    ? client.cache(srcId, BaseTransaction.class) : null;
            if (doImport && client.listDatasets("grid").contains(gridId)) {
                client.dropDataset(gridId);
            }
            GalaxyCache<BaseTransaction> grid = client.cache(gridId, BaseTransaction.class);
            int n = (int) (direct != null ? direct.stats() : grid.stats()).liveRecords();
            if (reindex) {
                section("Reindex the grid from its records (scan, decode, extract index fields, upsert, persisted on nodes)");
                long t0 = System.nanoTime();
                long docs = grid.reindex();
                double s = (System.nanoTime() - t0) / 1e9;
                say("reindexed %,d objects in %.0f s = %,.0f objects/s", docs, s, docs / s);
            }
            if (doImport) {
                importAll(direct, grid, n);
            }
            var gs = grid.stats();
            say("grid dataset %s: %,d objects, %.1f GiB on disk across 3 nodes", gridId, gs.liveRecords(),
                    gs.bytesOnDisk() / (double) (1L << 30));
            var store = (io.galaxycache.grid.client.GridDataStore) io.galaxycache.storage.StorageProviders
                    .open("grid", "grid-1", config.storage("grid-1").settings());
            store.statsPerNode(gridId).forEach((node, st) -> say("  %-7s %,9d objects  %6.1f GiB", node,
                    st.liveRecords(), st.bytesOnDisk() / (double) (1L << 30)));
            if (bench) {
                bench(direct, grid, n);
            }
            if (readAll) {
                readAll(grid, n, "grid");
            }
        }
    }

    // ------------------------------------------------------------------ import

    private static void importAll(GalaxyCache<BaseTransaction> src, GalaxyCache<BaseTransaction> dst, int n)
            throws Exception {
        section(String.format("Import %,d objects: direct storage -> 3-node grid (4 workers, chunks of 2,000)", n));
        int chunk = 2_000;
        AtomicLong next = new AtomicLong();
        AtomicLong done = new AtomicLong();
        long t0 = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(4);
        List<Future<?>> fs = new ArrayList<>();
        for (int w = 0; w < 4; w++) {
            fs.add(pool.submit(() -> {
                long from;
                while ((from = next.getAndAdd(chunk)) < n) {
                    List<String> ids = new ArrayList<>(chunk);
                    for (long i = from; i < Math.min(n, from + chunk); i++) {
                        ids.add(String.format("TX%010d", i));
                    }
                    Map<String, BaseTransaction> got = src.getAll(ids);
                    dst.putAll(got.values()); // routed by @GalaxyKey to the owner node
                    long d = done.addAndGet(got.size());
                    if (d % 100_000 < chunk) {
                        double s = (System.nanoTime() - t0) / 1e9;
                        say("  %,9d imported  %5.0f s  %,6.0f objects/s", d, s, d / s);
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        pool.shutdown();
        double s = (System.nanoTime() - t0) / 1e9;
        say("imported %,d objects in %.0f s = %,.0f objects/s (read direct + decode + encode + write to grid)",
                done.get(), s, done.get() / s);
    }

    // ------------------------------------------------------------------ benchmark

    private static void bench(GalaxyCache<BaseTransaction> direct, GalaxyCache<BaseTransaction> grid, int n) {
        LocalDate d = LocalDate.of(2026, 11, 7);
        section("Queries, fetch first 5,000 matches, best of 3 (ms); direct = idx-a pod + file storage, grid = 3 nodes");
        System.out.printf("   %-62s %9s | %8s %8s | %8s %8s%n", "filter", "matches", "dir ids", "dir get", "grid ids",
                "grid get");
        List<Filter> filters = List.of(
                eq("bookId", "BOOK-42"),
                and(eq("currency", "EUR"), eq("lifecycleStatus", "SETTLED"), eq("deskCode", "RATES-NY")),
                and(eq("transactionType", "IRS"), eq("tradeDate", d.minusDays(3))),
                in("bookId", "BOOK-1", "BOOK-2", "BOOK-3"),
                and(between("tradeDate", d.minusDays(6), d.minusDays(5)), eq("currency", "USD")),
                prefix("bookId", "BOOK-42"),
                or(eq("transactionType", "REPO"), eq("currency", "CHF")),
                not(eq("currency", "USD")),
                all());
        for (Filter f : filters) {
            long gc = grid.count(f);
            long dc = direct == null ? gc : direct.count(f);
            double di = direct == null ? Double.NaN : best(() -> direct.queryIds(Query.where(f)));
            double gi = best(() -> grid.queryIds(Query.where(f)));
            double dq = direct == null ? Double.NaN : best(() -> direct.query(Query.where(f).limit(5_000)));
            double gq = best(() -> grid.query(Query.where(f).limit(5_000)));
            String fl = f.toString();
            System.out.printf("   %-62s %,9d | %8.1f %8.0f | %8.1f %8.0f%s%n",
                    fl.length() > 62 ? fl.substring(0, 59) + "..." : fl, gc, di, dq, gi, gq,
                    dc == gc ? "" : "   MISMATCH direct=" + dc);
        }

        Random r = new Random(5);
        section("getAll by ids (random ids, 8 read threads), best of 2");
        for (int size : new int[]{100, 1_000, 5_000, 20_000}) {
            List<String> ids = randomIds(r, n, size);
            double dm = direct == null ? Double.NaN : best2(() -> direct.getAll(ids));
            double gm = best2(() -> grid.getAll(ids));
            say("getAll(%,6d): direct %,8.0f ms (%,6.0f obj/s)   grid %,8.0f ms (%,6.0f obj/s)", size, dm,
                    size / (dm / 1000), gm, size / (gm / 1000));
        }
        List<String> consecutive = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            consecutive.add(String.format("TX%010d", 400_000 + i));
        }
        double dm = direct == null ? Double.NaN : best2(() -> direct.getAll(consecutive));
        double gm = best2(() -> grid.getAll(consecutive));
        say("getAll(20,000 consecutive): direct %,6.0f ms (%,6.0f obj/s)   grid %,6.0f ms (%,6.0f obj/s)", dm,
                20_000 / (dm / 1000), gm, 20_000 / (gm / 1000));

        section("get(id): 2,000 random ids one after another, then 8,000 from 8 threads");
        List<String> ids = randomIds(r, n, 2_000);
        List<Map.Entry<String, GalaxyCache<BaseTransaction>>> paths = new ArrayList<>();
        if (direct != null) {
            paths.add(Map.entry("direct", direct));
        }
        paths.add(Map.entry("grid", grid));
        for (var e : paths) {
            GalaxyCache<BaseTransaction> c = e.getValue();
            ids.forEach(id -> c.get(id)); // warm
            double[] lat = new double[ids.size()];
            for (int i = 0; i < lat.length; i++) {
                String id = ids.get(i);
                lat[i] = ms(() -> c.get(id).orElseThrow());
            }
            Arrays.sort(lat);
            List<String> many = randomIds(r, n, 8_000);
            double par = ms(() -> parallelGets(c, many));
            say("%-6s single: p50 %.2f  p90 %.2f  p99 %.2f ms | 8 threads: %,6.0f gets/s", e.getKey(),
                    lat[lat.length / 2], lat[lat.length * 9 / 10], lat[lat.length * 99 / 100], 8_000 / (par / 1000));
        }
    }

    private static void readAll(GalaxyCache<BaseTransaction> cache, int n, String label) {
        section("Read all " + String.format("%,d", n) + " objects from the " + label + " (getAll, key order, chunks of 10,000)");
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ids.add(String.format("TX%010d", i));
        }
        long t0 = System.nanoTime();
        long read = 0;
        for (int from = 0; from < n; from += 10_000) {
            read += cache.getAll(ids.subList(from, Math.min(n, from + 10_000))).size();
            if ((from / 10_000) % 20 == 19) {
                double s = (System.nanoTime() - t0) / 1e9;
                say("  %,9d read  %5.1f s  %,6.0f objects/s", read, s, read / s);
            }
        }
        double s = (System.nanoTime() - t0) / 1e9;
        say("TOTAL %,d objects in %.1f s = %,.0f objects/s", read, s, read / s);
        Collections.shuffle(ids, new Random(3));
    }

    private static void parallelGets(GalaxyCache<BaseTransaction> c, List<String> ids) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(8);
        List<Future<?>> fs = new ArrayList<>();
        for (int t = 0; t < 8; t++) {
            List<String> part = ids.subList(t * ids.size() / 8, (t + 1) * ids.size() / 8);
            fs.add(pool.submit(() -> part.forEach(id -> c.get(id).orElseThrow())));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        pool.shutdown();
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

    private static double best(Action a) {
        return Math.min(ms(a), Math.min(ms(a), ms(a)));
    }

    private static double best2(Action a) {
        return Math.min(ms(a), ms(a));
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("== " + title);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
