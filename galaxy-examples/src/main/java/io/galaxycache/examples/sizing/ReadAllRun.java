package io.galaxycache.examples.sizing;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.config.GalaxyConfig;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Reads every object of the 1M dataset three ways and times each pass. Objects are consumed chunk by chunk
 * (1M deserialized objects would not fit in memory at once).
 * <pre>
 * java -Xmx4g -cp galaxy-examples.jar io.galaxycache.examples.sizing.ReadAllRun --config config/galaxy.yaml
 *      [--chunk 10000] [--passes ordered,random,get8]
 * </pre>
 */
public final class ReadAllRun {

    private ReadAllRun() {
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy.yaml";
        int chunk = 10_000;
        String passes = "ordered,random,get8";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--chunk" -> chunk = Integer.parseInt(args[++i]);
                case "--passes" -> passes = args[++i];
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        GalaxyConfig config = GalaxyConfig.load(Path.of(configPath));
        DatasetId ds = DatasetId.of("sizing", "txn", "perf1m", LocalDate.now());
        try (GalaxyClient client = new GalaxyClient(config)) {
            GalaxyCache<BaseTransaction> cache = client.cache(ds, BaseTransaction.class);
            int n = (int) cache.stats().liveRecords();
            List<String> ids = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                ids.add(String.format("TX%010d", i));
            }
            say("dataset %s: %,d objects, %.1f GiB on disk; client read threads = %d; getAll chunk = %,d ids",
                    ds, n, cache.stats().bytesOnDisk() / (double) (1L << 30), config.client().readParallelism(), chunk);
            for (String pass : passes.split(",")) {
                switch (pass) {
                    case "ordered" -> getAllPass(cache, ids, chunk, "getAll, ids in key order");
                    case "random" -> {
                        List<String> shuffled = new ArrayList<>(ids);
                        Collections.shuffle(shuffled, new Random(7));
                        getAllPass(cache, shuffled, chunk, "getAll, ids in random order");
                    }
                    case "get8" -> getPass(cache, ids, 8);
                    default -> throw new IllegalArgumentException("unknown pass " + pass);
                }
            }
        }
    }

    private static void getAllPass(GalaxyCache<BaseTransaction> cache, List<String> ids, int chunk, String name) {
        System.out.println();
        System.out.println("== " + name + " (" + String.format("%,d", ids.size()) + " objects)");
        long t0 = System.nanoTime();
        long read = 0;
        long check = 0;
        for (int from = 0; from < ids.size(); from += chunk) {
            Map<String, BaseTransaction> got = cache.getAll(ids.subList(from, Math.min(ids.size(), from + chunk)));
            read += got.size();
            for (BaseTransaction t : got.values()) {
                check += t.legs.size(); // touch the objects so nothing is optimised away
            }
            if ((from / chunk) % 20 == 19) {
                progress(read, t0);
            }
        }
        done(read, t0, check);
    }

    private static void getPass(GalaxyCache<BaseTransaction> cache, List<String> ids, int threads) throws Exception {
        System.out.println();
        System.out.println("== get(id) one object per call, " + threads + " caller threads ("
                + String.format("%,d", ids.size()) + " objects)");
        AtomicLong read = new AtomicLong();
        AtomicLong check = new AtomicLong();
        long t0 = System.nanoTime();
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<Future<?>> fs = new ArrayList<>();
        int per = (ids.size() + threads - 1) / threads;
        for (int t = 0; t < threads; t++) {
            List<String> part = ids.subList(Math.min(ids.size(), t * per), Math.min(ids.size(), (t + 1) * per));
            fs.add(pool.submit(() -> {
                for (String id : part) {
                    check.addAndGet(cache.get(id).orElseThrow().legs.size());
                    long r = read.incrementAndGet();
                    if (r % 200_000 == 0) {
                        progress(r, t0);
                    }
                }
                return null;
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        pool.shutdown();
        done(read.get(), t0, check.get());
    }

    private static void progress(long read, long t0) {
        double s = (System.nanoTime() - t0) / 1e9;
        say("  %,9d read  %6.1f s  %,7.0f objects/s", read, s, read / s);
    }

    private static void done(long read, long t0, long check) {
        double s = (System.nanoTime() - t0) / 1e9;
        say("TOTAL %,d objects in %.1f s = %,.0f objects/s  (%,d legs seen)", read, s, read / s, check);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
