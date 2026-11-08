package io.galaxycache.examples.sizing;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.codec.BinaryCodec;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.StorageProviders;
import io.galaxycache.storage.StoredRecord;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Splits the cost of reading BaseTransaction objects into its parts, as input for remote-storage estimates:
 * dataset open (directory rebuild), storage I/O only, decode CPU only, per-call catch-up overhead and heap
 * per decoded object.
 */
public final class ProfileRun {

    private ProfileRun() {
    }

    public static void main(String[] args) throws Exception {
        GalaxyConfig config = GalaxyConfig.load(Path.of(args.length > 1 ? args[1] : "config/galaxy.yaml"));
        DatasetId ds = DatasetId.of("sizing", "txn", "perf1m", LocalDate.now());
        var sc = config.storage("local-fs-1");
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try (DataStore store = StorageProviders.open(sc.type(), "local-fs-1", sc.settings())) {
            long t0 = System.nanoTime();
            var st = store.stats(ds);
            double openS = (System.nanoTime() - t0) / 1e9;
            say("open dataset (scan all segments, rebuild id->location map): %.1f s for %,d objects / %.1f GiB = %,.0f MB/s",
                    openS, st.liveRecords(), st.bytesOnDisk() / (double) (1L << 30), st.bytesOnDisk() / openS / 1e6);
            int n = (int) st.liveRecords();

            // ---- storage I/O only (no decode), 8 threads, chunks of 512 like the client
            Random r = new Random(11);
            List<String> random = new ArrayList<>();
            for (int i = 0; i < 20_000; i++) {
                random.add(id(r.nextInt(n)));
            }
            List<String> ordered = new ArrayList<>();
            int base = 700_000; // a range not read by the earlier ordered tests' first chunks
            for (int i = 0; i < 20_000; i++) {
                ordered.add(id(base + i));
            }
            List<StoredRecord> payloads = new ArrayList<>();
            ioPass("storage read only, 20,000 random ids", store, ds, random, pool, threads, payloads);
            ioPass("storage read only, 20,000 consecutive ids", store, ds, ordered, pool, threads, null);

            // ---- decode only
            BinaryCodec codec = new BinaryCodec();
            for (int i = 0; i < 2_000; i++) { // warm up the JIT
                codec.decode(payloads.get(i).payload(), 1, BaseTransaction.class);
            }
            long d0 = System.nanoTime();
            for (int i = 0; i < 5_000; i++) {
                codec.decode(payloads.get(i).payload(), 1, BaseTransaction.class);
            }
            double usPer = (System.nanoTime() - d0) / 1e3 / 5_000;
            say("decode only, 1 thread : %.0f us/object = %,.0f objects/s per core", usPer, 1e6 / usPer);
            AtomicLong cnt = new AtomicLong();
            long p0 = System.nanoTime();
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                int from = t * payloads.size() / threads;
                int to = (t + 1) * payloads.size() / threads;
                fs.add(pool.submit(() -> {
                    for (int i = from; i < to; i++) {
                        codec.decode(payloads.get(i).payload(), 1, BaseTransaction.class);
                        cnt.incrementAndGet();
                    }
                    return null;
                }));
            }
            for (Future<?> f : fs) {
                f.get();
            }
            double ps = (System.nanoTime() - p0) / 1e9;
            say("decode only, %d threads: %,.0f objects/s", threads, cnt.get() / ps);

            // ---- heap per decoded object
            Runtime rt = Runtime.getRuntime();
            List<Object> keep = new ArrayList<>();
            gc();
            long before = rt.totalMemory() - rt.freeMemory();
            for (int i = 0; i < 2_000; i++) {
                keep.add(codec.decode(payloads.get(i).payload(), 1, BaseTransaction.class));
            }
            gc();
            long after = rt.totalMemory() - rt.freeMemory();
            long avgPayload = (long) payloads.stream().limit(2_000).mapToInt(p -> p.payload().length).average().orElse(0);
            say("heap per decoded object: %,d bytes (binary payload %,d bytes, JSON ~66,400) -> 5M objects = %.0f GB heap",
                    (after - before) / keep.size(), avgPayload, (after - before) / (double) keep.size() * 5e6 / 1e9);
            keep.clear();

            // ---- per-call overhead (catch-up check of the _commit file + one record read), hot cache
            String hot = id(5);
            store.get(ds, hot);
            long g0 = System.nanoTime();
            for (int i = 0; i < 20_000; i++) {
                store.get(ds, hot);
            }
            say("get(id) on a cached record: %.1f us per call (commit-file check + read + CRC)",
                    (System.nanoTime() - g0) / 1e3 / 20_000);
        } finally {
            pool.shutdown();
        }
    }

    private static void ioPass(String name, DataStore store, DatasetId ds, List<String> ids, ExecutorService pool,
                               int threads, List<StoredRecord> keep) throws Exception {
        AtomicLong bytes = new AtomicLong();
        List<Future<Map<String, StoredRecord>>> fs = new ArrayList<>();
        long t0 = System.nanoTime();
        for (int i = 0; i < ids.size(); i += 512) {
            Collection<String> part = ids.subList(i, Math.min(ids.size(), i + 512));
            fs.add(pool.submit(() -> {
                Map<String, StoredRecord> m = store.getAll(ds, part);
                m.values().forEach(rec -> bytes.addAndGet(rec.payload().length));
                return m;
            }));
        }
        long n = 0;
        for (Future<Map<String, StoredRecord>> f : fs) {
            Map<String, StoredRecord> m = f.get();
            n += m.size();
            if (keep != null) {
                keep.addAll(m.values());
            }
        }
        double s = (System.nanoTime() - t0) / 1e9;
        say("%-45s: %,d objects in %.2f s = %,.0f objects/s, %,.0f MB/s payload (%d threads)", name, n, s, n / s,
                bytes.get() / s / 1e6, threads);
    }

    private static String id(int i) {
        return String.format("TX%010d", i);
    }

    private static void gc() throws InterruptedException {
        for (int i = 0; i < 3; i++) {
            System.gc();
            Thread.sleep(200);
        }
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
