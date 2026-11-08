package io.galaxycache.examples.sizing;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.GalaxyKey;
import io.galaxycache.common.domain.Indexed;
import io.galaxycache.common.query.Filters;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.client.GridCompact;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Soak and crash test for the grid. Writers update, delete and bulk-write keys while readers check every answer
 * against an exact model of what was acknowledged, and compaction runs periodically. Each key belongs to one writer,
 * so its history is sequential and the model is exact.
 * <pre>
 * GridSoak --config galaxy.yaml --minutes 10 [--compact-every 120]   run, then verify every key and index count
 * GridSoak --config galaxy.yaml --until-failure --model soak.bin     run until the grid dies, save the model
 * GridSoak --config galaxy.yaml --verify-only --model soak.bin       after a restart: every acknowledged write must be there
 * </pre>
 * Exit status 0 means no violation. Model: version &gt; 0 live at that version, &lt; 0 deleted at that version, 0 never written.
 */
public final class GridSoak {

    @GalaxyDomain(name = "SoakItem", version = 1, codec = "binary")
    public static class SoakItem {
        @GalaxyKey public String id;
        @Indexed public int bucket;
        @Indexed public long version;
        public String payload;

        public SoakItem() {
        }
    }

    private static final int BUCKETS = 16;
    private static final DatasetId DS = DatasetId.of("grid", "soak", "run", "2026-11-08");

    private final int keys;
    private final AtomicLongArray model;
    /** Non-zero while a write to the key is outstanding: its outcome is not known. */
    private final AtomicIntegerArray inFlight;
    private final AtomicLong writes = new AtomicLong();
    private final AtomicLong checkedReads = new AtomicLong();
    private final AtomicLong violations = new AtomicLong();
    private final AtomicBoolean stop = new AtomicBoolean();

    private GridSoak(int keys) {
        this.keys = keys;
        this.model = new AtomicLongArray(keys);
        this.inFlight = new AtomicIntegerArray(keys);
    }

    public static void main(String[] args) throws Exception {
        String config = "config/galaxy.yaml";
        Path modelFile = null;
        double minutes = 5;
        int compactEvery = 120;
        int keys = 20_000;
        boolean untilFailure = false;
        boolean verifyOnly = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> config = args[++i];
                case "--minutes" -> minutes = Double.parseDouble(args[++i]);
                case "--compact-every" -> compactEvery = Integer.parseInt(args[++i]);
                case "--keys" -> keys = Integer.parseInt(args[++i]);
                case "--model" -> modelFile = Path.of(args[++i]);
                case "--until-failure" -> untilFailure = true;
                case "--verify-only" -> verifyOnly = true;
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        System.setProperty("galaxy.grid.onFailure", "throw"); // this tool reports the failure itself
        GalaxyConfig cfg = GalaxyConfig.load(Path.of(config));
        GridSoak soak = new GridSoak(keys);
        long bad;
        if (verifyOnly) {
            soak.load(modelFile);
            try (GalaxyClient client = new GalaxyClient(cfg)) {
                bad = soak.verify(client.cache(DS, SoakItem.class), true);
            }
        } else {
            bad = soak.run(cfg, minutes, compactEvery, untilFailure, modelFile);
        }
        System.out.println(bad == 0 ? "RESULT: PASS, no violations" : "RESULT: FAIL, " + bad + " violations");
        System.exit(bad == 0 ? 0 : 1);
    }

    private long run(GalaxyConfig cfg, double minutes, int compactEvery, boolean untilFailure, Path modelFile) throws Exception {
        long end = System.currentTimeMillis() + (long) (minutes * 60_000);
        AtomicBoolean gridFailed = new AtomicBoolean();
        try (GalaxyClient client = new GalaxyClient(cfg)) {
            if (client.listDatasets("grid").contains(DS)) {
                client.dropDataset(DS);
            }
            GalaxyCache<SoakItem> cache = client.cache(DS, SoakItem.class);
            int writers = 4;
            int readers = 4;
            ExecutorService pool = Executors.newFixedThreadPool(writers + readers);
            List<Future<?>> fs = new ArrayList<>();
            for (int w = 0; w < writers; w++) {
                int me = w;
                fs.add(pool.submit(() -> guarded(gridFailed, () -> writer(cache, me, writers))));
            }
            for (int r = 0; r < readers; r++) {
                int seed = r;
                fs.add(pool.submit(() -> guarded(gridFailed, () -> reader(cache, seed))));
            }
            long nextCompact = System.currentTimeMillis() + compactEvery * 1000L;
            long nextReport = System.currentTimeMillis() + 30_000;
            int compactions = 0;
            while (!gridFailed.get() && (untilFailure || System.currentTimeMillis() < end)) {
                Thread.sleep(200);
                if (System.currentTimeMillis() > nextReport) {
                    System.out.printf("%,d writes, %,d checked reads, %d compactions, %d violations%n", writes.get(),
                            checkedReads.get(), compactions, violations.get());
                    nextReport += 30_000;
                }
                if (!untilFailure && System.currentTimeMillis() > nextCompact) {
                    long reclaimed = GridCompact.run(cfg.storage("grid-1").settings()).values().stream()
                            .mapToLong(Long::longValue).sum();
                    compactions++;
                    System.out.printf("compaction %d reclaimed %,d MB%n", compactions, reclaimed >> 20);
                    nextCompact = System.currentTimeMillis() + compactEvery * 1000L;
                }
            }
            stop.set(true);
            for (Future<?> f : fs) {
                f.get();
            }
            pool.shutdown();
            System.out.printf("finished: %,d writes, %,d checked reads, %d compactions, %d violations during the run%n",
                    writes.get(), checkedReads.get(), compactions, violations.get());
            if (untilFailure) {
                save(modelFile);
                return violations.get();
            }
            return violations.get() + verify(cache, false);
        }
    }

    private interface Body {
        void run() throws Exception;
    }

    /** The grid failing is the expected end of an until-failure run; anything else is a violation. */
    private void guarded(AtomicBoolean gridFailed, Body body) {
        try {
            body.run();
        } catch (io.galaxycache.grid.api.GridUnavailableException e) {
            if (gridFailed.compareAndSet(false, true)) {
                System.out.println("grid failed: " + e.getMessage());
            }
        } catch (Exception e) {
            if (e.getMessage() != null && e.getMessage().contains("grid unavailable")) {
                gridFailed.set(true);
                return;
            }
            violations.incrementAndGet();
            System.out.println("VIOLATION: unexpected " + e);
            gridFailed.set(true);
        }
    }

    private void writer(GalaxyCache<SoakItem> cache, int me, int writers) {
        Random r = new Random(1000 + me);
        while (!stop.get()) {
            int roll = r.nextInt(100);
            if (roll < 15) {
                int n = 50 + r.nextInt(200);
                List<SoakItem> batch = new ArrayList<>(n);
                List<Integer> indexes = new ArrayList<>(n);
                java.util.Set<Integer> chosen = new java.util.HashSet<>();
                while (chosen.size() < n) {
                    int key = key(r, me, writers);
                    if (chosen.add(key)) {
                        inFlight.set(key, 1);
                        batch.add(item(key, Math.abs(model.get(key)) + 1, r));
                        indexes.add(key);
                    }
                }
                cache.putAll(batch);
                for (int i = 0; i < batch.size(); i++) {
                    model.set(indexes.get(i), batch.get(i).version);
                    inFlight.set(indexes.get(i), 0);
                }
                writes.addAndGet(n);
            } else {
                int key = key(r, me, writers);
                long next = Math.abs(model.get(key)) + 1;
                inFlight.set(key, 1);
                if (roll < 30 && model.get(key) > 0) {
                    cache.delete(id(key));
                    model.set(key, -next);
                } else {
                    cache.put(item(key, next, r));
                    model.set(key, next);
                }
                inFlight.set(key, 0);
                writes.incrementAndGet();
            }
        }
    }

    private int key(Random r, int me, int writers) {
        return (r.nextInt(keys / writers) * writers) + me;
    }

    private void reader(GalaxyCache<SoakItem> cache, int seed) {
        Random r = new Random(2000 + seed);
        while (!stop.get()) {
            int key = r.nextInt(keys);
            long before = model.get(key);
            boolean busyBefore = inFlight.get(key) != 0;
            var got = cache.get(id(key));
            if (busyBefore || inFlight.get(key) != 0 || model.get(key) != before) {
                continue; // a write overlapped this read: either answer is legal
            }
            checkedReads.incrementAndGet();
            check(key, before, got.orElse(null));
        }
    }

    private boolean check(int key, long expected, SoakItem got) {
        boolean ok = expected > 0 ? got != null && got.version == expected
                : got == null;
        if (ok && got != null) {
            ok = got.id.equals(id(key)) && got.bucket == (int) (got.version % BUCKETS) && got.payload.length() == payloadLength(key, got.version);
        }
        if (!ok) {
            violations.incrementAndGet();
            System.out.printf("VIOLATION: %s expected %d, got %s%n", id(key), expected,
                    got == null ? "nothing" : "version " + got.version);
        }
        return ok;
    }

    /** Checks every key and the index. With {@code crashed}, a key with an outstanding write may hold either outcome. */
    private long verify(GalaxyCache<SoakItem> cache, boolean crashed) {
        long bad = 0;
        long live = 0;
        long uncertain = 0;
        long[] buckets = new long[BUCKETS];
        for (int from = 0; from < keys; from += 2_000) {
            List<String> ids = new ArrayList<>();
            for (int k = from; k < Math.min(keys, from + 2_000); k++) {
                ids.add(id(k));
            }
            Map<String, SoakItem> got = new LinkedHashMap<>(cache.getAll(ids));
            for (int k = from; k < Math.min(keys, from + 2_000); k++) {
                SoakItem item = got.get(id(k));
                long expected = model.get(k);
                if (crashed && inFlight.get(k) != 0) {
                    uncertain++;
                    long v = Math.abs(expected);
                    if (item != null && (item.version < v || item.version > v + 1)) {
                        bad++;
                        System.out.printf("VIOLATION: in-flight %s held version %d, acknowledged %d%n", id(k), item.version, expected);
                    }
                    continue;
                }
                long before = violations.get();
                check(k, expected, item);
                bad += violations.get() - before;
                if (expected > 0) {
                    live++;
                    buckets[(int) (expected % BUCKETS)]++;
                }
            }
        }
        long stored = cache.stats().liveRecords();
        long indexed = cache.count(Filters.all());
        if (uncertain == 0) {
            if (stored != live || indexed != live) {
                bad++;
                System.out.printf("VIOLATION: expected %,d live, storage has %,d, index has %,d%n", live, stored, indexed);
            }
            for (int b = 0; b < BUCKETS; b++) {
                long n = cache.count(Filters.eq("bucket", b));
                if (n != buckets[b]) {
                    bad++;
                    System.out.printf("VIOLATION: bucket %d: expected %,d, index has %,d%n", b, buckets[b], n);
                }
            }
        }
        System.out.printf("verified %,d keys: %,d live, %,d with an unknown last write; storage %,d, index %,d%n", keys, live,
                uncertain, stored, indexed);
        return bad;
    }

    private SoakItem item(int key, long version, Random r) {
        SoakItem s = new SoakItem();
        s.id = id(key);
        s.version = version;
        s.bucket = (int) (version % BUCKETS);
        s.payload = "p".repeat(payloadLength(key, version));
        return s;
    }

    private static int payloadLength(int key, long version) {
        return 100 + (int) ((key * 31L + version * 17L) % 4_000);
    }

    private static String id(int key) {
        return String.format("S%08d", key);
    }

    private void save(Path f) throws Exception {
        ByteBuffer b = ByteBuffer.allocate(keys * 12);
        for (int k = 0; k < keys; k++) {
            b.putLong(model.get(k)).putInt(inFlight.get(k));
        }
        Files.write(f, Arrays.copyOf(b.array(), b.position()));
        long busy = 0;
        for (int k = 0; k < keys; k++) {
            busy += inFlight.get(k);
        }
        System.out.printf("saved model to %s (%,d keys had a write in flight when the grid died)%n", f, busy);
    }

    private void load(Path f) throws Exception {
        ByteBuffer b = ByteBuffer.wrap(Files.readAllBytes(f));
        for (int k = 0; k < keys; k++) {
            model.set(k, b.getLong());
            inFlight.set(k, b.getInt());
        }
    }
}
