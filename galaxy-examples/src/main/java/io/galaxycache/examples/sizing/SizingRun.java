package io.galaxycache.examples.sizing;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.codec.Codec;
import io.galaxycache.common.codec.Codecs;
import io.galaxycache.common.codec.JsonCodec;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.examples.sizing.BaseTransaction.*;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

/**
 * Writes a sample of BaseTransaction objects (50-80 KiB each when written as JSON)
 * with the domain's codec and extrapolates disk usage.
 * <pre>
 * java -cp galaxy-examples.jar io.galaxycache.examples.sizing.SizingRun --config config/galaxy.yaml
 *      [--records 20000] [--project 1000000] [--batch 500]
 * </pre>
 */
public final class SizingRun {

    private static final String CODEC = BaseTransaction.class.getAnnotation(io.galaxycache.common.domain.GalaxyDomain.class).codec();
    private static final int MIN_BYTES = 50 * 1024;
    private static final int MAX_BYTES = 80 * 1024;
    private static final String ALNUM = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final String[] TYPES = {"IRS", "CDS", "FXFWD", "EQSWAP", "BOND", "REPO"};
    private static final String[] DESKS = {"RATES-NY", "RATES-LDN", "CREDIT", "FX-SPOT", "EQD", "FINANCING"};
    private static final String[] CCY = {"USD", "EUR", "GBP", "JPY", "CHF"};
    private static final String[] STATUS = {"NEW", "AMENDED", "CONFIRMED", "SETTLED", "CANCELLED"};

    private final Random r;
    private final JsonCodec codec = new JsonCodec();

    SizingRun(long seed) {
        this.r = new Random(seed);
    }

    public static void main(String[] args) throws Exception {
        String configPath = "config/galaxy.yaml";
        int records = 20_000;
        long project = 1_000_000;
        int batch = 500;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> configPath = args[++i];
                case "--records" -> records = Integer.parseInt(args[++i]);
                case "--project" -> project = Long.parseLong(args[++i]);
                case "--batch" -> batch = Integer.parseInt(args[++i]);
                case "--namespace" -> namespace = args[++i];
                case "--storage" -> storage = args[++i];
                case "--drop-after" -> dropAfter = true;
                case "--page-test" -> {
                    pageTest(Path.of(args[++i]), records);
                    return;
                }
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        new SizingRun(2026).run(GalaxyConfig.load(Path.of(configPath)), records, project, batch);
    }

    private static String namespace = "sizing";
    private static String storage = "local-fs-1";
    private static boolean dropAfter = false;

    private void run(GalaxyConfig config, int records, long project, int batch) throws Exception {
        section("Domain shape");
        int total = 0;
        for (Class<?> c : List.of(BaseTransaction.class, TradeEconomics.class, Party.class, PostalAddress.class,
                ContactInfo.class, Settlement.class, SettlementAccount.class, Leg.class, Cashflow.class)) {
            int n = scalarFields(c).size();
            total += n;
            say("%-20s %3d attributes", c.getSimpleName(), n);
        }
        say("%-20s %3d attributes  (+ Map<String,String> with 100 entries, depth 3)", "TOTAL", total);

        try (GalaxyClient client = new GalaxyClient(config)) {
            if (!client.listNamespaces().containsKey(namespace)) {
                client.createNamespace(namespace, storage, "idx-a");
            }
            DatasetId ds = DatasetId.of(namespace, "txn", "base", LocalDate.now());
            if (client.listDatasets(namespace).contains(ds)) {
                client.dropDataset(ds);
            }
            GalaxyCache<BaseTransaction> cache = client.cache(ds, BaseTransaction.class);

            Codec stored = Codecs.byName(cache.metadata().codec());
            section(String.format("Writing %,d sample objects into %s (codec %s, key %s, batches of %d)", records,
                    ds, stored.name(), "transactionId", batch));
            long jsonTotal = 0;
            long payloadTotal = 0;
            int payloadMin = Integer.MAX_VALUE;
            int payloadMax = 0;
            long gzipIn = 0;
            long gzipOut = 0;
            int[] histogram = new int[4]; // JSON size <60, 60-70, 70-80, >=80 KiB
            long encodeNanos = 0;
            long cmpJson = 0;
            long cmpJava = 0;
            long cmpBinary = 0;
            long t0 = System.nanoTime();
            for (int from = 0; from < records; from += batch) {
                List<BaseTransaction> chunk = new java.util.ArrayList<>();
                for (int i = from; i < Math.min(records, from + batch); i++) {
                    BaseTransaction tx = build(String.format("TX%010d", i));
                    byte[] json = codec.encode(tx);
                    long e0 = System.nanoTime();
                    byte[] bytes = stored.encode(tx);
                    encodeNanos += System.nanoTime() - e0;
                    jsonTotal += json.length;
                    payloadTotal += bytes.length;
                    payloadMin = Math.min(payloadMin, bytes.length);
                    payloadMax = Math.max(payloadMax, bytes.length);
                    histogram[Math.min(3, Math.max(0, (json.length / 1024 - 50) / 10))]++;
                    if (i == 0) {
                        Path sample = Path.of(config.logging().queryLogDir()).getParent().resolve("sizing-sample.json");
                        Files.write(sample, json);
                        say("sample object (as JSON, for reading) written to %s", sample.toAbsolutePath().normalize());
                        BaseTransaction back = stored.decode(bytes, 1, BaseTransaction.class);
                        if (!java.util.Arrays.equals(json, codec.encode(back))) {
                            throw new IllegalStateException("binary round trip changed the object");
                        }
                        say("round trip check: binary -> object -> JSON is identical to the original");
                    }
                    if (i < 500) {
                        cmpJava += Codecs.byName("java").encode(tx).length;
                        cmpBinary += Codecs.byName("binary").encode(tx).length;
                        cmpJson += json.length;
                        gzipIn += bytes.length;
                        gzipOut += gzip(bytes);
                    }
                    chunk.add(tx);
                }
                cache.putAll(chunk); // id = @GalaxyKey transactionId, in storage and in the index pod
                if ((from / batch) % 10 == 9) {
                    say("... %,d written", Math.min(records, from + batch));
                }
            }
            double secs = (System.nanoTime() - t0) / 1e9;
            var stats = cache.stats();

            double avgJson = (double) jsonTotal / records;
            double avgPayload = (double) payloadTotal / records;
            double perRecordDisk = (double) stats.bytesOnDisk() / stats.liveRecords();
            say("wrote %,d objects in %.1f s (%,.0f objects/s); %s encode %.1f us/object", records, secs,
                    records / secs, stored.name(), encodeNanos / 1e3 / records);

            section("Measured");
            say("same object as JSON     : avg %,.0f bytes  (spread KiB 50-60: %,d  60-70: %,d  70-80: %,d  >=80: %,d)",
                    avgJson, histogram[0], histogram[1], histogram[2], histogram[3]);
            say("stored %-6s payload  : min %,d  avg %,.0f  max %,d bytes  = %.1f%% of JSON", stored.name(),
                    payloadMin, avgPayload, payloadMax, 100 * avgPayload / avgJson);
            say("codec comparison (500)  : json %,d  java-serialization %,d  compact-binary %,d bytes/object",
                    cmpJson / 500, cmpJava / 500, cmpBinary / 500);
            say("live records            : %,d", stats.liveRecords());
            say("bytes on disk           : %,d  (%s) in %d segment files", stats.bytesOnDisk(),
                    human(stats.bytesOnDisk()), stats.files());
            say("disk per object         : %,.0f bytes  (payload %,.0f + header/id + page padding %,.0f)",
                    perRecordDisk, avgPayload, perRecordDisk - avgPayload);
            say("storage overhead        : %.1f%% on top of the payload", (perRecordDisk / avgPayload - 1) * 100);
            say("gzip of payload (500)   : %.1f%% of payload size (storage does NOT compress today)",
                    100.0 * gzipOut / gzipIn);
            List<String> ids = cache.queryIds(io.galaxycache.common.query.Query.where(
                    io.galaxycache.common.query.Filters.eq("currency", "EUR")).limit(3));
            say("index ids are the keys  : first EUR ids from the pod = %s -> get(%s).transactionId = %s", ids,
                    ids.get(0), cache.get(ids.get(0)).orElseThrow().transactionId);

            section(String.format("Projection for %,d objects", project));
            double projDisk = perRecordDisk * project;
            double projRaw = avgPayload * project;
            say("same objects as JSON    : %s", human((long) (avgJson * project)));
            say("raw %-6s payloads    : %s", stored.name(), human((long) projRaw));
            say("on disk (Galaxy store)  : %s  (~%,d x 64 MiB segment files)", human((long) projDisk),
                    (long) Math.ceil(projDisk / (64.0 * 1024 * 1024)));
            say("each full rewrite/update: + another %s (append-only, no compaction yet)", human((long) projDisk));
            say("if gzip were added      : ~%s", human((long) (projRaw * gzipOut / gzipIn)));
            if (dropAfter) {
                client.dropDataset(ds);
                say("dropped %s", ds);
            }
        }
    }

    /**
     * Storage-only experiment (no index pod): writes the same objects with several page sizes into
     * {@code dir} and reports bytes on disk per object, deleting each store afterwards.
     */
    private static void pageTest(Path dir, int records) throws Exception {
        section(String.format("Page size experiment, %,d objects each, storage only, in %s", records, dir));
        for (int pageSize : new int[]{64 * 1024, 256 * 1024, 1024 * 1024, 4 * 1024 * 1024}) {
            SizingRun gen = new SizingRun(2026);
            Path root = dir.resolve("p" + pageSize);
            DatasetId ds = DatasetId.of("sizing", "txn", "base", LocalDate.now());
            long payload = 0;
            try (var store = new io.galaxycache.storage.file.FileDataStore("page-test", root,
                    new io.galaxycache.storage.file.FileStoreOptions(pageSize, 64L * 1024 * 1024, false,
                            Math.max(pageSize, 1024 * 1024)))) {
                store.ensureDataset(new io.galaxycache.storage.DatasetMetadata(ds, "BaseTransaction",
                        BaseTransaction.class.getName(), 1, CODEC, Instant.now()));
                for (int from = 0; from < records; from += 500) {
                    List<io.galaxycache.storage.StoredRecord> chunk = new java.util.ArrayList<>();
                    for (int i = from; i < Math.min(records, from + 500); i++) {
                        String id = String.format("TX%010d", i);
                        byte[] json = Codecs.byName(CODEC).encode(gen.build(id));
                        payload += json.length;
                        chunk.add(new io.galaxycache.storage.StoredRecord(id, 1, json));
                    }
                    store.putAll(ds, chunk);
                }
                var st = store.stats(ds);
                double per = (double) st.bytesOnDisk() / st.liveRecords();
                say("pageSize %,9d : %,7.0f bytes/object on disk, overhead %5.1f%%, 1M objects = %s", pageSize,
                        per, (per / ((double) payload / records) - 1) * 100, human((long) (per * 1_000_000)));
                store.dropDataset(ds);
            }
        }
    }

    // ------------------------------------------------------------------ object generation

    BaseTransaction build(String id) throws Exception {
        BaseTransaction tx = new BaseTransaction();
        fill(tx);
        tx.transactionId = id;
        tx.transactionType = pick(TYPES);
        tx.productType = tx.transactionType + "-" + pick(new String[]{"VANILLA", "EXOTIC", "STRUCTURED"});
        tx.deskCode = pick(DESKS);
        tx.bookId = "BOOK-" + r.nextInt(500);
        tx.traderId = "TRD-" + r.nextInt(200);
        tx.counterpartyCode = "CP-" + r.nextInt(2_000);
        tx.currency = pick(CCY);
        tx.tradeDate = LocalDate.now().minusDays(r.nextInt(30));
        tx.lifecycleStatus = pick(STATUS);
        tx.comments = text(120 + r.nextInt(200));

        tx.economics = fill(new TradeEconomics());
        tx.counterparty = party();
        tx.principal = party();
        tx.settlement = fill(new Settlement());
        tx.settlement.account = fill(new SettlementAccount());
        for (int a = 1; a <= 100; a++) {
            tx.attributes.put(String.format("attr%03d_%s", a, text(6)), text(16 + r.nextInt(30)));
        }
        int legs = 2 + r.nextInt(3);
        for (int l = 0; l < legs; l++) {
            Leg leg = fill(new Leg());
            leg.legNumber = l + 1;
            tx.legs.add(leg);
        }
        tx.legCount = legs;

        // grow the cashflow schedule until the object reaches its target size (uniform 50-80 KiB)
        int target = MIN_BYTES + r.nextInt(MAX_BYTES - MIN_BYTES);
        int size = codec.encode(tx).length;
        Cashflow first = fill(new Cashflow());
        int add = codec.encode(first).length + 1; // cashflows are all about the same size: measure one
        int count = Math.max(0, (target - size) / add);
        for (int n = 0; n < count; n++) {
            tx.legs.get(n % legs).cashflows.add(n == 0 ? first : fill(new Cashflow()));
        }
        return tx;
    }

    private Party party() throws Exception {
        Party p = fill(new Party());
        p.address = fill(new PostalAddress());
        p.contact = fill(new ContactInfo());
        return p;
    }

    /** Sets every scalar field to a random value of its type. */
    private <T> T fill(T o) throws Exception {
        for (Field f : scalarFields(o.getClass())) {
            Class<?> t = f.getType();
            Object v;
            if (t == String.class) {
                v = text(6 + r.nextInt(19));
            } else if (t == long.class) {
                v = (long) r.nextInt(10_000_000);
            } else if (t == int.class) {
                v = r.nextInt(1_000);
            } else if (t == double.class) {
                v = Math.round(r.nextDouble() * 1e8) / 1e4;
            } else if (t == boolean.class) {
                v = r.nextBoolean();
            } else if (t == BigDecimal.class) {
                v = BigDecimal.valueOf(r.nextLong(1_000_000_000_000L), 2);
            } else if (t == LocalDate.class) {
                v = LocalDate.of(2020, 1, 1).plusDays(r.nextInt(5_000));
            } else if (t == Instant.class) {
                v = Instant.ofEpochMilli(1_700_000_000_000L + r.nextLong(100_000_000_000L));
            } else {
                throw new IllegalStateException("no generator for " + t);
            }
            f.set(o, v);
        }
        return o;
    }

    /** Fields that are plain values (not children, lists or maps). */
    private static List<Field> scalarFields(Class<?> c) {
        return java.util.Arrays.stream(c.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .filter(f -> !Collection.class.isAssignableFrom(f.getType()) && !Map.class.isAssignableFrom(f.getType()))
                .filter(f -> f.getType().isPrimitive() || f.getType().getName().startsWith("java."))
                .toList();
    }

    private String text(int len) {
        char[] c = new char[len];
        for (int i = 0; i < len; i++) {
            c[i] = ALNUM.charAt(r.nextInt(ALNUM.length()));
        }
        return new String(c);
    }

    private String pick(String[] a) {
        return a[r.nextInt(a.length)];
    }

    private static long gzip(byte[] b) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gz = new GZIPOutputStream(out)) {
            gz.write(b);
        }
        return out.size();
    }

    private static String human(long bytes) {
        if (bytes >= 1L << 40) {
            return String.format("%.2f TiB", bytes / (double) (1L << 40));
        }
        if (bytes >= 1L << 30) {
            return String.format("%.1f GiB", bytes / (double) (1L << 30));
        }
        return String.format("%.1f MiB", bytes / (double) (1L << 20));
    }

    private static void section(String title) {
        System.out.println();
        System.out.println("== " + title);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
