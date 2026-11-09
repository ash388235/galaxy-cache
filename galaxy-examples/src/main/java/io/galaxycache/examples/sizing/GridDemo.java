package io.galaxycache.examples.sizing;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.domain.GalaxyDomain;
import io.galaxycache.common.domain.GalaxyKey;
import io.galaxycache.common.query.Filters;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Local multi-process demo on the grid: several writer processes load trades, each records that it has finished,
 * and a reader process waits for all of them before checking and reading everything.
 * <pre>
 * GridDemo --config galaxy.yaml --reset
 * GridDemo --config galaxy.yaml --write 1 --writers 2 --total 100000
 * GridDemo --config galaxy.yaml --read --writers 2 --total 100000
 * </pre>
 * scripts\run-grid-demo.ps1 runs the whole thing.
 */
public final class GridDemo {

    /** Written by a writer when it has finished, so readers know the data is complete. */
    @GalaxyDomain(name = "DemoWriterDone", version = 1, codec = "binary")
    public static class WriterDone {
        @GalaxyKey public String writer;
        public long objects;
        public double seconds;
        public String finishedAt;
    }

    private static final DatasetId TRADES = DatasetId.of("grid", "demo", "trades", "latest");
    private static final DatasetId STATUS = DatasetId.of("grid", "demo", "status", "latest");

    private GridDemo() {
    }

    public static void main(String[] args) throws Exception {
        String config = "config/galaxy.yaml";
        int writer = 0;
        int writers = 2;
        int total = 100_000;
        boolean reset = false;
        boolean read = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> config = args[++i];
                case "--write" -> writer = Integer.parseInt(args[++i]);
                case "--writers" -> writers = Integer.parseInt(args[++i]);
                case "--total" -> total = Integer.parseInt(args[++i]);
                case "--reset" -> reset = true;
                case "--read" -> read = true;
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        try (GalaxyClient client = new GalaxyClient(GalaxyConfig.load(Path.of(config)))) {
            if (reset) {
                for (DatasetId ds : List.of(TRADES, STATUS)) {
                    if (client.listDatasets("grid").contains(ds)) {
                        client.dropDataset(ds);
                    }
                }
                System.out.println("previous demo data removed");
            } else if (read) {
                read(client, writers, total);
            } else if (writer > 0) {
                write(client, writer, writers, total);
            } else {
                throw new IllegalArgumentException("use --reset, --write <n> or --read");
            }
        }
    }

    private static String id(int i) {
        return String.format("DEMO%010d", i);
    }

    private static void write(GalaxyClient client, int writer, int writers, int total) throws Exception {
        GalaxyCache<BaseTransaction> trades = client.cache(TRADES, BaseTransaction.class);
        GalaxyCache<WriterDone> status = client.cache(STATUS, WriterDone.class);
        SizingRun generator = new SizingRun(9_000L + writer);
        long t0 = System.nanoTime();
        long nextReport = System.currentTimeMillis() + 5_000;
        int written = 0;
        List<BaseTransaction> batch = new ArrayList<>(250);
        for (int i = writer - 1; i < total; i += writers) { // writer n owns every writers-th id
            batch.add(generator.build(id(i)));
            if (batch.size() == 250) {
                trades.putAll(batch);
                written += batch.size();
                batch.clear();
                if (System.currentTimeMillis() > nextReport) {
                    double s = (System.nanoTime() - t0) / 1e9;
                    System.out.printf("writer-%d: %,d written, %,.0f objects/s%n", writer, written, written / s);
                    nextReport += 5_000;
                }
            }
        }
        if (!batch.isEmpty()) {
            trades.putAll(batch);
            written += batch.size();
        }
        double s = (System.nanoTime() - t0) / 1e9;
        WriterDone done = new WriterDone();
        done.writer = "writer-" + writer;
        done.objects = written;
        done.seconds = s;
        done.finishedAt = Instant.now().toString();
        status.put(done); // written last: seeing this means all of this writer's objects are stored and indexed
        System.out.printf("writer-%d: DONE, %,d objects in %.1f s = %,.0f objects/s%n", writer, written, s, written / s);
    }

    private static void read(GalaxyClient client, int writers, int total) throws Exception {
        GalaxyCache<WriterDone> status = client.cache(STATUS, WriterDone.class);
        long waitStart = System.currentTimeMillis();
        long nextReport = 0;
        List<WriterDone> done;
        while (true) {
            done = new ArrayList<>();
            for (int w = 1; w <= writers; w++) {
                status.get("writer-" + w).ifPresent(done::add);
            }
            if (done.size() == writers) {
                break;
            }
            if (System.currentTimeMillis() > nextReport) {
                System.out.printf("reader: waiting, %d of %d writers finished%n", done.size(), writers);
                nextReport = System.currentTimeMillis() + 5_000;
            }
            Thread.sleep(250);
        }
        System.out.printf("reader: all %d writers finished after %.1f s of waiting; reading%n", writers,
                (System.currentTimeMillis() - waitStart) / 1000.0);

        GalaxyCache<BaseTransaction> trades = client.cache(TRADES, BaseTransaction.class);
        boolean ok = true;
        long reported = done.stream().mapToLong(d -> d.objects).sum();
        long indexed = trades.count(Filters.all());
        long stored = trades.stats().liveRecords();
        ok &= report("writers reported", reported, total);
        ok &= report("objects in the index", indexed, total);
        ok &= report("objects in storage", stored, total);

        long byCurrency = 0;
        for (String c : List.of("USD", "EUR", "GBP", "JPY", "CHF")) {
            byCurrency += trades.count(Filters.eq("currency", c));
        }
        ok &= report("sum of counts by currency", byCurrency, total);

        long t0 = System.nanoTime();
        long read = 0;
        long mismatched = 0;
        for (int from = 0; from < total; from += 5_000) {
            List<String> ids = new ArrayList<>();
            for (int i = from; i < Math.min(total, from + 5_000); i++) {
                ids.add(id(i));
            }
            Map<String, BaseTransaction> got = trades.getAll(ids);
            read += got.size();
            for (Map.Entry<String, BaseTransaction> e : got.entrySet()) {
                if (!e.getKey().equals(e.getValue().transactionId)) {
                    mismatched++;
                }
            }
        }
        double s = (System.nanoTime() - t0) / 1e9;
        ok &= report("objects read back by id", read, total);
        ok &= report("objects with the wrong id", mismatched, 0);
        System.out.printf("reader: read %,d objects in %.1f s = %,.0f objects/s%n", read, s, read / s);

        List<BaseTransaction> eur = trades.query(Query.where(Filters.and(Filters.eq("currency", "EUR"),
                Filters.eq("lifecycleStatus", "SETTLED"))).limit(1_000));
        boolean match = eur.stream().allMatch(t -> t.currency.equals("EUR") && t.lifecycleStatus.equals("SETTLED"));
        System.out.printf("reader: query EUR and SETTLED returned %,d objects, all matching: %s%n", eur.size(), match);
        ok &= match && !eur.isEmpty();
        System.out.println(ok ? "reader: RESULT PASS" : "reader: RESULT FAIL");
        if (!ok) {
            System.exit(1);
        }
    }

    private static boolean report(String what, long actual, long expected) {
        boolean ok = actual == expected;
        System.out.printf("reader: %-28s %,10d %s%n", what, actual, ok ? "ok" : "EXPECTED " + expected);
        return ok;
    }
}
