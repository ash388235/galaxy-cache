package io.galaxycache.examples.sizing;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.codec.BinaryCodec;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;

import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.galaxycache.common.query.Filters.*;

/**
 * Use-case suite for the 1M grid dataset: every case checks the answer, not just that a call returned. Expected
 * counts come from the index-pod runs on the same objects before they were imported into the grid. Writes go to a
 * separate dataset, which is dropped at the end.
 * <pre>
 * GridUseCases --config galaxy.yaml                  run the suite, print a table
 * GridUseCases --config galaxy.yaml --load 30 --seed 1   mixed read load for 30 s (run several processes at once)
 * </pre>
 */
public final class GridUseCases {

    private static final DatasetId DATA = DatasetId.of("grid", "txn", "perf1m", "2026-11-07");
    private static final DatasetId WRITES = DatasetId.of("grid", "usecase", "write", "2026-11-08");
    private static final int N = 1_000_000;
    private static int failures;

    private GridUseCases() {
    }

    private interface Case {
        String run() throws Exception;
    }

    private static void row(String name, Case c) {
        long t0 = System.nanoTime();
        String result;
        boolean ok = true;
        try {
            result = c.run();
        } catch (Throwable e) {
            ok = false;
            failures++;
            result = e.toString();
        }
        System.out.printf("| %s | %,.1f | %s | %s |%n", name, (System.nanoTime() - t0) / 1e6, ok ? "PASS" : "FAIL", result);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private static String id(int i) {
        return String.format("TX%010d", i);
    }

    public static void main(String[] args) throws Exception {
        String config = "config/galaxy.yaml";
        int load = 0;
        int seed = 1;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> config = args[++i];
                case "--load" -> load = Integer.parseInt(args[++i]);
                case "--seed" -> seed = Integer.parseInt(args[++i]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        System.setProperty("galaxy.grid.onFailure", "throw");
        try (GalaxyClient client = new GalaxyClient(GalaxyConfig.load(Path.of(config)))) {
            GalaxyCache<BaseTransaction> grid = client.cache(DATA, BaseTransaction.class);
            if (load > 0) {
                load(grid, load, seed);
            } else {
                System.out.println("| Use case | ms | Result | Detail |");
                System.out.println("|---|---|---|---|");
                reads(grid);
                queries(grid);
                writes(client);
                System.out.println(failures == 0 ? "ALL PASSED" : failures + " FAILED");
            }
        }
        System.exit(failures == 0 ? 0 : 1);
    }

    private static void reads(GalaxyCache<BaseTransaction> g) {
        row("single get, 5,000 random ids", () -> {
            Random r = new Random(1);
            double[] lat = new double[5_000];
            for (int i = 0; i < lat.length; i++) {
                String id = id(r.nextInt(N));
                long t0 = System.nanoTime();
                BaseTransaction t = g.get(id).orElseThrow();
                lat[i] = (System.nanoTime() - t0) / 1e6;
                check(t.transactionId.equals(id), "wrong object for " + id);
            }
            Arrays.sort(lat);
            return String.format("p50 %.2f ms, p99 %.2f ms", lat[2_500], lat[4_950]);
        });
        row("get of a missing id", () -> {
            check(g.get("TX9999999999").isEmpty(), "found a missing id");
            return "empty, as expected";
        });
        // 20,000 is the most a 4 GB client heap can hold: a decoded object takes about 136 KB
        for (int size : new int[]{100, 1_000, 10_000, 20_000}) {
            row(String.format("getAll %,d ids, key order", size), () -> getAll(g, ordered(300_000, size)));
            row(String.format("getAll %,d ids, random", size), () -> getAll(g, random(size, size)));
        }
        row("getAll with missing ids mixed in", () -> {
            List<String> ids = new ArrayList<>(ordered(10, 100));
            ids.add("TX9999999998");
            check(g.getAll(ids).size() == 100, "wrong size");
            return "100 of 101 returned";
        });
    }

    private static List<String> ordered(int from, int n) {
        List<String> ids = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            ids.add(id(from + i));
        }
        return ids;
    }

    private static List<String> random(int n, long seed) {
        Random r = new Random(seed);
        java.util.Set<String> ids = new java.util.LinkedHashSet<>();
        while (ids.size() < n) {
            ids.add(id(r.nextInt(N)));
        }
        return new ArrayList<>(ids);
    }

    private static String getAll(GalaxyCache<BaseTransaction> g, List<String> ids) {
        long t0 = System.nanoTime();
        Map<String, BaseTransaction> got = g.getAll(ids);
        double s = (System.nanoTime() - t0) / 1e9;
        check(got.size() == ids.size(), got.size() + " of " + ids.size());
        got.forEach((k, v) -> check(k.equals(v.transactionId), "wrong object for " + k));
        return String.format("%,.0f objects/s", ids.size() / s);
    }

    private static void queries(GalaxyCache<BaseTransaction> g) {
        LocalDate d = LocalDate.of(2026, 11, 7);
        Object[][] cases = {
                {"selective: bookId = BOOK-42", eq("bookId", "BOOK-42"), 2_015L},
                {"3 fields: EUR, SETTLED, RATES-NY", and(eq("currency", "EUR"), eq("lifecycleStatus", "SETTLED"), eq("deskCode", "RATES-NY")), 6_507L},
                {"type IRS and one date", and(eq("transactionType", "IRS"), eq("tradeDate", d.minusDays(3))), 5_539L},
                {"IN 3 books", in("bookId", "BOOK-1", "BOOK-2", "BOOK-3"), 5_993L},
                {"date range and USD", and(between("tradeDate", d.minusDays(6), d.minusDays(5)), eq("currency", "USD")), 13_494L},
                {"prefix BOOK-42", prefix("bookId", "BOOK-42"), 22_110L},
                {"broad: REPO or CHF", or(eq("transactionType", "REPO"), eq("currency", "CHF")), 333_268L},
                {"broad: not USD", not(eq("currency", "USD")), 799_945L},
                {"everything", all(), 1_000_000L},
        };
        for (Object[] c : cases) {
            Filter f = (Filter) c[1];
            long expected = (Long) c[2];
            row("count: " + c[0], () -> {
                long n = g.count(f);
                check(n == expected, "got " + n + ", expected " + expected);
                return String.format("%,d matches", n);
            });
            row("ids: " + c[0], () -> {
                List<String> ids = g.queryIds(Query.where(f));
                check(ids.size() == expected, "got " + ids.size());
                for (int i = 1; i < ids.size(); i++) {
                    check(ids.get(i - 1).compareTo(ids.get(i)) < 0, "not sorted at " + i);
                }
                return String.format("%,d ids, sorted", ids.size());
            });
        }
        row("fetch 5,000 objects, every one matches the filter", () -> {
            List<BaseTransaction> list = g.query(Query.where(and(eq("currency", "EUR"), eq("lifecycleStatus", "SETTLED"),
                    eq("deskCode", "RATES-NY"))).limit(5_000));
            check(list.size() == 5_000, "got " + list.size());
            for (BaseTransaction t : list) {
                check(t.currency.equals("EUR") && t.lifecycleStatus.equals("SETTLED") && t.deskCode.equals("RATES-NY"), "wrong object");
            }
            return "5,000 objects verified field by field";
        });
        row("paging through all of BOOK-42, 500 per page", () -> {
            Filter f = eq("bookId", "BOOK-42");
            List<String> full = g.queryIds(Query.where(f));
            List<String> paged = new ArrayList<>();
            int pages = 0;
            while (true) {
                List<String> page = g.queryIds(Query.where(f).offset(paged.size()).limit(500));
                if (page.isEmpty()) {
                    break;
                }
                paged.addAll(page);
                pages++;
            }
            check(paged.equals(full), "pages differ from the full list");
            return pages + " pages equal the full result";
        });
        row("deep page: offset 500,000 of everything", () -> {
            List<String> page = g.queryIds(Query.where(all()).offset(500_000).limit(100));
            check(page.equals(ordered(500_000, 100)), "wrong page: " + page.get(0));
            return "exactly TX0000500000 to TX0000500099";
        });
        row("query with no matches", () -> {
            check(g.count(eq("bookId", "NO-SUCH-BOOK")) == 0 && g.query(eq("bookId", "NO-SUCH-BOOK")).isEmpty(), "found something");
            return "0 matches";
        });
    }

    private static void writes(GalaxyClient client) throws Exception {
        if (client.listDatasets("grid").contains(WRITES)) {
            client.dropDataset(WRITES);
        }
        GalaxyCache<BaseTransaction> c = client.cache(WRITES, BaseTransaction.class);
        BinaryCodec codec = new BinaryCodec();
        SizingRun gen = new SizingRun(77);
        List<BaseTransaction> all = new ArrayList<>();
        for (int i = 0; i < 5_000; i++) {
            BaseTransaction t = gen.build(String.format("WR%08d", i));
            t.lifecycleStatus = "UC_NEW";
            all.add(t);
        }
        row("putAll 5,000 objects (about 145 MB)", () -> {
            c.putAll(all);
            check(c.stats().liveRecords() == 5_000, "stored " + c.stats().liveRecords());
            return "5,000 stored";
        });
        row("read back: every byte equal to what was written", () -> {
            Map<String, BaseTransaction> got = c.getAll(all.stream().map(t -> t.transactionId).toList());
            check(got.size() == 5_000, "got " + got.size());
            for (BaseTransaction t : all) {
                check(Arrays.equals(codec.encode(t), codec.encode(got.get(t.transactionId))), "differs: " + t.transactionId);
            }
            return "5,000 objects identical";
        });
        row("update 1,000, query by the new value", () -> {
            List<BaseTransaction> part = new ArrayList<>(all.subList(0, 1_000));
            part.forEach(t -> t.lifecycleStatus = "UC_UPDATED");
            c.putAll(part);
            check(c.count(eq("lifecycleStatus", "UC_UPDATED")) == 1_000, "new value count");
            check(c.count(eq("lifecycleStatus", "UC_NEW")) == 4_000, "old value count");
            check(c.get(all.get(7).transactionId).orElseThrow().lifecycleStatus.equals("UC_UPDATED"), "not updated");
            check(c.stats().liveRecords() == 5_000, "record count changed");
            return "index and object both show the update";
        });
        row("single put by key, then get", () -> {
            BaseTransaction t = all.get(4_000);
            t.comments = "changed by single put";
            c.put(t);
            check(c.get(t.transactionId).orElseThrow().comments.equals("changed by single put"), "not changed");
            return "read back";
        });
        row("delete 500, verify gone everywhere", () -> {
            List<String> ids = all.subList(0, 500).stream().map(t -> t.transactionId).toList();
            check(c.deleteAll(ids) == 500, "delete count");
            check(c.getAll(ids).isEmpty(), "still readable");
            check(c.count(eq("lifecycleStatus", "UC_UPDATED")) == 500, "index count");
            check(c.stats().liveRecords() == 4_500, "storage count");
            check(c.queryIds(Query.where(all())).size() == 4_500, "id count");
            return "4,500 remain, index and storage agree";
        });
        row("wrong id for a keyed object is refused", () -> {
            BaseTransaction t = all.get(600);
            try {
                c.put("SOME-OTHER-ID", t);
            } catch (io.galaxycache.common.GalaxyException e) {
                check(e.getMessage().contains("@GalaxyKey"), e.getMessage());
                return "refused";
            }
            throw new AssertionError("accepted");
        });
        row("drop the write-test dataset", () -> {
            client.dropDataset(WRITES);
            check(!client.listDatasets("grid").contains(WRITES), "still listed");
            return "gone; 1M dataset untouched";
        });
    }

    /** Mixed verified reads for {@code seconds}; prints one LOAD line. */
    private static void load(GalaxyCache<BaseTransaction> g, int seconds, int seed) {
        Random r = new Random(seed);
        long end = System.currentTimeMillis() + seconds * 1000L;
        long ops = 0;
        long objects = 0;
        long errors = 0;
        List<Double> lat = new ArrayList<>();
        Filter[] filters = {eq("bookId", "BOOK-42"), eq("currency", "CHF"), prefix("bookId", "BOOK-7"), eq("deskCode", "CREDIT")};
        while (System.currentTimeMillis() < end) {
            try {
                int roll = r.nextInt(100);
                if (roll < 60) {
                    String id = id(r.nextInt(N));
                    long t0 = System.nanoTime();
                    check(g.get(id).orElseThrow().transactionId.equals(id), "wrong object");
                    lat.add((System.nanoTime() - t0) / 1e6);
                    objects++;
                } else if (roll < 80) {
                    List<String> ids = random(200, r.nextLong());
                    check(g.getAll(ids).size() == 200, "short getAll");
                    objects += 200;
                } else if (roll < 95) {
                    List<BaseTransaction> got = g.query(Query.where(filters[r.nextInt(filters.length)]).limit(200));
                    check(got.size() == 200, "short query");
                    objects += 200;
                } else {
                    check(g.count(all()) == N, "count");
                }
                ops++;
            } catch (RuntimeException | AssertionError e) {
                errors++;
                failures++;
                System.out.println("ERROR: " + e);
            }
        }
        lat.sort(null);
        System.out.printf("LOAD seed=%d ops=%,d objects=%,d errors=%d get_p50=%.2f ms get_p99=%.2f ms objects/s=%,.0f%n", seed, ops,
                objects, errors, lat.get(lat.size() / 2), lat.get(lat.size() * 99 / 100), objects / (double) seconds);
    }
}
