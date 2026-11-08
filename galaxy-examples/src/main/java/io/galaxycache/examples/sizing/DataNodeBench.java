package io.galaxycache.examples.sizing;

import com.sun.net.httpserver.HttpServer;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.codec.BinaryCodec;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.StorageProviders;
import io.galaxycache.storage.StoredRecord;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prototype of the "data node" architecture: one server process owns the storage (local disk + OS page cache)
 * and serves raw binary records to clients over HTTP; clients decode. Compared with the current direct
 * architecture (client reads storage itself) on the same ids, so the difference is the cost of the extra hop.
 * Server and client share this machine and talk over loopback, so real network time is NOT included.
 */
public final class DataNodeBench {

    private static final int THREADS = 8;
    private static final int CHUNK = 512;

    private DataNodeBench() {
    }

    public static void main(String[] args) throws Exception {
        GalaxyConfig config = GalaxyConfig.load(Path.of(args.length > 1 ? args[1] : "config/galaxy.yaml"));
        DatasetId ds = DatasetId.of("sizing", "txn", "perf1m", LocalDate.now());
        var sc = config.storage("local-fs-1");
        BinaryCodec codec = new BinaryCodec();
        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        try (DataStore store = StorageProviders.open(sc.type(), "local-fs-1", sc.settings())) {
            int n = (int) store.stats(ds).liveRecords(); // server-side open: done once, not per client

            // ---- data node: POST /get with newline-separated ids -> [idLen][id][len][payload]...
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 256);
            server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
            server.createContext("/get", ex -> {
                String[] ids = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8).split("\n");
                Map<String, StoredRecord> got = store.getAll(ds, Arrays.asList(ids));
                ByteArrayOutputStream buf = new ByteArrayOutputStream(got.size() * 30_000);
                DataOutputStream out = new DataOutputStream(buf);
                for (StoredRecord r : got.values()) {
                    byte[] id = r.id().getBytes(StandardCharsets.UTF_8);
                    out.writeInt(id.length);
                    out.write(id);
                    out.writeInt(r.payload().length);
                    out.write(r.payload());
                }
                ex.sendResponseHeaders(200, buf.size());
                try (OutputStream os = ex.getResponseBody()) {
                    buf.writeTo(os);
                }
            });
            server.start();
            String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/get";
            HttpClient http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                    .executor(Executors.newFixedThreadPool(THREADS)).build();

            Reader direct = ids -> store.getAll(ds, ids);
            Reader node = ids -> {
                HttpResponse<byte[]> resp = http.send(HttpRequest.newBuilder(URI.create(url))
                                .POST(HttpRequest.BodyPublishers.ofString(String.join("\n", ids))).build(),
                        HttpResponse.BodyHandlers.ofByteArray());
                ByteBuffer b = ByteBuffer.wrap(resp.body());
                Map<String, StoredRecord> m = new HashMap<>();
                while (b.hasRemaining()) {
                    byte[] id = new byte[b.getInt()];
                    b.get(id);
                    byte[] p = new byte[b.getInt()];
                    b.get(p);
                    String s = new String(id, StandardCharsets.UTF_8);
                    m.put(s, new StoredRecord(s, 1, p));
                }
                return m;
            };

            Random r = new Random(21);
            List<String> random = new ArrayList<>();
            for (int i = 0; i < 20_000; i++) {
                random.add(id(r.nextInt(n)));
            }
            List<String> consecutive = new ArrayList<>();
            for (int i = 0; i < 20_000; i++) {
                consecutive.add(id(300_000 + i));
            }
            say("1M dataset, %d client threads, chunks of %d ids, binary payloads decoded on the client", THREADS, CHUNK);
            say("each test runs twice per path; the 2nd run is warm (OS page cache = the data node's RAM cache)");
            System.out.println();
            System.out.printf("   %-34s %-26s %12s %12s %10s%n", "ids", "path", "objects/s", "MB/s", "run");
            for (String[] t : new String[][]{{"20,000 random ids", "r"}, {"20,000 consecutive ids", "c"}}) {
                List<String> ids = t[1].equals("r") ? random : consecutive;
                for (int run = 1; run <= 2; run++) {
                    bulk(t[0], "direct (client reads disk)", direct, ids, codec, pool, run);
                    bulk(t[0], "data node (via server)", node, ids, codec, pool, run);
                }
            }

            System.out.println();
            say("single get(id), one caller, 2,000 random ids, warm:");
            List<String> singles = random.subList(0, 2_000);
            for (Reader rd : List.of(direct, node)) {
                for (String id : singles) { // warm
                    rd.read(List.of(id));
                }
                double[] lat = new double[singles.size()];
                for (int i = 0; i < lat.length; i++) {
                    long t0 = System.nanoTime();
                    StoredRecord rec = rd.read(List.of(singles.get(i))).get(singles.get(i));
                    codec.decode(rec.payload(), 1, BaseTransaction.class);
                    lat[i] = (System.nanoTime() - t0) / 1e6;
                }
                Arrays.sort(lat);
                say("  %-28s p50 %.2f ms  p90 %.2f ms  p99 %.2f ms", rd == direct ? "direct" : "data node (loopback)",
                        lat[lat.length / 2], lat[lat.length * 9 / 10], lat[lat.length * 99 / 100]);
            }
            server.stop(0);
        } finally {
            pool.shutdown();
        }
        System.exit(0);
    }

    private interface Reader {
        Map<String, StoredRecord> read(List<String> ids) throws Exception;
    }

    private static void bulk(String label, String path, Reader reader, List<String> ids, BinaryCodec codec,
                             ExecutorService pool, int run) throws Exception {
        AtomicLong bytes = new AtomicLong();
        AtomicLong objs = new AtomicLong();
        long t0 = System.nanoTime();
        List<Future<?>> fs = new ArrayList<>();
        for (int i = 0; i < ids.size(); i += CHUNK) {
            List<String> part = ids.subList(i, Math.min(ids.size(), i + CHUNK));
            fs.add(pool.submit(() -> {
                for (StoredRecord rec : reader.read(part).values()) {
                    codec.decode(rec.payload(), 1, BaseTransaction.class);
                    bytes.addAndGet(rec.payload().length);
                    objs.incrementAndGet();
                }
                return null;
            }));
        }
        for (Future<?> f : fs) {
            f.get();
        }
        double s = (System.nanoTime() - t0) / 1e9;
        System.out.printf("   %-34s %-26s %,12.0f %,12.0f %10s%n", label, path, objs.get() / s, bytes.get() / s / 1e6,
                run == 1 ? "cold-ish" : "warm");
    }

    private static String id(int i) {
        return String.format("TX%010d", i);
    }

    private static void say(String fmt, Object... args) {
        System.out.println("   " + String.format(fmt, args));
    }
}
