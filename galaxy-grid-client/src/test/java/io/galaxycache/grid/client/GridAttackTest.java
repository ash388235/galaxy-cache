package io.galaxycache.grid.client;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.Filters;
import io.galaxycache.common.query.Query;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.GridConnection;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridSecurity;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.grid.node.GridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Attacks against real nodes over real sockets on localhost. Every test ends by checking the grid is still healthy,
 * because under fail-stop a request that crashes one node takes the whole grid down.
 */
@Timeout(120)
class GridAttackTest {

    static final String TOKEN = "attack-test-token-0123456789";
    static final DatasetId DS = DatasetId.of("orders", "attack", "us", "2026-11-08");

    @TempDir
    Path tmp;
    final List<GridNode> nodes = new ArrayList<>();
    final List<String> failStops = new java.util.concurrent.CopyOnWriteArrayList<>();
    GalaxyConfig config;
    GridSettings settings;
    GridTopology.Member target;
    int adminPort;

    @BeforeEach
    void start() throws Exception {
        int[] p = {GridEndToEndTest.freePort(), GridEndToEndTest.freePort(), GridEndToEndTest.freePort()};
        Files.writeString(tmp.resolve("galaxy.yaml"), """
                cluster: attack
                catalog: { path: catalog }
                storages:
                  grid-1:
                    type: grid
                    path: grid
                    fsync: false
                    heartbeatMs: 200
                    failAfterMs: 1500
                    startupWaitMs: 10000
                    handshakeTimeoutMs: 1000
                    maxConnections: 64
                    maxInFlightPerConnection: 8
                    authToken: "%s"
                    nodes: { node-1: "localhost:%d", node-2: "localhost:%d", node-3: "localhost:%d" }
                indexNodes:
                  grid-1: { url: "http://localhost:1" }
                namespaces:
                  orders: { storage: grid-1, indexNode: grid-1 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(TOKEN, p[0], p[1], p[2]));
        config = GalaxyConfig.load(tmp.resolve("galaxy.yaml"));
        settings = GridSettings.from(config.storage("grid-1").settings());
        target = settings.topology().members().get(0);
        adminPort = GridEndToEndTest.freePort();
        startNodes();
    }

    void startNodes() throws Exception {
        for (GridTopology.Member m : settings.topology().members()) {
            nodes.add(new GridNode(m.name(), settings, tmp.resolve("grid").resolve(m.name()),
                    m.equals(target) ? adminPort : -1, r -> failStops.add(m.name() + ": " + r)).start());
        }
        long deadline = System.currentTimeMillis() + 15_000;
        while (nodes.stream().anyMatch(n -> !n.state().equals("READY"))) {
            assertTrue(System.currentTimeMillis() < deadline, "grid did not form");
            Thread.sleep(20);
        }
    }

    @AfterEach
    void stop() {
        nodes.forEach(GridNode::close);
    }

    /** The grid survived: no node stopped, and a normal client can still write and read. */
    void assertGridHealthy() {
        assertEquals(List.of(), failStops, "a node fail-stopped");
        nodes.forEach(n -> assertEquals("READY", n.state(), n.name()));
        try (GalaxyClient client = new GalaxyClient(config)) {
            GalaxyCache<GridEndToEndTest.Order> c = client.cache(DS, GridEndToEndTest.Order.class);
            c.put(new GridEndToEndTest.Order(1));
            assertEquals(1L, c.get("ORD-000001").orElseThrow().quantity);
        }
    }

    // ------------------------------------------------------------------ attacker tooling

    Socket socket() throws IOException {
        Socket s = new Socket(target.host(), target.port());
        s.setSoTimeout(5_000);
        return s;
    }

    static byte[] hmac(String token, String label, byte[] a, byte[] b, String fp) throws Exception {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(token.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        mac.update(label.getBytes(StandardCharsets.UTF_8));
        mac.update(a);
        mac.update(b);
        return mac.doFinal(fp.getBytes(StandardCharsets.UTF_8));
    }

    record Hello(DataInputStream in, DataOutputStream out, byte[] nonce, String fingerprint) {
    }

    Hello hello(Socket s) throws IOException {
        DataOutputStream out = new DataOutputStream(s.getOutputStream());
        DataInputStream in = new DataInputStream(new BufferedInputStream(s.getInputStream()));
        out.writeInt(GridProtocol.MAGIC);
        out.writeInt(GridProtocol.VERSION);
        out.flush();
        assertEquals(GridProtocol.MAGIC, in.readInt());
        assertEquals(GridProtocol.VERSION, in.readInt());
        byte[] nonce = new byte[16];
        in.readFully(nonce);
        String fp = in.readUTF();
        assertEquals(1, in.readByte(), "node should require auth");
        return new Hello(in, out, nonce, fp);
    }

    static int finish(Hello h, byte[] clientNonce, byte[] proof) throws IOException {
        h.out().writeUTF(h.fingerprint());
        h.out().write(clientNonce);
        h.out().writeInt(proof.length);
        h.out().write(proof);
        h.out().flush();
        int status = h.in().readInt();
        h.in().readUTF();
        int n = h.in().readInt();
        h.in().readFully(new byte[n]);
        return status;
    }

    /** A logged-in raw connection, for attacks by someone holding the token. */
    Hello authed(Socket s) throws Exception {
        Hello h = hello(s);
        byte[] clientNonce = new byte[16];
        new Random().nextBytes(clientNonce);
        assertEquals(GridProtocol.HS_OK, finish(h, clientNonce, hmac(TOKEN, "galaxy-client", h.nonce(), clientNonce, h.fingerprint())));
        return h;
    }

    static boolean closedByPeer(Socket s) throws IOException {
        s.setSoTimeout(5_000);
        try {
            while (s.getInputStream().read(new byte[4096]) >= 0) {
                // drain
            }
            return true;
        } catch (SocketTimeoutException e) {
            return false;
        } catch (SocketException e) {
            return true;
        }
    }

    static void frame(Hello h, long id, byte op, byte[] body) throws IOException {
        GridProtocol.writeFrame(h.out(), id, op, body, 0, body.length);
        h.out().flush();
    }

    /** Reads frames until the last one for this request; returns its status. */
    static byte answer(Hello h) throws IOException {
        while (true) {
            GridProtocol.Frame f = GridProtocol.readFrame(h.in());
            if (f.code() != GridProtocol.PART) {
                return f.code();
            }
        }
    }

    // ------------------------------------------------------------------ authentication

    @Test
    void connectingWithoutAProofIsRefused() throws Exception {
        try (Socket s = socket()) {
            assertEquals(GridProtocol.HS_AUTH, finish(hello(s), new byte[16], new byte[0]));
            assertTrue(closedByPeer(s));
        }
        assertGridHealthy();
    }

    @Test
    void wrongTokenIsRefused() throws Exception {
        try (Socket s = socket()) {
            Hello h = hello(s);
            byte[] cn = new byte[16];
            assertEquals(GridProtocol.HS_AUTH, finish(h, cn, hmac("not-the-token-000000000", "galaxy-client", h.nonce(), cn, h.fingerprint())));
            assertTrue(closedByPeer(s));
        }
        assertGridHealthy();
    }

    @Test
    void aCapturedProofCannotBeReplayed() throws Exception {
        byte[] capturedNonce = new byte[16];
        byte[] capturedProof;
        try (Socket s = socket()) {
            Hello h = hello(s);
            capturedProof = hmac(TOKEN, "galaxy-client", h.nonce(), capturedNonce, h.fingerprint());
            assertEquals(GridProtocol.HS_OK, finish(h, capturedNonce, capturedProof));
        }
        try (Socket s = socket()) {
            assertEquals(GridProtocol.HS_AUTH, finish(hello(s), capturedNonce, capturedProof),
                    "a proof from another connection must not work");
        }
        assertGridHealthy();
    }

    @Test
    void theNodeProofCannotBeUsedAsAClientProof() throws Exception {
        try (Socket s = socket()) {
            Hello h = hello(s);
            byte[] cn = new byte[16];
            assertEquals(GridProtocol.HS_AUTH, finish(h, cn, hmac(TOKEN, "galaxy-node", cn, h.nonce(), h.fingerprint())));
        }
    }

    @Test
    void aClientRefusesAFakeNodeThatDoesNotKnowTheToken() throws Exception {
        try (ServerSocket fake = new ServerSocket(0)) {
            Thread t = new Thread(() -> {
                try (Socket s = fake.accept()) {
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream());
                    in.readInt();
                    in.readInt();
                    out.writeInt(GridProtocol.MAGIC);
                    out.writeInt(GridProtocol.VERSION);
                    out.write(new byte[16]);
                    out.writeUTF(settings.topology().fingerprint());
                    out.writeByte(1);
                    out.flush();
                    in.readUTF();
                    in.readFully(new byte[16]);
                    in.readFully(new byte[in.readInt()]);
                    out.writeInt(GridProtocol.HS_OK);
                    out.writeUTF("ok");
                    out.writeInt(32);
                    out.write(new byte[32]);
                    out.flush();
                    Thread.sleep(500);
                } catch (Exception ignored) {
                    // client hung up
                }
            });
            t.start();
            GalaxyException e = assertThrows(GalaxyException.class, () -> new GridConnection(
                    new GridTopology.Member("fake", "localhost", fake.getLocalPort()), settings.topology().fingerprint(),
                    3_000, r -> { }, settings.security()));
            assertTrue(e.getMessage().contains("could not prove"), e.getMessage());
            t.join();
        }
    }

    @Test
    void aClientWithATokenRefusesANodeThatOffersNoAuthentication() throws Exception {
        try (ServerSocket fake = new ServerSocket(0)) {
            Thread t = new Thread(() -> {
                try (Socket s = fake.accept()) {
                    DataInputStream in = new DataInputStream(s.getInputStream());
                    DataOutputStream out = new DataOutputStream(s.getOutputStream());
                    in.readInt();
                    in.readInt();
                    out.writeInt(GridProtocol.MAGIC);
                    out.writeInt(GridProtocol.VERSION);
                    out.write(new byte[16]);
                    out.writeUTF(settings.topology().fingerprint());
                    out.writeByte(0); // "no auth needed": a downgrade attempt
                    out.flush();
                    Thread.sleep(500);
                } catch (Exception ignored) {
                    // client hung up
                }
            });
            t.start();
            GalaxyException e = assertThrows(GalaxyException.class, () -> new GridConnection(
                    new GridTopology.Member("fake", "localhost", fake.getLocalPort()), settings.topology().fingerprint(),
                    3_000, r -> { }, settings.security()));
            assertTrue(e.getMessage().contains("does not use authentication"), e.getMessage());
            t.join();
        }
    }

    // ------------------------------------------------------------------ bad bytes before login

    @Test
    void garbageInsteadOfAHandshakeIsDropped() throws Exception {
        byte[][] attacks = {
                "GET / HTTP/1.1\r\nHost: x\r\n\r\n".getBytes(StandardCharsets.US_ASCII),
                {0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00, 0x01, (byte) 0xFC, 0x03, 0x03}, // a TLS ClientHello
                new byte[8],
                {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        };
        for (byte[] a : attacks) {
            try (Socket s = socket()) {
                s.getOutputStream().write(a);
                s.getOutputStream().flush();
                assertTrue(closedByPeer(s), "not closed");
            }
        }
        assertGridHealthy();
    }

    @Test
    void aHugeProofLengthDoesNotAllocateAndClosesTheConnection() throws Exception {
        try (Socket s = socket()) {
            Hello h = hello(s);
            h.out().writeUTF(h.fingerprint());
            h.out().write(new byte[16]);
            h.out().writeInt(Integer.MAX_VALUE);
            h.out().flush();
            assertTrue(closedByPeer(s));
        }
        assertGridHealthy();
    }

    @Test
    void connectionsThatStallDuringTheHandshakeAreClosed() throws Exception {
        try (Socket silent = socket(); Socket dribble = socket()) {
            long t0 = System.currentTimeMillis();
            Thread sender = new Thread(() -> {
                try {
                    for (int i = 0; i < 40; i++) { // slowloris: one byte at a time
                        dribble.getOutputStream().write(GridProtocol.MAGIC >>> ((3 - i % 4) * 8));
                        dribble.getOutputStream().flush();
                        Thread.sleep(150);
                    }
                } catch (IOException | InterruptedException ignored) {
                    // closed by the node, which is the point
                }
            });
            sender.start();
            assertTrue(closedByPeer(silent), "idle connection left open");
            assertTrue(System.currentTimeMillis() - t0 < 4_000, "took too long");
            sender.join();
        }
        assertGridHealthy();
    }

    @Test
    void connectionFloodIsCappedAndTheGridSurvives() throws Exception {
        List<Socket> flood = new ArrayList<>();
        try {
            for (int i = 0; i < 300; i++) {
                flood.add(new Socket(target.host(), target.port()));
            }
            long deadline = System.currentTimeMillis() + 3_000;
            long rejected = 0;
            while (rejected == 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
                rejected = nodes.get(0).nodeStatsForTest().counters().getOrDefault("node.connectionsRejected", 0L);
            }
            assertTrue(rejected >= 200, "rejected only " + rejected);
        } finally {
            for (Socket s : flood) {
                s.close();
            }
        }
        Thread.sleep(1_500); // handshake timeout frees the slots
        assertGridHealthy();
    }

    // ------------------------------------------------------------------ bad frames after login

    @Test
    void illegalFrameLengthsCloseTheConnection() throws Exception {
        for (int len : new int[]{-1, 0, 8, Integer.MAX_VALUE, Integer.MIN_VALUE, GridProtocol.MAX_FRAME + 10}) {
            try (Socket s = socket()) {
                Hello h = authed(s);
                h.out().writeInt(len);
                h.out().writeLong(1);
                h.out().flush();
                assertTrue(closedByPeer(s), "length " + len);
            }
        }
        assertGridHealthy();
    }

    @Test
    void aFrameThatStopsHalfWayJustEndsTheConnection() throws Exception {
        try (Socket s = socket()) {
            Hello h = authed(s);
            h.out().writeInt(1_000_000);
            h.out().writeLong(1);
            h.out().writeByte(GridProtocol.PING);
            h.out().write(new byte[100]);
            h.out().flush();
        }
        Thread.sleep(200);
        assertGridHealthy();
    }

    @Test
    void claimingBillionsOfItemsIsAnErrorNotACrash() throws Exception {
        for (byte op : new byte[]{GridProtocol.GET_ALL, GridProtocol.DELETE_RECORDS, GridProtocol.PUT_RECORDS}) {
            for (int count : new int[]{Integer.MAX_VALUE - 8, Integer.MAX_VALUE, 1_000_000_000, -1}) {
                try (Socket s = socket()) {
                    Hello h = authed(s);
                    frame(h, 1, op, new GridProtocol.Out().putString(DS.key()).putInt(count).toBytes());
                    assertEquals(GridProtocol.ERROR, answer(h), "op " + op + " count " + count);
                }
            }
        }
        assertGridHealthy();
    }

    private byte[] query(Filter f) {
        return Json.writeBytes(new io.galaxycache.index.api.Messages.QueryRequest(DS, Query.where(f).limit(1)));
    }

    @Test
    void expensiveFiltersAreRefusedBeforeTheyCanExhaustMemory() throws Exception {
        Filter deepNot = Filters.eq("symbol", "x");
        for (int i = 0; i < 450; i++) { // this exact request killed a node before the limits existed
            deepNot = Filters.not(deepNot);
        }
        Filter manyNots = Filters.not(Filters.and(Filters.not(Filters.eq("a", 1)), Filters.not(Filters.eq("b", 1)),
                Filters.not(Filters.eq("c", 1)), Filters.not(Filters.eq("d", 1))));
        List<Filter> wide = new ArrayList<>();
        for (int i = 0; i < 300; i++) {
            wide.add(Filters.eq("x", i));
        }
        Filter tooWide = Filters.or(wide.toArray(new Filter[0]));
        for (Filter f : List.of(deepNot, manyNots, tooWide)) {
            try (Socket s = socket()) {
                Hello h = authed(s);
                frame(h, 1, GridProtocol.INDEX_QUERY, query(f));
                assertEquals(GridProtocol.ERROR, answer(h));
            }
        }
        assertGridHealthy();
    }

    @Test
    void jsonNestedBeyondTheParserLimitIsAnError() throws Exception {
        String deep = "{\"dataset\":\"" + DS.key() + "\",\"query\":{\"filter\":" + "{\"op\":\"NOT\",\"children\":[".repeat(5_000);
        try (Socket s = socket()) {
            Hello h = authed(s);
            frame(h, 1, GridProtocol.INDEX_QUERY, deep.getBytes(StandardCharsets.UTF_8));
            assertEquals(GridProtocol.ERROR, answer(h));
        }
        assertGridHealthy();
    }

    @Test
    void randomFramesNeverHurtTheGrid() throws Exception {
        Random r = new Random(4242);
        try (Socket s = socket()) {
            Hello h = authed(s);
            for (int i = 0; i < 3_000; i++) {
                byte[] body = new byte[r.nextInt(300)];
                r.nextBytes(body);
                if (r.nextBoolean() && body.length > 8) { // plausible lengths reach deeper into the parsers
                    body[0] = 0;
                    body[1] = 0;
                    body[2] = 0;
                    body[3] = (byte) r.nextInt(16);
                }
                int op = 1 + r.nextInt(20);
                if (op == GridProtocol.REBALANCE || op == GridProtocol.COMPACT || op == GridProtocol.DROP_DATASET) {
                    continue;
                }
                frame(h, i, (byte) op, body);
                byte status = answer(h);
                assertTrue(status == GridProtocol.LAST || status == GridProtocol.ERROR);
            }
        }
        assertGridHealthy();
    }

    @Test
    void floodingOneConnectionWithRequestsIsThrottledNotFatal() throws Exception {
        try (Socket s = socket()) {
            Hello h = authed(s);
            int n = 3_000;
            Thread sender = new Thread(() -> {
                try {
                    for (int i = 0; i < n; i++) {
                        frame(h, i, GridProtocol.PING, new byte[0]);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            sender.start();
            for (int i = 0; i < n; i++) {
                assertEquals(GridProtocol.LAST, answer(h));
            }
            sender.join();
        }
        assertGridHealthy();
    }

    // ------------------------------------------------------------------ paths and stored data

    @Test
    void datasetNamesCannotEscapeTheDataFolder() throws Exception {
        String[] evil = {"../../../evil/a/b/c", "..\\..\\evil/a/b/c", "a/b/c/../../..", "/etc/a/b/c", "C:/a/b/c/d", "NUL/a/b/c", "a/b/c/d/"};
        Map<String, Long> before = files();
        try (Socket s = socket()) {
            Hello h = authed(s);
            for (String name : evil) {
                String md = "{\"dataset\":\"" + name.replace("\\", "\\\\") + "\",\"domainName\":\"x\",\"domainClass\":\"x\",\"domainVersion\":1,"
                        + "\"codec\":\"binary\",\"createdAt\":\"2026-11-08T00:00:00Z\"}";
                frame(h, 1, GridProtocol.ENSURE_DATASET, md.getBytes(StandardCharsets.UTF_8));
                assertEquals(GridProtocol.ERROR, answer(h), name);
                frame(h, 2, GridProtocol.DROP_DATASET, new GridProtocol.Out().putString(name).toBytes());
                assertEquals(GridProtocol.ERROR, answer(h), name);
                frame(h, 3, GridProtocol.GET_ALL, new GridProtocol.Out().putString(name).putInt(0).toBytes());
                assertEquals(GridProtocol.ERROR, answer(h), name);
            }
        }
        assertEquals(before.keySet(), files().keySet(), "the attack changed files on disk");
        assertGridHealthy();
    }

    private Map<String, Long> files() throws IOException {
        Map<String, Long> out = new LinkedHashMap<>();
        try (Stream<Path> w = Files.walk(tmp)) {
            w.filter(Files::isRegularFile).filter(p -> !p.toString().contains("logs")).forEach(p -> out.put(tmp.relativize(p).toString(), 0L));
        }
        return out;
    }

    @Test
    void aPoisonedIndexDocumentIsRefusedAndCannotStopARestart() throws Exception {
        String key = keyOwnedByTarget();
        try (Socket s = socket()) {
            Hello h = authed(s);
            String body = "{\"dataset\":\"" + DS.key() + "\",\"docs\":[{\"id\":\"" + key + "\",\"fields\":{\"x\":{\"nested\":[1,2,3]}}}]}";
            frame(h, 1, GridProtocol.INDEX_UPSERT, body.getBytes(StandardCharsets.UTF_8));
            assertEquals(GridProtocol.ERROR, answer(h));
        }
        assertGridHealthy();
        nodes.forEach(GridNode::close);
        nodes.clear();
        failStops.clear();
        startNodes();
        assertGridHealthy();
    }

    private String keyOwnedByTarget() {
        for (int i = 0; ; i++) {
            if (settings.topology().ownerOf("K" + i).equals(target)) {
                return "K" + i;
            }
        }
    }

    // ------------------------------------------------------------------ admin port and secrets

    @Test
    void theAdminPortRevealsNoSecretsOrData() throws Exception {
        String payloadMarker = "SECRET-TRADE-PAYLOAD";
        try (GalaxyClient client = new GalaxyClient(config)) {
            GridEndToEndTest.Order o = new GridEndToEndTest.Order(5);
            o.notes.add(payloadMarker);
            client.cache(DS, GridEndToEndTest.Order.class).put(o);
        }
        HttpClient http = HttpClient.newHttpClient();
        for (String path : List.of("/health", "/metrics", "/v1/stats", "/v1/stats?local")) {
            String body = http.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + adminPort + path)).build(),
                    HttpResponse.BodyHandlers.ofString()).body();
            assertFalse(body.contains(TOKEN), path + " leaks the token");
            assertFalse(body.contains(payloadMarker), path + " leaks object data");
        }
        assertGridHealthy();
    }

    @Test
    void secretsDoNotAppearWhenSettingsAreLogged() {
        assertFalse(settings.toString().contains(TOKEN));
        assertFalse(settings.security().toString().contains(TOKEN));
        assertFalse(config.toString().contains(TOKEN));
        assertFalse(config.storage("grid-1").toString().contains(TOKEN));
    }

    @Test
    void aNodeRefusesToListenOnTheNetworkWithoutAuthentication() {
        Map<String, Object> open = new LinkedHashMap<>(config.storage("grid-1").settings());
        open.remove("authToken");
        open.put("bindAddress", "0.0.0.0");
        GridSettings s = GridSettings.from(open);
        GalaxyException e = assertThrows(GalaxyException.class, () -> new GridNode("node-1", s, tmp.resolve("x"), -1, r -> { }));
        assertTrue(e.getMessage().contains("without authentication"), e.getMessage());
    }

    @Test
    void shortTokensAreRefusedAtStartup() {
        assertThrows(GalaxyException.class, () -> GridSecurity.from(Map.of("authToken", "short")));
    }

    @SuppressWarnings("unused")
    private static void silence(EOFException e) {
    }
}
