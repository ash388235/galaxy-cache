package io.galaxycache.grid.client;

import io.galaxycache.client.GalaxyCache;
import io.galaxycache.client.GalaxyClient;
import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridProtocol;
import io.galaxycache.grid.api.GridSecurity;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.grid.node.GridNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.galaxycache.common.query.Filters.*;
import static org.junit.jupiter.api.Assertions.*;

class GridSecurityTest {

    static final String TOKEN = "s3cret-grid-token-0123456789";

    @TempDir
    Path tmp;
    final List<GridNode> nodes = new ArrayList<>();
    GalaxyConfig config;

    @AfterEach
    void stop() {
        nodes.forEach(GridNode::close);
    }

    private void start(String extra) throws IOException {
        int[] p = {GridEndToEndTest.freePort(), GridEndToEndTest.freePort(), GridEndToEndTest.freePort()};
        String yaml = """
                cluster: grid-sec
                catalog: { path: catalog }
                storages:
                  grid-1:
                    type: grid
                    path: grid
                    fsync: false
                    heartbeatMs: 200
                    failAfterMs: 1000
                    startupWaitMs: 10000
                    authToken: "%s"
                %s
                    nodes: { node-1: "localhost:%d", node-2: "localhost:%d", node-3: "localhost:%d" }
                indexNodes:
                  grid-1: { url: "http://localhost:1" }
                namespaces:
                  orders: { storage: grid-1, indexNode: grid-1 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(TOKEN, extra, p[0], p[1], p[2]);
        Path file = tmp.resolve("galaxy.yaml");
        Files.writeString(file, yaml);
        config = GalaxyConfig.load(file);
        GridSettings s = GridSettings.from(config.storage("grid-1").settings());
        for (GridTopology.Member m : s.topology().members()) {
            nodes.add(new GridNode(m.name(), s, tmp.resolve("grid").resolve(m.name()), -1, r -> { }).start());
        }
    }

    private void roundTrip() {
        try (GalaxyClient client = new GalaxyClient(config)) {
            GalaxyCache<GridEndToEndTest.Order> orders = client.cache(DatasetId.of("orders", "eq", "us", "2026-11-08"),
                    GridEndToEndTest.Order.class);
            List<GridEndToEndTest.Order> list = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                list.add(new GridEndToEndTest.Order(i));
            }
            orders.putAll(list);
            assertEquals(125, orders.count(eq("symbol", "NVDA")));
            assertEquals(42, orders.get("ORD-000042").orElseThrow().quantity);
        }
    }

    private GalaxyException connectWith(Map<String, Object> settingsChange) {
        Map<String, Object> s = new LinkedHashMap<>(config.storage("grid-1").settings());
        s.putAll(settingsChange);
        s.values().removeIf(v -> v == null);
        GridSettings gs = GridSettings.from(s);
        try (GridClient c = new GridClient(gs.topology(), 1, 5_000, null, gs.security())) {
            return assertThrows(GalaxyException.class, () -> c.call(gs.topology().members().get(0), GridProtocol.PING, new byte[0]));
        }
    }

    @Test
    void tokenAuthAcceptsTheRightTokenOnly() throws IOException {
        start("");
        roundTrip();
        GalaxyException wrong = connectWith(Map.of("authToken", "not-the-right-token-xx"));
        assertTrue(wrong.getMessage().contains("authentication failed"), wrong.getMessage());
        Map<String, Object> none = new LinkedHashMap<>();
        none.put("authToken", null);
        GalaxyException missing = connectWith(none);
        assertTrue(missing.getMessage().contains("requires authentication"), missing.getMessage());
        assertThrows(GalaxyException.class, () -> GridSecurity.from(Map.of("authToken", "short")));
    }

    @Test
    void nodesListenOnlyOnTheirOwnAddress() throws IOException {
        start("");
        for (GridNode n : nodes) {
            assertEquals("127.0.0.1", n.boundAddress(), n.name());
        }
    }

    @Test
    void tlsEncryptsTrafficAndRejectsPlainOrUntrustedClients() throws Exception {
        Path ks = tmp.resolve("grid.p12");
        keytool("-genkeypair", "-alias", "grid", "-keyalg", "RSA", "-keysize", "2048", "-dname", "CN=localhost",
                "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-validity", "2", "-storetype", "PKCS12",
                "-keystore", ks.toString(), "-storepass", "changeit", "-keypass", "changeit");
        String ksPath = ks.toString().replace("\\", "/");
        start(String.join("\n",
                "    tls: true",
                "    tlsKeyStore: \"" + ksPath + "\"",
                "    tlsKeyStorePassword: changeit",
                "    tlsTrustStore: \"" + ksPath + "\"",
                "    tlsTrustStorePassword: changeit"));
        roundTrip();

        Map<String, Object> plain = new LinkedHashMap<>();
        plain.put("tls", "false");
        assertNotNull(connectWith(plain)); // a plain-TCP client cannot talk to a TLS node

        Path other = tmp.resolve("other.p12");
        keytool("-genkeypair", "-alias", "x", "-keyalg", "RSA", "-keysize", "2048", "-dname", "CN=localhost",
                "-validity", "2", "-storetype", "PKCS12", "-keystore", other.toString(), "-storepass", "changeit",
                "-keypass", "changeit");
        Map<String, Object> untrusted = new LinkedHashMap<>();
        untrusted.put("tlsTrustStore", other.toString().replace("\\", "/"));
        untrusted.put("tlsKeyStore", null);
        assertNotNull(connectWith(untrusted)); // client does not trust the nodes' certificate
    }

    private static void keytool(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        cmd.addAll(List.of(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes());
        assertEquals(0, p.waitFor(), out);
    }
}
