package io.galaxycache.dashboard;

import io.galaxycache.config.GalaxyConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DashboardSecurityTest {

    @TempDir
    Path tmp;
    DashboardServer server;

    @BeforeEach
    void start() throws IOException {
        Files.writeString(tmp.resolve("g.yaml"), """
                cluster: t
                catalog: { path: catalog }
                storages: { s: { type: file, path: data } }
                indexNodes: { i: { url: "http://localhost:1" } }
                namespaces: { n: { storage: s, indexNode: i } }
                logging: { queryLogDir: logs, systemLogger: false }
                """);
        server = new DashboardServer(GalaxyConfig.load(tmp.resolve("g.yaml")), 0).start();
    }

    @AfterEach
    void stop() {
        server.close();
    }

    /** Raw HTTP: the JDK client will not let a test set the Host header. */
    private List<String> request(String method, String path, String host) throws IOException {
        try (Socket s = new Socket("127.0.0.1", server.port())) {
            OutputStream out = s.getOutputStream();
            out.write((method + " " + path + " HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                    .getBytes(StandardCharsets.US_ASCII));
            out.flush();
            BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            List<String> lines = new ArrayList<>();
            for (String l = r.readLine(); l != null; l = r.readLine()) {
                lines.add(l);
            }
            return lines;
        }
    }

    private static int status(List<String> response) {
        return Integer.parseInt(response.get(0).split(" ")[1]);
    }

    @Test
    void normalRequestsWorkAndCarrySecurityHeaders() throws IOException {
        List<String> r = request("GET", "/", "localhost:" + server.port());
        assertEquals(200, status(r));
        String headers = String.join("\n", r).toLowerCase();
        assertTrue(headers.contains("x-content-type-options: nosniff"));
        assertTrue(headers.contains("x-frame-options: deny"));
        assertTrue(headers.contains("content-security-policy:"));
    }

    @Test
    void anotherHostNameIsRefused_dnsRebinding() throws IOException {
        for (String path : List.of("/", "/api/overview", "/api/datasets", "/api/queries")) {
            assertEquals(403, status(request("GET", path, "evil.example.com")), path);
        }
        assertEquals(403, status(request("GET", "/api/overview", "localhost.evil.example.com:8080")));
    }

    @Test
    void onlyGetIsServed() throws IOException {
        for (String m : List.of("POST", "PUT", "DELETE")) {
            assertEquals(405, status(request(m, "/api/overview", "localhost")), m);
        }
    }

    @Test
    void hostileQueryParametersAreHarmless() throws IOException {
        for (String q : List.of("?namespace=../../etc", "?dataset=%3Cscript%3Ealert(1)%3C/script%3E", "?namespace=%00", "?limit=99999999999")) {
            List<String> r = request("GET", "/api/queries" + q, "localhost");
            assertEquals(200, status(r), q);
            assertFalse(String.join("\n", r).contains("<script>"), "reflected: " + q);
        }
    }

    @Test
    void itDoesNotListenOnOtherAddressesByDefault() throws IOException {
        List<InetAddress> lan = new ArrayList<>();
        for (NetworkInterface n : Collections.list(NetworkInterface.getNetworkInterfaces())) {
            for (InetAddress a : Collections.list(n.getInetAddresses())) {
                if (a instanceof Inet4Address && !a.isLoopbackAddress()) {
                    lan.add(a);
                }
            }
        }
        for (InetAddress a : lan) {
            try (Socket s = new Socket()) {
                assertThrows(IOException.class, () -> s.connect(new InetSocketAddress(a, server.port()), 500), a.toString());
            }
        }
    }
}
