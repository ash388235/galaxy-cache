package io.galaxycache.ignite.node;

import io.galaxycache.client.GalaxyClient;
import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.ignite.IgniteSettings;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Ignite servers in the test JVM, configured through galaxy.yaml exactly as IgniteNodeMain does. */
final class IgniteTestCluster implements AutoCloseable {

    final Path dir;
    final GalaxyConfig config;
    final IgniteSettings settings;
    final List<IgniteNode> nodes = new ArrayList<>();
    /** Fail-stop reasons per node name. */
    final Map<String, String> failStops = new ConcurrentHashMap<>();

    /** @param extra extra lines for the {@code ignite:} section (2-space indented keys) */
    IgniteTestCluster(int servers, String extra) throws IOException {
        dir = Files.createTempDirectory("ignite-test");
        StringBuilder sv = new StringBuilder();
        for (int i = 1; i <= servers; i++) {
            sv.append("    node-").append(i).append(": { host: 127.0.0.1, discoveryPort: ").append(freePort())
                    .append(", communicationPort: ").append(freePort()).append(", clientPort: ").append(freePort()).append(" }\n");
        }
        Path file = dir.resolve("galaxy.yaml");
        Files.writeString(file, """
                cluster: ignite-test
                provider: ignite
                ignite:
                  workDir: work
                  persistence: false
                  dataRegionMb: 128
                  heartbeatMs: 200
                  failAfterMs: 3000
                  startupWaitMs: 30000
                  onFailure: throw
                  servers:
                %s%s
                namespaces:
                  contract: {}
                  trading: {}
                client: { readParallelism: 4, readBatchSize: 16 }
                logging: { queryLogDir: logs, systemLogger: false }
                """.formatted(sv, extra == null ? "" : extra));
        config = GalaxyConfig.load(file);
        settings = IgniteSettings.from(config);
    }

    IgniteTestCluster start() {
        for (String name : settings.servers().keySet()) {
            nodes.add(startNode(name));
        }
        nodes.forEach(n -> n.awaitFormed(60_000));
        return this;
    }

    IgniteNode startNode(String name) {
        IgniteNode n = new IgniteNode(name, settings, Path.of(settings.workDir()), reason -> {
            failStops.put(name, reason);
            nodes.stream().filter(x -> x.name().equals(name)).forEach(IgniteNode::close);
        });
        return n.start();
    }

    IgniteNode node(String name) {
        return nodes.stream().filter(n -> n.name().equals(name)).findFirst().orElseThrow();
    }

    GalaxyClient client() {
        return new GalaxyClient(config);
    }

    static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    @Override
    public void close() {
        nodes.forEach(IgniteNode::close);
    }
}
