package io.galaxycache.ignite.node;

import io.galaxycache.config.GalaxyConfig;
import io.galaxycache.ignite.IgniteSettings;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Starts one Ignite server node of a {@code provider: ignite} cluster.
 * <pre>
 * java $IGNITE_OPENS -jar galaxy-ignite-node.jar --config config/galaxy-ignite.yaml --node node-1
 * </pre>
 * Data goes to {@code ignite.workDir} (default: {@code ./ignite-work}). Under {@code failureMode: failstop} the
 * process exits with status 75 when any server of the formed cluster leaves; restart all servers together.
 */
public final class IgniteNodeMain {

    private IgniteNodeMain() {
    }

    public static void main(String[] args) {
        String config = "config/galaxy-ignite.yaml";
        String node = null;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--config" -> config = args[++i];
                case "--node" -> node = args[++i];
                default -> {
                    System.err.println("usage: IgniteNodeMain --config <galaxy.yaml> --node <name>");
                    System.exit(2);
                }
            }
        }
        if (node == null) {
            System.err.println("usage: IgniteNodeMain --config <galaxy.yaml> --node <name>");
            System.exit(2);
        }
        GalaxyConfig cfg = GalaxyConfig.load(Path.of(config));
        IgniteSettings settings = IgniteSettings.from(cfg);
        Path workDir = Path.of(settings.workDir() != null ? settings.workDir() : "ignite-work");
        AtomicReference<IgniteNode> ref = new AtomicReference<>();
        String name = node;
        IgniteNode n = new IgniteNode(node, settings, workDir, reason -> {
            System.err.println("[galaxy] ignite node " + name + " FAIL-STOP: " + reason + " -> stopping, exit status "
                    + IgniteSettings.EXIT_STATUS);
            IgniteNode self = ref.get();
            if (self != null) {
                self.close();
            }
            System.exit(IgniteSettings.EXIT_STATUS);
        });
        ref.set(n);
        Runtime.getRuntime().addShutdownHook(new Thread(n::close, "galaxy-ignite-shutdown"));
        n.start();
        System.out.println("[galaxy] ignite node " + node + " started; waiting for " + settings.expectedNodes()
                + " servers (" + settings + ")");
    }
}
