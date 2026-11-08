package io.galaxycache.dashboard;

import io.galaxycache.config.GalaxyConfig;

import java.nio.file.Path;

/** {@code java -jar galaxy-dashboard.jar --config galaxy.yaml [--port 8080]} */
public final class DashboardMain {

    private DashboardMain() {
    }

    public static void main(String[] args) throws InterruptedException {
        String config = "galaxy.yaml";
        int port = 8080;
        for (int i = 0; i + 1 < args.length; i += 2) {
            switch (args[i]) {
                case "--config" -> config = args[i + 1];
                case "--port" -> port = Integer.parseInt(args[i + 1]);
                default -> throw new IllegalArgumentException("unknown option " + args[i]);
            }
        }
        DashboardServer server = new DashboardServer(GalaxyConfig.load(Path.of(config)), port).start();
        System.out.println("Galaxy dashboard on http://localhost:" + server.port());
        Runtime.getRuntime().addShutdownHook(new Thread(server::close));
        Thread.currentThread().join();
    }
}
