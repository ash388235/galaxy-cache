package io.galaxycache.grid.client;

import io.galaxycache.grid.api.ClusterMonitor;
import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridSettings;
import io.galaxycache.grid.api.GridTopology;
import io.galaxycache.grid.api.GridUnavailableException;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One shared, reference-counted grid connection set per grid storage name in a JVM, with its fail-stop monitor.
 * Opening waits (up to {@code startupWaitMs}) until every node is READY with the same topology; afterwards
 * the first sign of trouble (node unreachable, restarted, failed, or a broken connection) fails the grid for this
 * process: every call throws {@link GridUnavailableException}, and with {@code onFailure: exit} the JVM exits.
 */
final class GridClients {

    private static final System.Logger LOG = System.getLogger(GridClients.class.getName());

    /** Exit status a client process uses when the grid fails and {@code onFailure: exit} is set. */
    static final int EXIT_STATUS = 75;

    private record Entry(GridClient data, GridClient monitorClient, ClusterMonitor monitor, int refs) {
    }

    private static final Map<String, Entry> CLIENTS = new HashMap<>();

    private GridClients() {
    }

    static synchronized GridClient acquire(String name, Map<String, Object> raw) {
        Entry e = CLIENTS.get(name);
        if (e == null || e.data().failure() != null) {
            if (e != null) {
                throw new GridUnavailableException(e.data().failure() + " (restart this process after the grid is back)");
            }
            e = open(name, GridSettings.from(raw));
        }
        CLIENTS.put(name, new Entry(e.data(), e.monitorClient(), e.monitor(), e.refs() + 1));
        return e.data();
    }

    private static Entry open(String name, GridSettings s) {
        GridTopology topo = s.topology();
        AtomicReference<ClusterMonitor> monitor = new AtomicReference<>();
        GridClient data = new GridClient(topo, s.connectionsPerNode(), s.timeoutMs(), reason -> {
            ClusterMonitor m = monitor.get();
            if (m != null) {
                m.fail(reason);
            }
        }, s.security());
        GridClient monClient = new GridClient(topo, 1, (int) Math.max(2_000, s.heartbeatMs() * 2), null, s.security());
        ClusterMonitor m = new ClusterMonitor(monClient, topo.members(), topo.fingerprint(), s.heartbeatMs(),
                s.failAfterMs(), true, reason -> {
            data.fail(reason);
            monClient.close();
            if (s.exitOnFailure()) {
                System.err.println("[galaxy] grid '" + name + "' failed: " + reason + " -> exiting with status " + EXIT_STATUS
                        + " (onFailure: exit)");
                new Thread(() -> System.exit(EXIT_STATUS), "galaxy-grid-exit").start();
            }
        });
        monitor.set(m);
        try {
            m.awaitFormed(s.startupWaitMs());
        } catch (RuntimeException ex) {
            m.close();
            monClient.close();
            data.close();
            throw new GridUnavailableException(ex.getMessage());
        }
        m.start();
        LOG.log(System.Logger.Level.INFO, "grid {0} ready: {1}", name, topo);
        return new Entry(data, monClient, m, 0);
    }

    static synchronized void release(String name, GridClient client) {
        Entry e = CLIENTS.get(name);
        if (e == null || e.data() != client) {
            return;
        }
        if (e.refs() <= 1) {
            CLIENTS.remove(name);
            e.monitor().close();
            e.monitorClient().close();
            client.close();
        } else {
            CLIENTS.put(name, new Entry(client, e.monitorClient(), e.monitor(), e.refs() - 1));
        }
    }
}
