package io.galaxycache.grid.client;

import io.galaxycache.grid.api.GridClient;
import io.galaxycache.grid.api.GridTopology;

import java.util.HashMap;
import java.util.Map;

/** One shared, reference-counted {@link GridClient} per grid storage name in a JVM. */
final class GridClients {

    private record Entry(GridClient client, int refs) {
    }

    private static final Map<String, Entry> CLIENTS = new HashMap<>();

    private GridClients() {
    }

    static synchronized GridClient acquire(String name, Map<String, Object> settings) {
        Entry e = CLIENTS.get(name);
        if (e == null) {
            int conns = intSetting(settings, "connectionsPerNode", 4);
            int timeout = intSetting(settings, "timeoutMs", 120_000);
            e = new Entry(new GridClient(GridTopology.fromSettings(settings), conns, timeout), 0);
        }
        CLIENTS.put(name, new Entry(e.client(), e.refs() + 1));
        return e.client();
    }

    static synchronized void release(String name, GridClient client) {
        Entry e = CLIENTS.get(name);
        if (e == null || e.client() != client) {
            return;
        }
        if (e.refs() <= 1) {
            CLIENTS.remove(name);
            client.close();
        } else {
            CLIENTS.put(name, new Entry(client, e.refs() - 1));
        }
    }

    private static int intSetting(Map<String, Object> s, String key, int def) {
        Object v = s.get(key);
        return v == null ? def : Integer.parseInt(v.toString());
    }
}
