package io.galaxycache.grid.client;

import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.StorageProvider;

import java.util.Map;

/**
 * {@code type: grid}. Settings: {@code nodes} (map name -> host:port, or "name=host:port,..."), optional
 * {@code partitions} (default 1024), {@code connectionsPerNode} (default 4), {@code timeoutMs} (default 120000).
 * Nodes additionally read {@code path} and the file-store settings.
 */
public final class GridStorageProvider implements StorageProvider {

    @Override
    public String type() {
        return "grid";
    }

    @Override
    public DataStore open(String name, Map<String, Object> settings) {
        return new GridDataStore(name, GridClients.acquire(name, settings),
                (int) io.galaxycache.grid.api.GridSettings.num(settings, "maxBatchBytes", 16L * 1024 * 1024));
    }

    @Override
    public java.util.Set<String> clusterCapabilities() {
        java.util.Set<String> names = new java.util.TreeSet<>();
        GridProviderCluster.CAPABILITIES.forEach(c -> names.add(c.name()));
        return names;
    }
}
