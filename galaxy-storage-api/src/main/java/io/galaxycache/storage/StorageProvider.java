package io.galaxycache.storage;

import java.util.Map;

/**
 * Factory for one storage type, discovered with {@link java.util.ServiceLoader}.
 * Register an implementation in {@code META-INF/services/io.galaxycache.storage.StorageProvider}.
 */
public interface StorageProvider {

    /** Type used in configuration, e.g. {@code file}, {@code s3}, {@code jdbc}. */
    String type();

    DataStore open(String name, Map<String, Object> settings);

    /**
     * Names of the cluster capabilities ({@code io.galaxycache.client.Capability}) a store of this type offers, known
     * without connecting, so a client can report them before it opens the store. Default: none.
     */
    default java.util.Set<String> clusterCapabilities() {
        return java.util.Set.of();
    }
}
