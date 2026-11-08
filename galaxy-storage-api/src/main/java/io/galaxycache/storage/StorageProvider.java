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
}
