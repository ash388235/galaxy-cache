package io.galaxycache.storage;

import io.galaxycache.common.GalaxyException;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeSet;

/** Finds {@link StorageProvider}s on the classpath. */
public final class StorageProviders {

    private StorageProviders() {
    }

    public static StorageProvider find(String type) {
        TreeSet<String> available = new TreeSet<>();
        for (StorageProvider p : ServiceLoader.load(StorageProvider.class)) {
            if (p.type().equalsIgnoreCase(type)) {
                return p;
            }
            available.add(p.type());
        }
        throw new GalaxyException("No storage provider for type '" + type + "' on the classpath. Available: " + available);
    }

    public static DataStore open(String type, String name, Map<String, Object> settings) {
        return find(type).open(name, settings);
    }
}
