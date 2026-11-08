package io.galaxycache.index.api;

import io.galaxycache.common.GalaxyException;

import java.util.Map;
import java.util.ServiceLoader;
import java.util.TreeSet;

/** Factory for an {@link IndexPersistence} type, discovered with {@link ServiceLoader}. */
public interface IndexPersistenceProvider {

    String type();

    IndexPersistence create(String nodeName, Map<String, Object> settings);

    static IndexPersistence open(String type, String nodeName, Map<String, Object> settings) {
        TreeSet<String> available = new TreeSet<>();
        for (IndexPersistenceProvider p : ServiceLoader.load(IndexPersistenceProvider.class)) {
            if (p.type().equalsIgnoreCase(type)) {
                return p.create(nodeName, settings);
            }
            available.add(p.type());
        }
        throw new GalaxyException("No index persistence '" + type + "' on the classpath. Available: " + available);
    }
}
