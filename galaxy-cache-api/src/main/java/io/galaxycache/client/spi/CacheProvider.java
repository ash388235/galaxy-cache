package io.galaxycache.client.spi;

import io.galaxycache.config.GalaxyConfig;

/**
 * One cache implementation ("galaxy", "ignite"), found with {@link java.util.ServiceLoader}. Register it in
 * {@code META-INF/services/io.galaxycache.client.spi.CacheProvider}. {@code provider:} in {@code galaxy.yaml}
 * picks which one a {@code GalaxyClient} uses.
 */
public interface CacheProvider {

    /** Name used in configuration. */
    String name();

    ProviderSession open(GalaxyConfig config);
}
