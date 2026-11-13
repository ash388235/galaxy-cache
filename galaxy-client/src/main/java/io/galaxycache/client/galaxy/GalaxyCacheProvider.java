package io.galaxycache.client.galaxy;

import io.galaxycache.client.spi.CacheProvider;
import io.galaxycache.client.spi.ProviderSession;
import io.galaxycache.config.GalaxyConfig;

/** {@code provider: galaxy}: our own storage (file store or grid) plus index pods. */
public final class GalaxyCacheProvider implements CacheProvider {

    public static final String NAME = "galaxy";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ProviderSession open(GalaxyConfig config) {
        return new GalaxyProviderSession(config);
    }
}
