package io.galaxycache.ignite;

import io.galaxycache.client.spi.CacheProvider;
import io.galaxycache.client.spi.ProviderSession;
import io.galaxycache.config.GalaxyConfig;

/** {@code provider: ignite}: the cache API on an Apache Ignite 2.x cluster, through the thin client. */
public final class IgniteCacheProvider implements CacheProvider {

    public static final String NAME = "ignite";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public ProviderSession open(GalaxyConfig config) {
        return new IgniteProviderSession(config, IgniteSettings.from(config));
    }
}
