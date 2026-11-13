package io.galaxycache.client.spi;

/**
 * Implemented by a galaxy {@code DataStore} that is a cluster (the grid), so the galaxy provider can offer
 * cluster operations for it.
 */
public interface ClusterSupport {

    ProviderCluster cluster();
}
