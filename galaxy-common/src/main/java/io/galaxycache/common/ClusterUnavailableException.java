package io.galaxycache.common;

/**
 * The cluster behind the cache (the galaxy grid or an Ignite cluster) has failed or is not ready. Under the
 * fail-stop model nothing is retried: stop the processes that use it, restart the cluster, then restart them.
 */
public class ClusterUnavailableException extends GalaxyException {

    public ClusterUnavailableException(String message) {
        super(message);
    }
}
