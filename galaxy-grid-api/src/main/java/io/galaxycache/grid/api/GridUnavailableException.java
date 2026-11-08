package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

/**
 * The grid has failed (a node died, restarted or stopped answering) or is not ready. Under the fail-stop model
 * nothing is retried: stop the processes that use the grid, restart the grid, then restart them.
 */
public class GridUnavailableException extends GalaxyException {

    public GridUnavailableException(String message) {
        super("grid unavailable: " + message);
    }
}
