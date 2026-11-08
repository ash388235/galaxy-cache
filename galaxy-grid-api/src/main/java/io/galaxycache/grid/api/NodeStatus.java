package io.galaxycache.grid.api;

/**
 * Answer to {@link GridProtocol#PING}. {@code startupId} changes on every node start, so a member that restarted
 * behind the grid's back is detected. {@code state} is FORMING, READY or FAILED.
 */
public record NodeStatus(String node, long startupId, String state, String fingerprint, String reason) {

    public static final String FORMING = "FORMING";
    public static final String READY = "READY";
    public static final String FAILED = "FAILED";
}
