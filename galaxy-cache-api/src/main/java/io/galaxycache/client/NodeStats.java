package io.galaxycache.client;

import java.util.Map;

/**
 * Size of one node's share of the data.
 *
 * @param records  primary records held by the node
 * @param bytes    bytes used, or -1 when the provider cannot tell cheaply
 * @param counters provider-specific counters (grid: request counters; Ignite: per-cache sizes)
 */
public record NodeStats(String node, long records, long bytes, Map<String, Long> counters) {
    public NodeStats {
        counters = counters == null ? Map.of() : Map.copyOf(counters);
    }
}
