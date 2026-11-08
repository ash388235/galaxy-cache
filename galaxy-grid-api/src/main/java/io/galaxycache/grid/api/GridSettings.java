package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Settings of one grid, read from its {@code type: grid} storage entry. Nodes and clients read the same entry,
 * so they agree on topology and protocol options. Values may reference environment variables as
 * {@code ${env:NAME}}; JVM system properties {@code galaxy.grid.<key>} override any value for one process.
 *
 * <pre>
 * nodes                node name -> host:port (map, or "n1=h:p,n2=h:p")
 * partitions           1024
 * path                 node data folder (each node uses path/&lt;node&gt;)            nodes only
 * connectionsPerNode   4                                                          clients
 * timeoutMs            120000   per request
 * heartbeatMs          1000     fail-stop heartbeat interval
 * failAfterMs          5000     a member unreachable this long fails the whole grid
 * startupWaitMs        60000    how long a client waits for all nodes to be READY
 * onFailure            throw | exit   what a client does when the grid fails      clients
 * maxBatchBytes        16777216 records per write request are split at this size
 * </pre>
 */
public record GridSettings(GridTopology topology, int connectionsPerNode, int timeoutMs, long heartbeatMs,
                           long failAfterMs, long startupWaitMs, String onFailure, int maxBatchBytes,
                           Map<String, Object> raw) {

    private static final Pattern ENV = Pattern.compile("\\$\\{env:([A-Za-z0-9_]+)}");

    public static GridSettings from(Map<String, Object> settings) {
        return new GridSettings(GridTopology.fromSettings(settings),
                (int) num(settings, "connectionsPerNode", 4),
                (int) num(settings, "timeoutMs", 120_000),
                num(settings, "heartbeatMs", 1_000),
                num(settings, "failAfterMs", 5_000),
                num(settings, "startupWaitMs", 60_000),
                str(settings, "onFailure", "throw"),
                (int) num(settings, "maxBatchBytes", 16L * 1024 * 1024),
                settings);
    }

    public boolean exitOnFailure() {
        return "exit".equalsIgnoreCase(onFailure);
    }

    /** A setting as a string, after {@code ${env:...}} substitution and system-property override. */
    public static String str(Map<String, Object> s, String key, String def) {
        String override = System.getProperty("galaxy.grid." + key);
        Object v = override != null ? override : s.get(key);
        if (v == null) {
            return def;
        }
        Matcher m = ENV.matcher(v.toString());
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String env = System.getenv(m.group(1));
            if (env == null) {
                throw new GalaxyException("grid setting '" + key + "' needs environment variable " + m.group(1));
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(env));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public static long num(Map<String, Object> s, String key, long def) {
        String v = str(s, key, null);
        try {
            return v == null ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new GalaxyException("grid setting '" + key + "' must be a number, got '" + v + "'");
        }
    }
}
