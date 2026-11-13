package io.galaxycache.ignite;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.config.GalaxyConfig;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The {@code ignite:} section of {@code galaxy.yaml}, shared by clients ({@code provider: ignite}) and server nodes
 * ({@code IgniteNodeMain}). Values may use {@code ${env:NAME}}; system properties {@code galaxy.ignite.<key>}
 * override any value for one process.
 *
 * <pre>
 * servers          name -> {host, discoveryPort (47500), communicationPort (47100), clientPort (10800)} or "host"
 * addresses        thin-client addresses (default: every server's host:clientPort)
 * expectedNodes    servers that must be up before the cluster is used / first activated (default: all)
 * partitions       1024    backups 0    atomicity ATOMIC | TRANSACTIONAL
 * failureMode      failstop (any server lost: all servers stop, clients fail) | failover (needs backups >= 1)
 * onFailure        throw | exit (client exits with status 75 when the cluster fails)
 * persistence      true    workDir   dataRegionMb 1024
 * heartbeatMs 1000  failAfterMs 5000  startupWaitMs 60000  timeoutMs 30000
 * username / password (Ignite authentication; needs persistence), tls + tlsKeyStore/tlsTrustStore (+Password)
 * </pre>
 */
public record IgniteSettings(String clusterName, Map<String, Server> servers, List<String> addresses,
                             int expectedNodes, int partitions, int backups, String atomicity, String failureMode,
                             String onFailure, boolean persistence, String workDir, long dataRegionMb,
                             long heartbeatMs, long failAfterMs, long startupWaitMs, int timeoutMs,
                             String username, String password, boolean tls, String tlsKeyStore,
                             String tlsKeyStorePassword, String tlsTrustStore, String tlsTrustStorePassword) {

    public static final String FAILSTOP = "failstop";
    public static final String FAILOVER = "failover";
    /** Exit status of a client process when the cluster fails and {@code onFailure: exit} is set (same as the grid). */
    public static final int EXIT_STATUS = 75;

    private static final Pattern ENV = Pattern.compile("\\$\\{env:([A-Za-z0-9_]+)}");

    /** One server node and its ports. */
    public record Server(String name, String host, int discoveryPort, int communicationPort, int clientPort) {
    }

    public static IgniteSettings from(GalaxyConfig config) {
        if (config.ignite().isEmpty()) {
            throw new GalaxyException("provider: ignite needs an 'ignite:' section in the configuration");
        }
        return from(config.cluster(), config.ignite());
    }

    public static IgniteSettings from(String clusterName, Map<String, Object> s) {
        Object rawServers = s.get("servers");
        if (!(rawServers instanceof Map<?, ?> sm) || sm.isEmpty()) {
            throw new GalaxyException("ignite.servers must map node names to hosts, e.g. { node-1: { host: localhost } }");
        }
        Map<String, Server> servers = new TreeMap<>();
        sm.forEach((k, v) -> {
            String name = k.toString();
            if (!name.matches("[A-Za-z0-9][A-Za-z0-9_.\\-]{0,63}")) {
                throw new GalaxyException("ignite server name '" + name + "' must be letters, digits, '.', '_' or '-'");
            }
            if (v instanceof Map<?, ?> m) {
                @SuppressWarnings("unchecked")
                Map<String, Object> mm = (Map<String, Object>) m;
                servers.put(name, new Server(name, str(mm, "host", "localhost"), (int) num(mm, "discoveryPort", 47500),
                        (int) num(mm, "communicationPort", 47100), (int) num(mm, "clientPort", 10800)));
            } else {
                servers.put(name, new Server(name, v.toString(), 47500, 47100, 10800));
            }
        });
        List<String> addresses = new ArrayList<>();
        if (s.get("addresses") instanceof List<?> l && !l.isEmpty()) {
            l.forEach(a -> addresses.add(a.toString()));
        } else {
            servers.values().forEach(sv -> addresses.add(sv.host() + ":" + sv.clientPort()));
        }
        String failureMode = str(s, "failureMode", FAILSTOP).toLowerCase(java.util.Locale.ROOT);
        int backups = (int) num(s, "backups", 0);
        if (!FAILSTOP.equals(failureMode) && !FAILOVER.equals(failureMode)) {
            throw new GalaxyException("ignite.failureMode must be failstop or failover, got '" + failureMode + "'");
        }
        if (FAILOVER.equals(failureMode) && backups < 1) {
            throw new GalaxyException("ignite.failureMode: failover needs backups >= 1 (a lost node would lose data)");
        }
        String atomicity = str(s, "atomicity", "ATOMIC").toUpperCase(java.util.Locale.ROOT);
        if (!atomicity.equals("ATOMIC") && !atomicity.equals("TRANSACTIONAL")) {
            throw new GalaxyException("ignite.atomicity must be ATOMIC or TRANSACTIONAL, got '" + atomicity + "'");
        }
        int partitions = (int) num(s, "partitions", 1024);
        if (partitions < 1 || partitions > 65_000) {
            throw new GalaxyException("ignite.partitions must be 1..65000");
        }
        return new IgniteSettings(clusterName, servers, List.copyOf(addresses),
                (int) num(s, "expectedNodes", servers.size()), partitions, backups, atomicity, failureMode,
                str(s, "onFailure", "throw"), Boolean.parseBoolean(str(s, "persistence", "true")),
                str(s, "workDir", null), num(s, "dataRegionMb", 1024), num(s, "heartbeatMs", 1_000),
                num(s, "failAfterMs", 5_000), num(s, "startupWaitMs", 60_000), (int) num(s, "timeoutMs", 30_000),
                str(s, "username", null), str(s, "password", null), Boolean.parseBoolean(str(s, "tls", "false")),
                str(s, "tlsKeyStore", null), str(s, "tlsKeyStorePassword", null),
                str(s, "tlsTrustStore", null), str(s, "tlsTrustStorePassword", null));
    }

    public Server server(String name) {
        Server sv = servers.get(name);
        if (sv == null) {
            throw new GalaxyException("unknown ignite server '" + name + "'; known: " + servers.keySet());
        }
        return sv;
    }

    public boolean failStop() {
        return FAILSTOP.equals(failureMode);
    }

    public boolean exitOnFailure() {
        return "exit".equalsIgnoreCase(onFailure);
    }

    public boolean transactional() {
        return "TRANSACTIONAL".equals(atomicity);
    }

    /**
     * Identifies the data layout: servers, partitions, backups, atomicity and failure mode. Servers store it at
     * first activation; nodes and clients with another value refuse to work with the cluster.
     */
    public String fingerprint() {
        StringBuilder sb = new StringBuilder("ignite-v1|").append(partitions).append('|').append(backups).append('|')
                .append(atomicity).append('|').append(failureMode).append('|');
        servers.values().forEach(sv -> sb.append(sv.name()).append('@').append(sv.host()).append(';'));
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return Long.toHexString(crc.getValue()) + "-" + servers.size() + "n-" + partitions + "p-" + backups + "b";
    }

    /** Never prints passwords. */
    @Override
    public String toString() {
        return "IgniteSettings[servers=" + servers.keySet() + ", partitions=" + partitions + ", backups=" + backups
                + ", " + atomicity + ", " + failureMode + ", persistence=" + persistence + ", tls=" + tls
                + ", auth=" + (username != null) + "]";
    }

    static String str(Map<String, Object> s, String key, String def) {
        String override = System.getProperty("galaxy.ignite." + key);
        Object v = override != null ? override : s.get(key);
        if (v == null) {
            return def;
        }
        Matcher m = ENV.matcher(v.toString());
        StringBuilder sb = new StringBuilder();
        while (m.find()) {
            String env = System.getenv(m.group(1));
            if (env == null) {
                throw new GalaxyException("ignite setting '" + key + "' needs environment variable " + m.group(1));
            }
            m.appendReplacement(sb, Matcher.quoteReplacement(env));
        }
        m.appendTail(sb);
        return sb.toString();
    }

    static long num(Map<String, Object> s, String key, long def) {
        String v = str(s, key, null);
        try {
            return v == null ? def : Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new GalaxyException("ignite setting '" + key + "' must be a number, got '" + v + "'");
        }
    }

    /** Unmodifiable view of a raw map, for tests. */
    static Map<String, Object> map(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put(kv[i].toString(), kv[i + 1]);
        }
        return m;
    }
}
