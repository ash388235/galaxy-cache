package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Static grid membership and the key -> partition -> node mapping shared by every node and client.
 *
 * <p>{@code partition = mix(key.hashCode()) mod partitions} ({@link String#hashCode()} is fixed by the Java
 * spec, so every JVM agrees). Each partition is owned by the member with the highest rendezvous score for
 * it, so adding a member later only moves the partitions that member wins.
 */
public final class GridTopology {

    public static final int DEFAULT_PARTITIONS = 1024;

    /** One grid node and where its binary protocol listens. */
    public record Member(String name, String host, int port) {
        @Override
        public String toString() {
            return name + "@" + host + ":" + port;
        }
    }

    private final List<Member> members;
    private final int partitions;
    private final int[] owner;

    public GridTopology(List<Member> members, int partitions) {
        if (members.isEmpty()) {
            throw new GalaxyException("A grid needs at least one node");
        }
        if (partitions < 1) {
            throw new GalaxyException("partitions must be >= 1");
        }
        List<Member> sorted = new ArrayList<>(members);
        sorted.sort(Comparator.comparing(Member::name));
        this.members = List.copyOf(sorted);
        this.partitions = partitions;
        this.owner = new int[partitions];
        for (int p = 0; p < partitions; p++) {
            long best = Long.MIN_VALUE;
            for (int m = 0; m < this.members.size(); m++) {
                long score = mix64(((long) this.members.get(m).name().hashCode() << 32) ^ p);
                if (score > best) {
                    best = score;
                    owner[p] = m;
                }
            }
        }
    }

    /**
     * Reads {@code nodes} and {@code partitions} from storage settings. {@code nodes} is either a map
     * {@code {node-1: "localhost:7101", ...}} or a string {@code "node-1=localhost:7101,node-2=..."}.
     */
    public static GridTopology fromSettings(Map<String, Object> settings) {
        Object nodes = settings.get("nodes");
        if (nodes == null) {
            throw new GalaxyException("grid storage needs 'nodes'");
        }
        Map<String, String> m = new LinkedHashMap<>();
        if (nodes instanceof Map<?, ?> map) {
            map.forEach((k, v) -> m.put(k.toString(), v.toString()));
        } else {
            for (String part : nodes.toString().split(",")) {
                String[] kv = part.trim().split("=", 2);
                if (kv.length != 2) {
                    throw new GalaxyException("grid node entry must be name=host:port, got '" + part + "'");
                }
                m.put(kv[0].trim(), kv[1].trim());
            }
        }
        List<Member> members = new ArrayList<>();
        m.forEach((name, addr) -> {
            int colon = addr.lastIndexOf(':');
            if (colon < 0) {
                throw new GalaxyException("grid node '" + name + "' needs host:port, got '" + addr + "'");
            }
            members.add(new Member(name, addr.substring(0, colon), Integer.parseInt(addr.substring(colon + 1))));
        });
        Object p = settings.get("partitions");
        return new GridTopology(members, p == null ? DEFAULT_PARTITIONS : Integer.parseInt(p.toString()));
    }

    public List<Member> members() {
        return members;
    }

    public int partitions() {
        return partitions;
    }

    public Member member(String name) {
        return members.stream().filter(m -> m.name().equals(name)).findFirst()
                .orElseThrow(() -> new GalaxyException("unknown grid node '" + name + "'; known: " + members));
    }

    public int partition(String key) {
        int h = key.hashCode();
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;
        h ^= h >>> 16;
        return Math.floorMod(h, partitions);
    }

    public Member ownerOfPartition(int partition) {
        return members.get(owner[partition]);
    }

    public Member ownerOf(String key) {
        return members.get(owner[partition(key)]);
    }

    /** Number of partitions each member owns, for logs and the dashboard. */
    public Map<String, Integer> partitionCounts() {
        Map<String, Integer> out = new LinkedHashMap<>();
        members.forEach(m -> out.put(m.name(), 0));
        for (int o : owner) {
            out.merge(members.get(o).name(), 1, Integer::sum);
        }
        return out;
    }

    /** Splits ids by owning member, keeping their order within each group. */
    public Map<Member, List<String>> groupByOwner(Collection<String> ids) {
        Map<Member, List<String>> out = new LinkedHashMap<>();
        for (String id : ids) {
            out.computeIfAbsent(ownerOf(id), k -> new ArrayList<>()).add(id);
        }
        return out;
    }

    /**
     * Identifies this exact topology (members, addresses, partition count, assignment scheme). Nodes and clients
     * refuse to work together unless their fingerprints match, so a stale config can never misroute keys.
     */
    public String fingerprint() {
        StringBuilder sb = new StringBuilder("v1|").append(partitions).append('|');
        members.forEach(m -> sb.append(m.name()).append('@').append(m.host()).append(':').append(m.port()).append(';'));
        java.util.zip.CRC32 crc = new java.util.zip.CRC32();
        crc.update(sb.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return Long.toHexString(crc.getValue()) + "-" + members.size() + "n-" + partitions + "p";
    }

    private static long mix64(long z) {
        z = (z ^ (z >>> 33)) * 0xff51afd7ed558ccdL;
        z = (z ^ (z >>> 33)) * 0xc4ceb9fe1a85ec53L;
        return z ^ (z >>> 33);
    }

    @Override
    public String toString() {
        return members + " x " + partitions + " partitions";
    }
}
