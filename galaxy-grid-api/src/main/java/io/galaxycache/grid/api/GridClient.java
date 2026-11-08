package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connections to every member of one grid ({@code connectionsPerNode} each, used round-robin), plus helpers
 * to call one member or all of them in parallel. Thread-safe; broken connections are reopened on next use.
 */
public final class GridClient implements AutoCloseable {

    private final GridTopology topology;
    private final int connectionsPerNode;
    private final int timeoutMs;
    private final Map<String, GridConnection[]> pools = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> rr = new ConcurrentHashMap<>();

    public GridClient(GridTopology topology, int connectionsPerNode, int timeoutMs) {
        this.topology = topology;
        this.connectionsPerNode = Math.max(1, connectionsPerNode);
        this.timeoutMs = timeoutMs;
    }

    public GridTopology topology() {
        return topology;
    }

    private GridConnection connection(GridTopology.Member m) {
        GridConnection[] pool = pools.computeIfAbsent(m.name(), k -> new GridConnection[connectionsPerNode]);
        int i = Math.floorMod(rr.computeIfAbsent(m.name(), k -> new AtomicInteger()).getAndIncrement(), pool.length);
        GridConnection c = pool[i];
        if (c == null || !c.isOpen()) {
            synchronized (pool) {
                c = pool[i];
                if (c == null || !c.isOpen()) {
                    c = new GridConnection(m, Math.min(timeoutMs, 5_000));
                    pool[i] = c;
                }
            }
        }
        return c;
    }

    /** Starts a call; use {@link #await} to wait for it. */
    public CompletableFuture<List<byte[]>> send(GridTopology.Member m, byte op, byte[] body) {
        return connection(m).call(op, body);
    }

    public List<byte[]> call(GridTopology.Member m, byte op, byte[] body) {
        return await(m, send(m, op, body));
    }

    /** Same request to every member in parallel; results keyed by member, in topology order. */
    public Map<GridTopology.Member, List<byte[]>> callAll(byte op, byte[] body) {
        Map<GridTopology.Member, CompletableFuture<List<byte[]>>> fs = new LinkedHashMap<>();
        for (GridTopology.Member m : topology.members()) {
            fs.put(m, send(m, op, body));
        }
        Map<GridTopology.Member, List<byte[]>> out = new LinkedHashMap<>();
        fs.forEach((m, f) -> out.put(m, await(m, f)));
        return out;
    }

    /** Per-member requests sent in parallel (e.g. ids grouped by owner). */
    public Map<GridTopology.Member, List<byte[]>> callEach(Map<GridTopology.Member, byte[]> bodies, byte op) {
        Map<GridTopology.Member, CompletableFuture<List<byte[]>>> fs = new LinkedHashMap<>();
        bodies.forEach((m, b) -> fs.put(m, send(m, op, b)));
        Map<GridTopology.Member, List<byte[]>> out = new LinkedHashMap<>();
        fs.forEach((m, f) -> out.put(m, await(m, f)));
        return out;
    }

    public List<byte[]> await(GridTopology.Member m, CompletableFuture<List<byte[]>> f) {
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            throw e.getCause() instanceof GalaxyException ge ? ge
                    : new GalaxyException("grid call to " + m + " failed: " + e.getCause(), e.getCause());
        } catch (TimeoutException e) {
            throw new GalaxyException("grid call to " + m + " timed out after " + timeoutMs + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GalaxyException("interrupted", e);
        }
    }

    /** Pings every member; returns the names that answered. */
    public List<String> ping() {
        List<String> up = new ArrayList<>();
        for (GridTopology.Member m : topology.members()) {
            try {
                call(m, GridProtocol.PING, new byte[0]);
                up.add(m.name());
            } catch (RuntimeException ignored) {
                // down
            }
        }
        return up;
    }

    @Override
    public void close() {
        pools.values().forEach(pool -> {
            for (GridConnection c : pool) {
                if (c != null) {
                    c.close();
                }
            }
        });
        pools.clear();
    }
}
