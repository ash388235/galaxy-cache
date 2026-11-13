package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Connections to every member of one grid ({@code connectionsPerNode} each, used round-robin), plus helpers
 * to call one member or all of them in parallel. Thread-safe.
 *
 * <p>Fail-fast: once {@link #fail} is called (by the cluster monitor, or because a connection broke while
 * {@code failOnConnectionLoss} is set) every call, including ones already waiting, throws
 * {@link GridUnavailableException}. There is no reconnect: the grid is restarted as a whole.
 */
public final class GridClient implements AutoCloseable {

    private final GridTopology topology;
    private final int connectionsPerNode;
    private final int timeoutMs;
    private final Map<String, GridConnection[]> pools = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> rr = new ConcurrentHashMap<>();
    private final Consumer<String> onConnectionLost;
    private final GridSecurity security;
    private volatile String failure;
    private final java.util.List<Consumer<String>> failureListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    public GridClient(GridTopology topology, int connectionsPerNode, int timeoutMs) {
        this(topology, connectionsPerNode, timeoutMs, null);
    }

    /** @param onConnectionLost if not null, called when an established connection breaks (fail-fast clients) */
    public GridClient(GridTopology topology, int connectionsPerNode, int timeoutMs, Consumer<String> onConnectionLost) {
        this(topology, connectionsPerNode, timeoutMs, onConnectionLost, GridSecurity.NONE);
    }

    public GridClient(GridTopology topology, int connectionsPerNode, int timeoutMs, Consumer<String> onConnectionLost,
                      GridSecurity security) {
        this.security = security;
        this.topology = topology;
        this.connectionsPerNode = Math.max(1, connectionsPerNode);
        this.timeoutMs = timeoutMs;
        this.onConnectionLost = onConnectionLost;
    }

    public GridTopology topology() {
        return topology;
    }

    /** Why the grid failed for this client, or null while it is healthy. */
    public String failure() {
        return failure;
    }

    /** Ends this client: all current and future calls fail with {@code reason}. */
    public void fail(String reason) {
        boolean first;
        synchronized (failureListeners) {
            first = failure == null;
            if (first) {
                failure = reason;
            }
        }
        if (first) {
            closeConnections();
            failureListeners.forEach(l -> {
                try {
                    l.accept(reason);
                } catch (RuntimeException ignored) {
                    // a listener must not stop the others
                }
            });
        }
    }

    /** {@code listener} gets the reason once when this client fails (at once if it already has). */
    public void onFailure(Consumer<String> listener) {
        String already;
        synchronized (failureListeners) {
            already = failure;
            if (already == null) {
                failureListeners.add(listener);
            }
        }
        if (already != null) {
            listener.accept(already);
        }
    }

    private GridConnection connection(GridTopology.Member m) {
        GridConnection[] pool = pools.computeIfAbsent(m.name(), k -> new GridConnection[connectionsPerNode]);
        int i = Math.floorMod(rr.computeIfAbsent(m.name(), k -> new AtomicInteger()).getAndIncrement(), pool.length);
        GridConnection c = pool[i];
        if (c == null || !c.isOpen()) {
            synchronized (pool) {
                c = pool[i];
                if (c == null || !c.isOpen()) {
                    if (c != null && onConnectionLost != null) {
                        throw new GridUnavailableException("connection to grid node " + m.name() + " was lost");
                    }
                    c = new GridConnection(m, topology.fingerprint(), Math.min(timeoutMs, 5_000), onConnectionLost, security);
                    pool[i] = c;
                }
            }
        }
        return c;
    }

    /** Starts a call; use {@link #await} to wait for it. */
    public CompletableFuture<List<byte[]>> send(GridTopology.Member m, byte op, byte[] body) {
        String f = failure;
        if (f != null) {
            return CompletableFuture.failedFuture(new GridUnavailableException(f));
        }
        if (body.length > GridProtocol.MAX_FRAME) {
            return CompletableFuture.failedFuture(new GalaxyException("request of " + body.length
                    + " bytes exceeds the grid frame limit of " + GridProtocol.MAX_FRAME + " bytes"));
        }
        try {
            return connection(m).call(op, body);
        } catch (GalaxyException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public List<byte[]> call(GridTopology.Member m, byte op, byte[] body) {
        return await(m, send(m, op, body), timeoutMs);
    }

    public List<byte[]> call(GridTopology.Member m, byte op, byte[] body, long timeoutMs) {
        return await(m, send(m, op, body), timeoutMs);
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
        return await(m, f, timeoutMs);
    }

    public List<byte[]> await(GridTopology.Member m, CompletableFuture<List<byte[]>> f, long waitMs) {
        try {
            return f.get(waitMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            String reason = failure;
            if (reason != null) {
                throw new GridUnavailableException(reason);
            }
            throw e.getCause() instanceof GalaxyException ge ? ge
                    : new GalaxyException("grid call to " + m + " failed: " + e.getCause(), e.getCause());
        } catch (TimeoutException e) {
            f.cancel(false);
            throw new GalaxyException("grid call to " + m + " timed out after " + waitMs + " ms");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GalaxyException("interrupted", e);
        }
    }

    /**
     * Streams a large response (e.g. SCAN) frame by frame to {@code onFrame}, on the calling thread, over a
     * connection opened for this stream only. At most a few frames are buffered; a slow consumer slows the node.
     */
    public void stream(GridTopology.Member m, byte op, byte[] body, Consumer<byte[]> onFrame) {
        String f = failure;
        if (f != null) {
            throw new GridUnavailableException(f);
        }
        java.util.concurrent.BlockingQueue<Object> q = new java.util.concurrent.ArrayBlockingQueue<>(4);
        GridConnection c = new GridConnection(m, topology.fingerprint(), Math.min(timeoutMs, 5_000), onConnectionLost, security);
        try {
            c.stream(op, body, q);
            while (true) {
                Object o;
                try {
                    o = q.poll(timeoutMs, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new GalaxyException("interrupted", e);
                }
                if (o == null) {
                    throw new GalaxyException("stream from " + m + " stalled for " + timeoutMs + " ms");
                }
                if (o == GridConnection.END) {
                    return;
                }
                if (o instanceof RuntimeException e) {
                    String reason = failure;
                    throw reason != null ? new GridUnavailableException(reason) : e;
                }
                onFrame.accept((byte[]) o);
            }
        } finally {
            c.close();
        }
    }

    private void closeConnections() {
        pools.values().forEach(pool -> {
            for (GridConnection c : pool) {
                if (c != null) {
                    c.close();
                }
            }
        });
    }

    @Override
    public void close() {
        closeConnections();
        pools.clear();
    }
}
