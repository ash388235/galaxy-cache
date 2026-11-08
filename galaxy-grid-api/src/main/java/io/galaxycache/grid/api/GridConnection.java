package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One TCP connection to a grid node, shared by many threads: requests are tagged with an id, a reader thread
 * routes response frames back to the waiting caller. Streamed responses are collected frame by frame.
 */
public final class GridConnection implements AutoCloseable {

    private final GridTopology.Member member;
    private Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicLong ids = new AtomicLong();
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private static final class Pending {
        final CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        final List<byte[]> parts = new ArrayList<>(2);
        /** When set, frames are handed to the caller one by one instead of being collected (streaming). */
        java.util.concurrent.BlockingQueue<Object> queue;
    }

    /** Marks the end of a streamed response in the queue. */
    public static final Object END = new Object();

    /**
     * Sends one request whose (possibly huge) response is consumed frame by frame: each frame body, then
     * {@link #END}, or an exception, is put into {@code queue}. A bounded queue gives TCP backpressure, so use a
     * connection dedicated to this stream.
     */
    public void stream(byte op, byte[] body, java.util.concurrent.BlockingQueue<Object> queue) {
        if (closed) {
            throw new GalaxyException("connection to " + member + " is closed");
        }
        long id = ids.incrementAndGet();
        Pending p = new Pending();
        p.queue = queue;
        pending.put(id, p);
        writeLock.lock();
        try {
            GridProtocol.writeFrame(out, id, op, body, 0, body.length);
            if (!writeLock.hasQueuedThreads()) {
                out.flush(); // otherwise the next writer in line flushes both: fewer syscalls under load
            }
        } catch (IOException e) {
            pending.remove(id);
            fail(e);
            throw new GalaxyException("send to " + member + " failed: " + e.getMessage(), e);
        } finally {
            writeLock.unlock();
        }
    }

    private static void offer(Pending p, Object o) throws IOException {
        try {
            p.queue.put(o);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while streaming", e);
        }
    }

    private final java.util.function.Consumer<String> onLost;

    /**
     * @param fingerprint the client's grid fingerprint; the node refuses the connection if it differs
     * @param onLost      called once if the connection breaks (not when {@link #close()} is called)
     */
    public GridConnection(GridTopology.Member member, String fingerprint, int connectTimeoutMs,
                          java.util.function.Consumer<String> onLost) {
        this(member, fingerprint, connectTimeoutMs, onLost, GridSecurity.NONE);
    }

    public GridConnection(GridTopology.Member member, String fingerprint, int connectTimeoutMs,
                          java.util.function.Consumer<String> onLost, GridSecurity security) {
        this.member = member;
        this.onLost = onLost == null ? reason -> { } : onLost;
        try {
            socket = security.clientSocket(member.host(), member.port(), connectTimeoutMs);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 256 * 1024));
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1024 * 1024));
            socket.setSoTimeout(connectTimeoutMs);
            GridProtocol.clientHandshake(in, out, fingerprint, security.token(), "grid node " + member);
            socket.setSoTimeout(0);
        } catch (GalaxyException e) {
            closeQuietly();
            throw e;
        } catch (IOException e) {
            closeQuietly();
            throw new GalaxyException("Cannot connect to grid node " + member + ": " + e.getMessage(), e);
        }
        Thread reader = new Thread(this::readLoop, "galaxy-grid-reader-" + member.name());
        reader.setDaemon(true);
        reader.start();
    }

    private void closeQuietly() {
        try {
            socket.close();
        } catch (IOException | RuntimeException ignored) {
            // failed connect
        }
    }

    public GridTopology.Member member() {
        return member;
    }

    public boolean isOpen() {
        return !closed;
    }

    /** Sends one request; the future completes with all response frame bodies, in order. */
    public CompletableFuture<List<byte[]>> call(byte op, byte[] body) {
        if (closed) {
            return CompletableFuture.failedFuture(new GalaxyException("connection to " + member + " is closed"));
        }
        long id = ids.incrementAndGet();
        Pending p = new Pending();
        pending.put(id, p);
        writeLock.lock();
        try {
            GridProtocol.writeFrame(out, id, op, body, 0, body.length);
            if (!writeLock.hasQueuedThreads()) {
                out.flush(); // otherwise the next writer in line flushes both: fewer syscalls under load
            }
        } catch (IOException e) {
            pending.remove(id);
            fail(e);
            p.future.completeExceptionally(new GalaxyException("send to " + member + " failed: " + e.getMessage(), e));
        } finally {
            writeLock.unlock();
        }
        return p.future;
    }

    private void readLoop() {
        try {
            while (!closed) {
                GridProtocol.Frame f = GridProtocol.readFrame(in);
                Pending p = pending.get(f.id());
                if (p == null) {
                    continue; // caller gave up (timeout)
                }
                if (p.queue != null) {
                    switch (f.code()) {
                        case GridProtocol.PART -> offer(p, f.body());
                        case GridProtocol.LAST -> {
                            pending.remove(f.id());
                            offer(p, f.body());
                            offer(p, END);
                        }
                        case GridProtocol.ERROR -> {
                            pending.remove(f.id());
                            offer(p, new GalaxyException("grid node " + member.name() + ": "
                                    + new String(f.body(), StandardCharsets.UTF_8)));
                        }
                        default -> throw new IOException("unknown response status " + f.code());
                    }
                    continue;
                }
                switch (f.code()) {
                    case GridProtocol.PART -> p.parts.add(f.body());
                    case GridProtocol.LAST -> {
                        pending.remove(f.id());
                        p.parts.add(f.body());
                        p.future.complete(p.parts);
                    }
                    case GridProtocol.ERROR -> {
                        pending.remove(f.id());
                        p.future.completeExceptionally(new GalaxyException("grid node " + member.name() + ": "
                                + new String(f.body(), StandardCharsets.UTF_8)));
                    }
                    default -> throw new IOException("unknown response status " + f.code());
                }
            }
        } catch (IOException e) {
            fail(e);
        } catch (Throwable t) { // e.g. OutOfMemoryError: without a reader every waiting call would hang until its timeout
            fail(new IOException("reader thread died: " + t, t));
        }
    }

    private void fail(Exception e) {
        if (closed) {
            return;
        }
        closed = true;
        try {
            socket.close();
        } catch (IOException ignored) {
            // already broken
        }
        // first tell the owner, so that callers woken below already see the grid as failed
        String reason = "connection to grid node " + member.name() + " lost: " + e.getMessage();
        onLost.accept(reason);
        GalaxyException ex = new GridUnavailableException(reason);
        pending.values().forEach(p -> {
            p.future.completeExceptionally(ex);
            if (p.queue != null) {
                p.queue.offer(ex);
            }
        });
        pending.clear();
    }
    @Override
    public void close() {
        closed = true;
        try {
            socket.close();
        } catch (IOException ignored) {
            // best effort
        }
        GalaxyException ex = new GalaxyException("connection to " + member + " closed");
        pending.values().forEach(p -> {
            p.future.completeExceptionally(ex);
            if (p.queue != null) {
                p.queue.offer(ex);
            }
        });
        pending.clear();
    }
}
