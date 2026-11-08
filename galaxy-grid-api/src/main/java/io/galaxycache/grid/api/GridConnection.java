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
    private final Socket socket;
    private final DataOutputStream out;
    private final DataInputStream in;
    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicLong ids = new AtomicLong();
    private final Map<Long, Pending> pending = new ConcurrentHashMap<>();
    private volatile boolean closed;

    private static final class Pending {
        final CompletableFuture<List<byte[]>> future = new CompletableFuture<>();
        final List<byte[]> parts = new ArrayList<>(2);
    }

    public GridConnection(GridTopology.Member member, int connectTimeoutMs) {
        this.member = member;
        try {
            socket = new Socket();
            socket.setTcpNoDelay(true);
            socket.setReceiveBufferSize(4 * 1024 * 1024);
            socket.setSendBufferSize(1024 * 1024);
            socket.connect(new InetSocketAddress(member.host(), member.port()), connectTimeoutMs);
            out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 256 * 1024));
            in = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 1024 * 1024));
            out.writeInt(GridProtocol.MAGIC);
            out.writeInt(GridProtocol.VERSION);
            out.flush();
        } catch (IOException e) {
            throw new GalaxyException("Cannot connect to grid node " + member + ": " + e.getMessage(), e);
        }
        Thread reader = new Thread(this::readLoop, "galaxy-grid-reader-" + member.name());
        reader.setDaemon(true);
        reader.start();
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
            out.flush();
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
        }
    }

    private void fail(Exception e) {
        if (closed) {
            return;
        }
        closed = true;
        GalaxyException ex = new GalaxyException("connection to grid node " + member + " lost: " + e.getMessage(), e);
        pending.values().forEach(p -> p.future.completeExceptionally(ex));
        pending.clear();
        try {
            socket.close();
        } catch (IOException ignored) {
            // already broken
        }
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
        pending.values().forEach(p -> p.future.completeExceptionally(ex));
        pending.clear();
    }
}
