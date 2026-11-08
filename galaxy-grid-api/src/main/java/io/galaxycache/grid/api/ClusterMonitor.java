package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Fail-stop watchdog over a set of grid members, used by nodes (watching their peers) and by clients (watching
 * all nodes). After {@link #awaitFormed} it pings every member each {@code heartbeatMs}; the first of these
 * ends the grid for this process and calls {@code onFailure} exactly once:
 * <ul>
 *   <li>a member unreachable for {@code failAfterMs}</li>
 *   <li>a member that restarted (new startup id) or reports FAILED</li>
 *   <li>a member with a different topology fingerprint</li>
 * </ul>
 */
public final class ClusterMonitor implements AutoCloseable {

    private static final System.Logger LOG = System.getLogger(ClusterMonitor.class.getName());

    private final GridClient client;
    private final List<GridTopology.Member> watched;
    private final String fingerprint;
    private final long heartbeatMs;
    private final long failAfterMs;
    private final boolean requireReady;
    private final Consumer<String> onFailure;
    private final Map<String, Long> startupIds = new HashMap<>();
    private final Map<String, Long> lastOk = new HashMap<>();
    private final AtomicBoolean failed = new AtomicBoolean();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "galaxy-grid-heartbeat");
        t.setDaemon(true);
        return t;
    });

    /**
     * @param requireReady clients need every member READY; nodes forming a grid accept FORMING peers
     */
    public ClusterMonitor(GridClient client, List<GridTopology.Member> watched, String fingerprint, long heartbeatMs,
                          long failAfterMs, boolean requireReady, Consumer<String> onFailure) {
        this.client = client;
        this.watched = List.copyOf(watched);
        this.fingerprint = fingerprint;
        this.heartbeatMs = heartbeatMs;
        this.failAfterMs = failAfterMs;
        this.requireReady = requireReady;
        this.onFailure = onFailure;
    }

    /** Asks one member for its status, waiting at most {@code timeoutMs}. */
    public static NodeStatus ping(GridClient client, GridTopology.Member m, long timeoutMs) {
        return Json.read(client.call(m, GridProtocol.PING, new byte[0], timeoutMs).get(0), NodeStatus.class);
    }

    /**
     * Waits until every watched member answers with the same fingerprint (and READY if required), then remembers
     * their startup ids. Throws with the last problem seen if that does not happen within {@code timeoutMs}.
     */
    public synchronized void awaitFormed(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        String problem = "no answer yet";
        while (true) {
            Map<String, Long> ids = new HashMap<>();
            problem = null;
            for (GridTopology.Member m : watched) {
                try {
                    NodeStatus s = ping(client, m, Math.max(500, heartbeatMs));
                    if (!fingerprint.equals(s.fingerprint())) {
                        throw new GalaxyException("grid node " + m.name() + " runs a different grid configuration ("
                                + s.fingerprint() + " vs " + fingerprint + "); all nodes and clients must use the same nodes/partitions");
                    }
                    if (NodeStatus.FAILED.equals(s.state()) || (requireReady && !NodeStatus.READY.equals(s.state()))) {
                        problem = m.name() + " is " + s.state() + (s.reason() == null ? "" : " (" + s.reason() + ")");
                        break;
                    }
                    ids.put(m.name(), s.startupId());
                } catch (GalaxyException e) {
                    if (e.getMessage() != null && e.getMessage().contains("different grid configuration")) {
                        throw e;
                    }
                    problem = m.name() + " unreachable: " + e.getMessage();
                    break;
                }
            }
            if (problem == null) {
                long now = System.currentTimeMillis();
                startupIds.clear();
                startupIds.putAll(ids);
                watched.forEach(m -> lastOk.put(m.name(), now));
                return;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new GalaxyException("grid not ready after " + timeoutMs + " ms: " + problem);
            }
            sleep(Math.min(500, heartbeatMs));
        }
    }

    /** Starts the heartbeat; call after {@link #awaitFormed}. */
    public void start() {
        if (!timer.isShutdown()) {
            timer.scheduleWithFixedDelay(this::check, heartbeatMs, heartbeatMs, TimeUnit.MILLISECONDS);
        }
    }

    public boolean failed() {
        return failed.get();
    }

    private synchronized void check() {
        if (failed.get()) {
            return;
        }
        long now = System.currentTimeMillis();
        for (GridTopology.Member m : watched) {
            try {
                NodeStatus s = ping(client, m, Math.max(500, heartbeatMs));
                if (NodeStatus.FAILED.equals(s.state())) {
                    fail("grid node " + m.name() + " failed: " + s.reason());
                    return;
                }
                Long known = startupIds.get(m.name());
                if (known != null && known != s.startupId()) {
                    fail("grid node " + m.name() + " restarted (its in-memory state no longer matches the grid)");
                    return;
                }
                if (!fingerprint.equals(s.fingerprint())) {
                    fail("grid node " + m.name() + " now runs a different grid configuration");
                    return;
                }
                lastOk.put(m.name(), now);
            } catch (RuntimeException e) {
                long since = now - lastOk.getOrDefault(m.name(), now);
                if (since >= failAfterMs) {
                    fail("grid node " + m.name() + " unreachable for " + since + " ms: " + e.getMessage());
                    return;
                }
            }
        }
    }

    /** Ends the grid for this process (idempotent). */
    public void fail(String reason) {
        if (failed.compareAndSet(false, true)) {
            LOG.log(System.Logger.Level.ERROR, "GRID FAILURE: {0}", reason);
            timer.shutdown(); // not shutdownNow: this may run on the timer thread itself
            try {
                onFailure.accept(reason);
            } catch (RuntimeException e) {
                LOG.log(System.Logger.Level.ERROR, "grid failure handler threw", e);
            }
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GalaxyException("interrupted", e);
        }
    }

    @Override
    public void close() {
        timer.shutdown();
    }
}
