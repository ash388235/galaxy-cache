package io.galaxycache.ignite.node;

import io.galaxycache.client.NodeInfo;
import io.galaxycache.client.NodeStats;
import io.galaxycache.common.Json;
import io.galaxycache.ignite.IgniteNames;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Server side of the cluster API for {@code provider: ignite}, run by thin clients by class name. The argument is
 * {@code "<command>\n<argument>"}; the result is JSON. {@code nodeStats} runs on every server, the other commands
 * on one.
 */
public final class GalaxyAdminTask extends ComputeTaskAdapter<String, String> {

    private static final long serialVersionUID = 1L;

    @Override
    public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) {
        String[] parts = arg == null ? new String[]{""} : arg.split("\n", 2);
        String command = parts[0];
        String argument = parts.length > 1 ? parts[1] : "";
        Map<ComputeJob, ClusterNode> jobs = new LinkedHashMap<>();
        if (command.equals("nodeStats")) {
            subgrid.stream().filter(n -> !n.isClient()).forEach(n -> jobs.put(new Job(command, argument), n));
        } else {
            ClusterNode target = subgrid.stream().filter(n -> !n.isClient()).findFirst()
                    .orElseThrow(() -> new IgniteException("no server node to run '" + command + "' on"));
            jobs.put(new Job(command, argument), target);
        }
        return jobs;
    }

    @Override
    public String reduce(List<ComputeJobResult> results) {
        if (results.size() > 1 || ((Job) results.get(0).getJob()).command.equals("nodeStats")) {
            List<Object> all = new ArrayList<>();
            for (ComputeJobResult r : results) {
                if (r.getException() != null) {
                    throw r.getException();
                }
                all.add(Json.read((String) r.getData(), NodeStats.class));
            }
            all.sort((a, b) -> ((NodeStats) a).node().compareTo(((NodeStats) b).node()));
            return Json.write(all);
        }
        ComputeJobResult r = results.get(0);
        if (r.getException() != null) {
            throw r.getException();
        }
        return r.getData();
    }

    /** One command, run on the node it is mapped to. */
    static final class Job extends ComputeJobAdapter {

        private static final long serialVersionUID = 1L;

        final String command;
        final String argument;

        @IgniteInstanceResource
        private transient Ignite ignite;

        Job(String command, String argument) {
            this.command = command;
            this.argument = argument;
        }

        @Override
        public Object execute() {
            return switch (command) {
                case "nodes" -> Json.write(nodes(ignite));
                case "ownerOf" -> Json.write(String.valueOf(
                        ignite.affinity(IgniteNames.AFFINITY).mapKeyToNode(argument).consistentId()));
                case "nodeStats" -> Json.write(localStats(ignite));
                case "baseline" -> Json.write(baseline(ignite));
                case "setBaseline" -> {
                    setBaseline(ignite, List.of(argument.split(",")));
                    yield Json.write("ok");
                }
                case "rebalance" -> {
                    setBaseline(ignite, IgniteNode.names(ignite.cluster().forServers().nodes()));
                    Map<String, Long> owned = new TreeMap<>();
                    nodes(ignite).forEach(n -> owned.put(n.name(), (long) n.partitions()));
                    yield Json.write(owned);
                }
                default -> throw new IgniteException("unknown admin command '" + command + "'");
            };
        }
    }

    static List<NodeInfo> nodes(Ignite ignite) {
        String state = ignite.cluster().state().name();
        List<NodeInfo> out = new ArrayList<>();
        for (ClusterNode n : ignite.cluster().forServers().nodes()) {
            int primary;
            try {
                primary = ignite.affinity(IgniteNames.AFFINITY).primaryPartitions(n).length;
            } catch (RuntimeException e) {
                primary = -1;
            }
            Collection<String> addrs = n.addresses();
            Object port = n.attribute("clientListenerPort");
            String address = (addrs.isEmpty() ? "?" : addrs.iterator().next()) + (port == null ? "" : ":" + port);
            out.add(new NodeInfo(String.valueOf(n.consistentId()), address, state, primary));
        }
        out.sort((a, b) -> a.name().compareTo(b.name()));
        return out;
    }

    static NodeStats localStats(Ignite ignite) {
        long records = 0;
        Map<String, Long> perCache = new HashMap<>();
        for (String cache : ignite.cacheNames()) {
            if (cache.startsWith(IgniteNames.DATASET_CACHE_PREFIX)) {
                long n = ignite.cache(cache).localSizeLong(CachePeekMode.PRIMARY);
                records += n;
                perCache.put("records." + cache, n);
            }
        }
        long bytes = -1;
        try {
            bytes = ignite.dataRegionMetrics(IgniteNode.DATA_REGION).getTotalUsedSize();
        } catch (RuntimeException ignored) {
            // metrics not available
        }
        return new NodeStats(String.valueOf(ignite.cluster().localNode().consistentId()), records, bytes, perCache);
    }

    static List<String> baseline(Ignite ignite) {
        Collection<BaselineNode> b = ignite.cluster().currentBaselineTopology();
        if (b == null) {
            return IgniteNode.names(ignite.cluster().forServers().nodes());
        }
        return b.stream().map(n -> String.valueOf(n.consistentId())).sorted().toList();
    }

    static void setBaseline(Ignite ignite, List<String> names) {
        Map<String, ClusterNode> byName = new HashMap<>();
        ignite.cluster().forServers().nodes().forEach(n -> byName.put(String.valueOf(n.consistentId()), n));
        List<BaselineNode> target = new ArrayList<>();
        for (String name : names) {
            ClusterNode n = byName.get(name.trim());
            if (n == null) {
                throw new IgniteException("node '" + name + "' is not up; servers up: " + new TreeMap<>(byName).keySet());
            }
            target.add(n);
        }
        if (ignite.cluster().isBaselineAutoAdjustEnabled()) {
            ignite.cluster().baselineAutoAdjustEnabled(false);
        }
        ignite.cluster().setBaselineTopology(target);
    }
}
