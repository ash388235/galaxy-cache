package io.galaxycache.client;

/**
 * Optional features of the cache API. Operations every provider offers (put, get, query, delete, stats, datasets,
 * namespaces) have no capability. An operation tied to a capability the provider lacks throws
 * {@link NotSupportedException}; ask first with {@link GalaxyClient#supports(Capability)}.
 * The full matrix is in {@code docs/PROVIDERS.md}.
 */
public enum Capability {

    /** {@code putIfAbsent}, {@code replace}, {@code getAndPut}: atomic single-key operations. */
    ATOMIC_OPS("putIfAbsent / replace / getAndPut"),
    /** {@code put(id, value, ttl)}: entries that expire. */
    EXPIRY("put with a time-to-live"),
    /** {@code inTransaction}: several keys changed all-or-nothing. */
    TRANSACTIONS("inTransaction"),
    /** {@code listen}: change events pushed to the client. */
    CHANGE_EVENTS("listen (change events)"),
    /** {@code nodes}, {@code state}, {@code awaitReady}, {@code onFailure} on {@link GalaxyCluster}. */
    CLUSTER_VIEW("cluster nodes / state / awaitReady / onFailure"),
    /** {@code partitions}, {@code ownerOf}. */
    PARTITION_MAP("cluster partitions / ownerOf"),
    /** {@code nodeStats}. */
    NODE_STATS("cluster nodeStats"),
    /** {@code rebalance}: move data to the nodes that own it after a membership change. */
    REBALANCE("cluster rebalance"),
    /** {@code compact}: reclaim disk held by old versions and deleted records. */
    COMPACT("cluster compact"),
    /** {@code activate}, {@code deactivate}. */
    ACTIVATION("cluster activate / deactivate"),
    /** {@code baseline}, {@code setBaseline}. */
    BASELINE("cluster baseline / setBaseline"),
    /** Data survives the loss of a node (backups + failover). Informational: no method of its own. */
    FAILOVER("surviving a node loss (backups)"),
    /** Nodes can be added to a running cluster. Informational: no method of its own. */
    ONLINE_SCALE("adding nodes while running");

    private final String operations;

    Capability(String operations) {
        this.operations = operations;
    }

    /** The API operations this capability covers, for messages. */
    public String operations() {
        return operations;
    }
}
