package io.galaxycache.ignite;

/** Names shared by the Ignite provider (client) and the Ignite server nodes. */
public final class IgniteNames {

    private IgniteNames() {
    }

    /** Cache template every dataset table is created from (partitions, backups, loss policy). */
    public static final String TEMPLATE = "gx_template";
    /** Replicated: dataset key to its metadata and table layout (JSON). */
    public static final String DATASETS = "gx_datasets";
    /** Replicated: namespaces created at runtime (JSON). */
    public static final String NAMESPACES = "gx_namespaces";
    /** Replicated: cluster facts written by servers, e.g. {@link #FINGERPRINT_KEY}. */
    public static final String META = "gx_meta";
    public static final String FINGERPRINT_KEY = "fingerprint";
    /** Partitioned like every dataset table, empty: answers "which node owns this key". */
    public static final String AFFINITY = "gx_affinity";
    /** Compute task on the servers behind the cluster API; arguments and results are text. */
    public static final String ADMIN_TASK = "io.galaxycache.ignite.node.GalaxyAdminTask";
    /** Prefix of every cache that holds a dataset. */
    public static final String DATASET_CACHE_PREFIX = "GX_";
}
