package io.galaxycache.client;

/** Overall state of the cluster behind a client. */
public enum ClusterState {
    /** Nodes are still joining; data operations are not accepted yet. */
    FORMING,
    /** Every node is up and data operations are accepted. */
    READY,
    /** Nodes are up but the cluster was deactivated (Ignite) or never activated. */
    INACTIVE,
    /** The cluster failed (fail-stop): restart it, then restart the clients. */
    FAILED
}
