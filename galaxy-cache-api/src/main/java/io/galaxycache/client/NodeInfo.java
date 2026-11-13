package io.galaxycache.client;

/**
 * One server node as the cluster reports it.
 *
 * @param name       stable node name (galaxy grid member name, Ignite consistent id)
 * @param address    host:port the node serves on
 * @param state      FORMING / READY / FAILED (galaxy) or ACTIVE / INACTIVE (Ignite)
 * @param partitions primary partitions this node owns, or -1 when unknown
 */
public record NodeInfo(String name, String address, String state, int partitions) {
}
