package io.galaxycache.index.api;

/**
 * Implemented by a {@code DataStore} that also holds the index of its data (a grid, where each node indexes the
 * partitions it stores). The client then uses this index instead of the namespace's index pod.
 */
public interface IndexingStore {

    IndexClient indexClient();
}
