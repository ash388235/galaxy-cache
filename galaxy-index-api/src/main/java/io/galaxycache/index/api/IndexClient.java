package io.galaxycache.index.api;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.query.Query;

import java.util.Collection;
import java.util.List;

/** Client view of one index pod. The default implementation talks HTTP; tests may call the engine directly. */
public interface IndexClient extends AutoCloseable {

    void upsert(DatasetId dataset, List<IndexDocument> docs);

    void delete(DatasetId dataset, Collection<String> ids);

    Messages.QueryResponse query(DatasetId dataset, Query query);

    void drop(DatasetId dataset);

    Messages.StatsResponse stats();

    boolean healthy();

    @Override
    default void close() {
    }
}
