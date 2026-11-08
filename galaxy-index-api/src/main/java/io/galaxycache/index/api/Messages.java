package io.galaxycache.index.api;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.query.Query;

import java.util.List;
import java.util.Map;

/** JSON wire messages of the index pod HTTP API. */
public final class Messages {

    private Messages() {
    }

    public record UpsertRequest(DatasetId dataset, List<IndexDocument> docs) {
    }

    public record DeleteRequest(DatasetId dataset, List<String> ids) {
    }

    public record DropRequest(DatasetId dataset) {
    }

    public record QueryRequest(DatasetId dataset, Query query) {
    }

    /** {@code total} is the number of matches before limit and offset were applied. */
    public record QueryResponse(List<String> ids, long total, long tookMicros) {
    }

    public record CountResponse(long count) {
    }

    public record AckResponse(int affected) {
    }

    public record ErrorResponse(String error) {
    }

    public record DatasetIndexStats(DatasetId dataset, long documents, List<String> fields) {
    }

    public record StatsResponse(String node, long startedAtEpochMs, String persistence,
                                List<String> namespaces, List<DatasetIndexStats> datasets,
                                Map<String, Long> counters) {
    }
}
