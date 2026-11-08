package io.galaxycache.storage;

import io.galaxycache.common.DatasetId;

/** Size information for dashboards. {@code liveRecords} is -1 when the store cannot count cheaply. */
public record DatasetStats(DatasetId dataset, long liveRecords, long bytesOnDisk, int files) {
}
