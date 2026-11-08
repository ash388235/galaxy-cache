package io.galaxycache.storage;

import io.galaxycache.common.DatasetId;

import java.time.Instant;

/** Descriptor stored next to every dataset (for the file store: {@code _dataset.json}). */
public record DatasetMetadata(DatasetId dataset, String domainName, String domainClass, int domainVersion,
                              String codec, Instant createdAt) {
}
