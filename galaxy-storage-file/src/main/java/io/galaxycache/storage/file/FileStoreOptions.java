package io.galaxycache.storage.file;

import io.galaxycache.common.GalaxyException;

/** Tuning for {@link FileDataStore}. */
public record FileStoreOptions(int pageSize, long segmentSize, boolean fsync, int maxReadBytes) {

    public static final int DEFAULT_PAGE_SIZE = 64 * 1024;
    public static final long DEFAULT_SEGMENT_SIZE = 64L * 1024 * 1024;
    public static final int DEFAULT_MAX_READ_BYTES = 1024 * 1024;

    public FileStoreOptions {
        if (pageSize < 4096 || pageSize > 4 * 1024 * 1024 || Integer.bitCount(pageSize) != 1) {
            throw new GalaxyException("pageSize must be a power of two between 4 KiB and 4 MiB, was " + pageSize);
        }
        if (segmentSize < pageSize || segmentSize % pageSize != 0) {
            throw new GalaxyException("segmentSize must be a multiple of pageSize, was " + segmentSize);
        }
        if (maxReadBytes < pageSize) {
            maxReadBytes = pageSize;
        }
    }

    public static FileStoreOptions defaults() {
        return new FileStoreOptions(DEFAULT_PAGE_SIZE, DEFAULT_SEGMENT_SIZE, true, DEFAULT_MAX_READ_BYTES);
    }
}
