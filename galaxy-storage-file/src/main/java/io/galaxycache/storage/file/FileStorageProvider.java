package io.galaxycache.storage.file;

import io.galaxycache.common.GalaxyException;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.StorageProvider;

import java.nio.file.Path;
import java.util.Map;

/**
 * {@code type: file}. Settings: {@code path} (required), {@code pageSize} (default 65536),
 * {@code segmentSize} (default 64 MiB), {@code fsync} (default true), {@code maxReadBytes} (default 1 MiB).
 */
public final class FileStorageProvider implements StorageProvider {

    @Override
    public String type() {
        return "file";
    }

    @Override
    public DataStore open(String name, Map<String, Object> settings) {
        Object path = settings.get("path");
        if (path == null) {
            throw new GalaxyException("Storage '" + name + "' of type file needs a 'path'");
        }
        FileStoreOptions o = new FileStoreOptions(
                intSetting(settings, "pageSize", FileStoreOptions.DEFAULT_PAGE_SIZE),
                longSetting(settings, "segmentSize", FileStoreOptions.DEFAULT_SEGMENT_SIZE),
                boolSetting(settings, "fsync", true),
                intSetting(settings, "maxReadBytes", FileStoreOptions.DEFAULT_MAX_READ_BYTES),
                boolSetting(settings, "exclusive", false));
        return new FileDataStore(name, Path.of(path.toString()), o);
    }

    private static int intSetting(Map<String, Object> s, String key, int def) {
        Object v = s.get(key);
        return v == null ? def : Integer.parseInt(v.toString());
    }

    private static long longSetting(Map<String, Object> s, String key, long def) {
        Object v = s.get(key);
        return v == null ? def : Long.parseLong(v.toString());
    }

    private static boolean boolSetting(Map<String, Object> s, String key, boolean def) {
        Object v = s.get(key);
        return v == null ? def : Boolean.parseBoolean(v.toString());
    }
}
