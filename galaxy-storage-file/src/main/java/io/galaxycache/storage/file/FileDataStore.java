package io.galaxycache.storage.file;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.storage.DataStore;
import io.galaxycache.storage.DatasetMetadata;
import io.galaxycache.storage.DatasetStats;
import io.galaxycache.storage.StoredRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Local (or network-mounted) file system storage. Layout:
 * {@code <root>/<namespace>/<domain>/<subDomain>/<date>/{_dataset.json,_commit,.lock,seg-000001.gdat,...}}.
 */
public final class FileDataStore implements DataStore {

    static final String METADATA_FILE = "_dataset.json";

    private final String name;
    private final Path root;
    private final FileStoreOptions options;
    private final Map<DatasetId, PagedDataset> open = new ConcurrentHashMap<>();

    public FileDataStore(String name, Path root, FileStoreOptions options) {
        this.name = name;
        this.root = root.toAbsolutePath().normalize();
        this.options = options;
        try {
            Files.createDirectories(this.root);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create storage root " + this.root, e);
        }
    }

    @Override
    public String name() {
        return name;
    }

    public Path root() {
        return root;
    }

    public FileStoreOptions options() {
        return options;
    }

    private Path dirOf(DatasetId ds) {
        return root.resolve(ds.namespace()).resolve(ds.domain()).resolve(ds.subDomain()).resolve(ds.date());
    }

    private PagedDataset dataset(DatasetId ds) {
        return open.computeIfAbsent(ds, k -> {
            if (!Files.exists(dirOf(k).resolve(METADATA_FILE))) {
                throw new GalaxyException("Dataset " + k + " does not exist in storage '" + name + "'");
            }
            return new PagedDataset(k, dirOf(k), options);
        });
    }

    @Override
    public synchronized DatasetMetadata ensureDataset(DatasetMetadata metadata) {
        Path dir = dirOf(metadata.dataset());
        Path file = dir.resolve(METADATA_FILE);
        try {
            if (Files.exists(file)) {
                return Json.read(Files.readAllBytes(file), DatasetMetadata.class);
            }
            Files.createDirectories(dir);
            Path tmp = dir.resolve(METADATA_FILE + "." + ProcessHandle.current().pid() + ".tmp");
            Files.write(tmp, Json.MAPPER.writerWithDefaultPrettyPrinter().writeValueAsBytes(metadata));
            try {
                // ATOMIC_MOVE without REPLACE_EXISTING: if another process won the race, keep theirs
                Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE);
            } catch (java.nio.file.FileAlreadyExistsException race) {
                Files.deleteIfExists(tmp);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, file);
            }
            return Json.read(Files.readAllBytes(file), DatasetMetadata.class);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create dataset " + metadata.dataset(), e);
        }
    }

    @Override
    public Optional<DatasetMetadata> metadata(DatasetId ds) {
        Path file = dirOf(ds).resolve(METADATA_FILE);
        if (!Files.exists(file)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Json.read(Files.readAllBytes(file), DatasetMetadata.class));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void put(DatasetId ds, StoredRecord record) {
        dataset(ds).putAll(List.of(record));
    }

    @Override
    public void putAll(DatasetId ds, List<StoredRecord> records) {
        if (!records.isEmpty()) {
            dataset(ds).putAll(records);
        }
    }

    @Override
    public Optional<StoredRecord> get(DatasetId ds, String id) {
        return Optional.ofNullable(dataset(ds).getAll(List.of(id)).get(id));
    }

    @Override
    public Map<String, StoredRecord> getAll(DatasetId ds, Collection<String> ids) {
        return dataset(ds).getAll(ids);
    }

    @Override
    public boolean delete(DatasetId ds, String id) {
        return dataset(ds).deleteAll(List.of(id)) == 1;
    }

    @Override
    public int deleteAll(DatasetId ds, Collection<String> ids) {
        return ids.isEmpty() ? 0 : dataset(ds).deleteAll(ids);
    }

    @Override
    public void scan(DatasetId ds, Consumer<StoredRecord> consumer) {
        dataset(ds).scan(consumer, 1024);
    }

    @Override
    public DatasetStats stats(DatasetId ds) {
        PagedDataset d = dataset(ds);
        return new DatasetStats(ds, d.liveRecords(), d.bytesOnDisk(), d.segmentCount());
    }

    @Override
    public List<DatasetId> listDatasets(String namespace) {
        Path base = namespace == null ? root : root.resolve(namespace);
        if (!Files.isDirectory(base)) {
            return List.of();
        }
        List<DatasetId> out = new ArrayList<>();
        try (Stream<Path> files = Files.find(base, namespace == null ? 5 : 4,
                (p, a) -> a.isRegularFile() && p.getFileName().toString().equals(METADATA_FILE))) {
            files.forEach(f -> {
                Path rel = root.relativize(f.getParent());
                if (rel.getNameCount() == 4) {
                    try {
                        out.add(new DatasetId(rel.getName(0).toString(), rel.getName(1).toString(),
                                rel.getName(2).toString(), rel.getName(3).toString()));
                    } catch (IllegalArgumentException ignored) {
                        // not a dataset folder
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        out.sort(Comparator.naturalOrder());
        return out;
    }

    @Override
    public long compact(DatasetId ds) {
        return dataset(ds).compact(PagedDataset.CrashPoint.NONE);
    }

    /** Test hook: compaction that stops as if the process died at {@code crash}. */
    long compact(DatasetId ds, PagedDataset.CrashPoint crash) {
        return dataset(ds).compact(crash);
    }

    @Override
    public synchronized void dropDataset(DatasetId ds) {
        PagedDataset d = open.remove(ds);
        if (d != null) {
            d.close();
        }
        Path dir = dirOf(ds);
        if (!Files.exists(dir)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(dir)) {
            for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(p);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot drop dataset " + ds, e);
        }
    }

    @Override
    public void close() {
        open.values().forEach(PagedDataset::close);
        open.clear();
    }

    @Override
    public String toString() {
        return "FileDataStore[" + name + " @ " + root + ", pageSize=" + options.pageSize() + "]";
    }
}
