package io.galaxycache.index.server.persistence;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.Json;
import io.galaxycache.index.api.IndexDocument;
import io.galaxycache.index.api.IndexPersistence;
import io.galaxycache.index.api.IndexPersistenceProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * {@code persistence: postgres} (STUB, not exercised in v1). Plain JDBC; needs the PostgreSQL driver on
 * the classpath and {@code persistenceSettings: {jdbcUrl, user, password}}.
 *
 * <pre>
 * CREATE TABLE IF NOT EXISTS galaxy_index_doc (
 *   node       TEXT NOT NULL,
 *   dataset    TEXT NOT NULL,
 *   id         TEXT NOT NULL,
 *   fields     JSONB NOT NULL,
 *   updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 *   PRIMARY KEY (node, dataset, id));
 * </pre>
 */
public final class PostgresIndexPersistence implements IndexPersistence {

    private static final String DDL = "CREATE TABLE IF NOT EXISTS galaxy_index_doc (node TEXT NOT NULL, dataset TEXT NOT NULL, "
            + "id TEXT NOT NULL, fields JSONB NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            + "PRIMARY KEY (node, dataset, id))";

    private final String node;
    private final Connection conn;

    public PostgresIndexPersistence(String node, String url, String user, String password) {
        this.node = node;
        try {
            this.conn = DriverManager.getConnection(url, user, password);
            this.conn.setAutoCommit(false);
            try (Statement s = conn.createStatement()) {
                s.execute(DDL);
            }
            conn.commit();
        } catch (SQLException e) {
            throw new GalaxyException("Cannot connect to Postgres at " + url + ": " + e.getMessage(), e);
        }
    }

    @Override
    public String type() {
        return "postgres";
    }

    @Override
    public synchronized void upsert(DatasetId dataset, List<IndexDocument> docs) {
        String sql = "INSERT INTO galaxy_index_doc(node, dataset, id, fields) VALUES (?, ?, ?, ?::jsonb) "
                + "ON CONFLICT (node, dataset, id) DO UPDATE SET fields = EXCLUDED.fields, updated_at = now()";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (IndexDocument d : docs) {
                ps.setString(1, node);
                ps.setString(2, dataset.key());
                ps.setString(3, d.id());
                ps.setString(4, Json.write(d.fields()));
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            rollback();
            throw new GalaxyException("Postgres upsert failed", e);
        }
    }

    @Override
    public synchronized void delete(DatasetId dataset, Collection<String> ids) {
        try (PreparedStatement ps = conn.prepareStatement(
                "DELETE FROM galaxy_index_doc WHERE node = ? AND dataset = ? AND id = ?")) {
            for (String id : ids) {
                ps.setString(1, node);
                ps.setString(2, dataset.key());
                ps.setString(3, id);
                ps.addBatch();
            }
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            rollback();
            throw new GalaxyException("Postgres delete failed", e);
        }
    }

    @Override
    public synchronized void dropDataset(DatasetId dataset) {
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM galaxy_index_doc WHERE node = ? AND dataset = ?")) {
            ps.setString(1, node);
            ps.setString(2, dataset.key());
            ps.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            rollback();
            throw new GalaxyException("Postgres drop failed", e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized void loadAll(BiConsumer<DatasetId, IndexDocument> sink) {
        try (PreparedStatement ps = conn.prepareStatement("SELECT dataset, id, fields::text FROM galaxy_index_doc WHERE node = ?")) {
            ps.setString(1, node);
            ps.setFetchSize(5000);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    sink.accept(DatasetId.parse(rs.getString(1)),
                            new IndexDocument(rs.getString(2), Json.read(rs.getString(3), Map.class)));
                }
            }
            conn.commit();
        } catch (SQLException e) {
            rollback();
            throw new GalaxyException("Postgres load failed", e);
        }
    }

    private void rollback() {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // nothing more to do
        }
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (SQLException ignored) {
            // best effort
        }
    }

    public static final class Provider implements IndexPersistenceProvider {
        @Override
        public String type() {
            return "postgres";
        }

        @Override
        public IndexPersistence create(String nodeName, Map<String, Object> s) {
            Object url = s.get("jdbcUrl");
            if (url == null) {
                throw new GalaxyException("postgres persistence needs persistenceSettings.jdbcUrl");
            }
            return new PostgresIndexPersistence(nodeName, url.toString(),
                    String.valueOf(s.getOrDefault("user", "")), String.valueOf(s.getOrDefault("password", "")));
        }
    }
}
