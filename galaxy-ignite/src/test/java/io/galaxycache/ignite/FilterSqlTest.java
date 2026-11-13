package io.galaxycache.ignite;

import io.galaxycache.common.DatasetId;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static io.galaxycache.common.query.Filters.*;
import static org.junit.jupiter.api.Assertions.*;

class FilterSqlTest {

    static final IgniteTable TABLE;

    static {
        Map<String, Class<?>> types = new LinkedHashMap<>();
        types.put("symbol", String.class);
        types.put("qty", long.class);
        types.put("price", double.class);
        types.put("active", boolean.class);
        types.put("tradeDate", LocalDate.class);
        types.put("we'ird; DROP TABLE x", String.class);
        TABLE = IgniteTable.of(DatasetId.of("trading", "equities", "us", "2026-11-13"), types);
    }

    @Test
    void valuesAreAlwaysBoundAsParameters() {
        FilterSql.Sql s = FilterSql.where(eq("symbol", "x' OR '1'='1"), TABLE);
        assertEquals("F_SYMBOL = ?", s.where());
        assertEquals(List.of("x' OR '1'='1"), s.args());
        assertFalse(s.where().contains("'"));
    }

    @Test
    void namesAreReducedToSafeCharacters() {
        IgniteTable.Column odd = TABLE.columns().get("we'ird; DROP TABLE x");
        assertTrue(odd.name().matches("[A-Z0-9_]+"), odd.name());
        assertTrue(TABLE.table().matches("GX_[A-Z0-9_]+"), TABLE.table());
        assertTrue(TABLE.createTableSql("gx_template", "ATOMIC", 0).contains(odd.name() + " VARCHAR"));
        // a field that is not indexed is simply false; its name never reaches SQL
        FilterSql.Sql s = FilterSql.where(eq("x\"; DROP TABLE y; --", "v"), TABLE);
        assertEquals(FilterSql.FALSE, s.where());
        assertEquals(List.of(), s.args());
    }

    @Test
    void columnTypesFollowTheJavaFields() {
        assertEquals(IgniteTable.SqlType.VARCHAR, TABLE.columns().get("symbol").type());
        assertEquals(IgniteTable.SqlType.BIGINT, TABLE.columns().get("qty").type());
        assertEquals(IgniteTable.SqlType.DOUBLE, TABLE.columns().get("price").type());
        assertEquals(IgniteTable.SqlType.BOOLEAN, TABLE.columns().get("active").type());
        assertEquals(IgniteTable.SqlType.VARCHAR, TABLE.columns().get("tradeDate").type());
    }

    @Test
    void operatorsTranslate() {
        assertEquals("(F_SYMBOL = ?) AND (F_QTY >= ?)", FilterSql.where(and(eq("symbol", "A"), gte("qty", 5)), TABLE).where());
        assertEquals("((F_SYMBOL = ?) IS NOT TRUE)", FilterSql.where(not(eq("symbol", "A")), TABLE).where());
        assertEquals("F_SYMBOL LIKE ? ESCAPE '\\'", FilterSql.where(prefix("symbol", "A%_"), TABLE).where());
        assertEquals(List.of("A\\%\\_%"), FilterSql.where(prefix("symbol", "A%_"), TABLE).args());
        assertEquals("(F_SYMBOL = ? OR F_SYMBOL = ?)", FilterSql.where(in("symbol", "A", "B", 3), TABLE).where());
        assertEquals("F_PRICE > ?", FilterSql.where(gt("price", 9), TABLE).where());
        assertEquals(List.of(9.0), FilterSql.where(gt("price", 9), TABLE).args());
        assertEquals(FilterSql.FALSE, FilterSql.where(eq("qty", 4.0), TABLE).where());
        assertEquals("F_QTY IS NOT NULL", FilterSql.where(lt("qty", "z"), TABLE).where());
        assertEquals(FilterSql.FALSE, FilterSql.where(between("qty", 9, 1), TABLE).where());
        assertEquals(FilterSql.TRUE, FilterSql.where(all(), TABLE).where());
    }

    @Test
    void settingsValidate() {
        Map<String, Object> s = new LinkedHashMap<>();
        s.put("servers", Map.of("node-1", "localhost"));
        s.put("failureMode", "failover");
        assertThrows(io.galaxycache.common.GalaxyException.class, () -> IgniteSettings.from("c", s));
        s.put("backups", 1);
        IgniteSettings ok = IgniteSettings.from("c", s);
        assertEquals(List.of("localhost:10800"), ok.addresses());
        s.put("password", "secret");
        assertFalse(IgniteSettings.from("c", s).toString().contains("secret"));
        assertNotEquals(ok.fingerprint(), IgniteSettings.from("c", Map.of("servers", Map.of("node-1", "localhost"))).fingerprint());
    }
}
