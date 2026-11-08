package io.galaxycache.common.codec;

import io.galaxycache.common.DatasetId;
import io.galaxycache.common.GalaxyException;
import io.galaxycache.common.query.Filter;
import io.galaxycache.common.query.FilterLimits;
import io.galaxycache.common.query.Filters;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

/** Stored bytes and names are untrusted input: anyone able to write to the store can plant them. */
@Timeout(60)
class HostileDataTest {

    static class AnyHolder {
        Object anything;
    }

    static class Base {
        int x;
    }

    static class BaseHolder {
        Base b;
    }

    static class Unrelated {
        int y;
    }

    static class ListHolder {
        List<String> list;
    }

    static class MapHolder {
        Map<String, String> map;
    }

    static class ArrayHolder {
        int[] array;
    }

    static class StringHolder {
        String s;
    }

    static class Chain {
        Chain next;
    }

    /** Format byte and schema fingerprint of an empty object of this type. */
    private static byte[] prefix(Object emptyObject) {
        return Arrays.copyOf(new BinaryCodec().encode(emptyObject), 5);
    }

    private static byte[] payload(Object emptyObject, Object... parts) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(prefix(emptyObject));
        for (Object p : parts) {
            if (p instanceof byte[] b) {
                out.write(b);
            } else if (p instanceof String s) {
                byte[] b = s.getBytes(StandardCharsets.UTF_8);
                out.write(varint(b.length + 1L));
                out.write(b);
            } else {
                out.write((Integer) p);
            }
        }
        return out.toByteArray();
    }

    private static byte[] varint(long v) {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        while ((v & ~0x7FL) != 0) {
            b.write((int) ((v & 0x7F) | 0x80));
            v >>>= 7;
        }
        b.write((int) v);
        return b.toByteArray();
    }

    @Test
    void classNamesOutsideTheDomainPackageAreNeverLoaded() throws Exception {
        BinaryCodec codec = new BinaryCodec();
        for (String gadget : List.of("java.net.URL", "javax.script.ScriptEngineManager", "com.sun.rowset.JdbcRowSetImpl",
                "org.springframework.context.support.ClassPathXmlApplicationContext", "java.lang.Runtime", "[Ljava.lang.Object;")) {
            byte[] attack = payload(new AnyHolder(), 1, gadget); // tag 1 = "a class name follows"
            GalaxyException e = assertThrows(GalaxyException.class, () -> codec.decode(attack, 1, AnyHolder.class), gadget);
            assertTrue(rootMessage(e).contains("outside the domain package"), rootMessage(e));
        }
    }

    @Test
    void aSubclassMarkerCannotSmuggleAnUnrelatedClass() throws Exception {
        BinaryCodec codec = new BinaryCodec();
        byte[] outside = payload(new BaseHolder(), 2, "java.util.ArrayList");
        assertTrue(rootMessage(assertThrows(GalaxyException.class, () -> codec.decode(outside, 1, BaseHolder.class))).contains("outside"));
        byte[] unrelated = payload(new BaseHolder(), 2, Unrelated.class.getName());
        assertTrue(rootMessage(assertThrows(GalaxyException.class, () -> codec.decode(unrelated, 1, BaseHolder.class))).contains("does not fit"));
    }

    @Test
    void hugeClaimedSizesFailWithoutAllocating() throws Exception {
        BinaryCodec codec = new BinaryCodec();
        byte[] huge = varint(Integer.MAX_VALUE - 1L);
        for (Object[] c : List.of(new Object[]{new ListHolder(), ListHolder.class}, new Object[]{new MapHolder(), MapHolder.class},
                new Object[]{new ArrayHolder(), ArrayHolder.class}, new Object[]{new StringHolder(), StringHolder.class})) {
            byte[] attack = payload(c[0], huge);
            assertThrows(GalaxyException.class, () -> codec.decode(attack, 1, (Class<?>) c[1]), c[1].toString());
        }
    }

    @Test
    void deepNestingFailsCleanly() throws Exception {
        byte[] chain = new byte[2_000_000];
        Arrays.fill(chain, (byte) 1); // "next object follows", over and over
        byte[] attack = payload(new Chain(), chain);
        assertTrue(rootMessage(assertThrows(GalaxyException.class, () -> new BinaryCodec().decode(attack, 1, Chain.class))).contains("too deeply"));
    }

    static class Rich {
        String id = "T-1";
        long qty = 123456789L;
        BigDecimal px = new BigDecimal("1234.5678");
        List<String> tags = new ArrayList<>(List.of("a", "bb", "ccc"));
        Map<String, Integer> m = new LinkedHashMap<>(Map.of("k", 1));
        Object any = "text";
        Chain chain = new Chain();
    }

    @Test
    void truncatedAndCorruptedPayloadsOnlyEverThrowOrdinaryExceptions() {
        BinaryCodec codec = new BinaryCodec();
        byte[] good = codec.encode(new Rich());
        for (int n = 0; n < good.length; n++) {
            byte[] cut = Arrays.copyOf(good, n);
            assertThrows(RuntimeException.class, () -> codec.decode(cut, 1, Rich.class), "cut at " + n);
        }
        Random r = new Random(7);
        for (int i = 0; i < 20_000; i++) {
            byte[] bad = good.clone();
            for (int flips = 1 + r.nextInt(4); flips > 0; flips--) {
                bad[5 + r.nextInt(bad.length - 5)] = (byte) r.nextInt(256);
            }
            try {
                codec.decode(bad, 1, Rich.class);
            } catch (RuntimeException expected) {
                // fine: only an Error (out of memory, stack overflow) would fail this test
            }
        }
    }

    // ---- java serialization

    private static byte[] serialize(Object o) {
        return new JavaSerializationCodec().encode(o);
    }

    @Test
    void javaGadgetClassesAreRejected() throws Exception {
        JavaSerializationCodec codec = new JavaSerializationCodec();
        HashMap<Object, Object> urlDns = new HashMap<>(); // the classic URLDNS gadget chain
        urlDns.put(new URL("http://localhost/"), "x");
        assertThrows(GalaxyException.class, () -> codec.decode(serialize(urlDns), 1, HashMap.class));

        HashMap<Object, Object> uri = new HashMap<>();
        uri.put(URI.create("http://localhost/"), "x"); // java.net: not on the allow-list either
        assertThrows(GalaxyException.class, () -> codec.decode(serialize(uri), 1, HashMap.class));

        HashMap<Object, Object> lambda = new HashMap<>();
        lambda.put("f", (java.util.function.Supplier<String> & Serializable) () -> "x"); // SerializedLambda
        assertThrows(GalaxyException.class, () -> codec.decode(serialize(lambda), 1, HashMap.class));
    }

    @Test
    void javaStreamsNestedTooDeeplyAreRejected() {
        ArrayList<Object> deep = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            ArrayList<Object> outer = new ArrayList<>();
            outer.add(deep);
            deep = outer;
        }
        byte[] bytes = serialize(deep);
        assertThrows(GalaxyException.class, () -> new JavaSerializationCodec().decode(bytes, 1, ArrayList.class));
    }

    @Test
    void javaStreamWithAHugeArrayIsRejected() {
        byte[] bytes = serialize(new long[1024]); // the array header is what matters
        // array length field sits at the end of the header, just before the 8 KB of elements
        int lengthAt = bytes.length - 1024 * 8 - 4;
        bytes[lengthAt] = 0x7F;
        byte[] attack = bytes;
        assertThrows(GalaxyException.class, () -> new JavaSerializationCodec().decode(attack, 1, long[].class));
    }

    // ---- names that become paths

    @Test
    void datasetPartsThatAreDangerousAsFolderNamesAreRefused() {
        for (String bad : new String[]{"..", ".", "../x", "..\\x", "a/b", "a\\b", "C:", "a:b", "a:stream", "CON", "con", "nul", "AUX",
                "aux.txt", "COM1", "lpt9.log", "PRN", "a.", "abc..", "", " a", "a b", "a\u0000b", "a\nb", "%2e%2e", "x".repeat(129)}) {
            assertThrows(IllegalArgumentException.class, () -> DatasetId.of(bad, "d", "s", "2026-11-08"), "accepted: " + bad);
            assertFalse(DatasetId.validSegment(bad), bad);
        }
        for (String ok : new String[]{"trading", "a.b", "A-1_b.c", "com10", "console", "nullable", "x".repeat(128)}) {
            assertTrue(DatasetId.validSegment(ok), ok);
        }
    }

    @Test
    void malformedDatasetKeysAreRefused() {
        for (String bad : new String[]{"a/b/c", "a/b/c/d/", "/a/b/c/d", "a/b/c/d/e", "../b/c/d", "a/b/../d", ""}) {
            assertThrows(IllegalArgumentException.class, () -> DatasetId.parse(bad), bad);
        }
    }

    // ---- query limits

    @Test
    void filterLimitsAllowRealQueriesAndRefuseExpensiveOnes() {
        FilterLimits.check(Filters.and(Filters.eq("a", 1), Filters.or(Filters.in("b", 1, 2, 3), Filters.not(Filters.eq("c", 1))),
                Filters.between("d", 1, 9)));
        FilterLimits.check(Filters.all());

        Filter deep = Filters.eq("a", 1);
        for (int i = 0; i < FilterLimits.MAX_DEPTH; i++) {
            deep = Filters.and(deep);
        }
        Filter tooDeep = deep;
        assertThrows(GalaxyException.class, () -> FilterLimits.check(tooDeep));
        assertThrows(GalaxyException.class, () -> FilterLimits.check(null));
        assertThrows(GalaxyException.class, () -> FilterLimits.check(new Filter(Filter.Op.NOT, null, null, null)));
        assertThrows(GalaxyException.class, () -> FilterLimits.check(new Filter(Filter.Op.BETWEEN, "a", List.of(1), null)));
        assertThrows(GalaxyException.class, () -> FilterLimits.check(new Filter(Filter.Op.EQ, null, List.of(1), null)));
        Object[] many = new Object[FilterLimits.MAX_VALUES + 1];
        Arrays.fill(many, 1);
        assertThrows(GalaxyException.class, () -> FilterLimits.check(new Filter(Filter.Op.IN, "a", Arrays.asList(many), null)));
    }

    private static String rootMessage(Throwable t) {
        return String.valueOf(t.getMessage());
    }
}
