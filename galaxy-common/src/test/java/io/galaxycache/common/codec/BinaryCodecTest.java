package io.galaxycache.common.codec;

import io.galaxycache.common.GalaxyException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BinaryCodecTest {

    enum Side { BUY, SELL }

    record Money(BigDecimal amount, String currency) {
    }

    static class Address {
        String city;
        String country;
        Address next; // recursive type

        Address() {
        }

        Address(String city, String country) {
            this.city = city;
            this.country = country;
        }
    }

    static class Special extends Address {
        int floor;

        Special() {
        }
    }

    static class Leg {
        int number;
        double rate;
        List<Money> flows = new ArrayList<>();
    }

    static class Txn {
        String id;
        long quantity;
        int count;
        double price;
        float ratio;
        boolean live;
        char flag;
        short small;
        Long boxed;
        Integer nullInt;
        Side side;
        Side noSide;
        BigDecimal notional;
        LocalDate tradeDate;
        LocalDateTime created;
        Instant at;
        UUID uuid;
        byte[] blob;
        int[] ints;
        String nothing;
        Address address;
        Address polymorphic;
        List<Leg> legs = new ArrayList<>();
        Map<String, String> attributes = new LinkedHashMap<>();
        TreeMap<String, Integer> sorted = new TreeMap<>();
        Set<String> tags;
        Object anything;
        Map<String, Object> mixed = new LinkedHashMap<>();
        transient String skipped = "transient";
        Money money;
    }

    private static Txn sample() {
        Txn t = new Txn();
        t.id = "TX-1";
        t.quantity = -9_000_000_000L;
        t.count = 42;
        t.price = 101.25;
        t.ratio = 0.5f;
        t.live = true;
        t.flag = 'Z';
        t.small = -7;
        t.boxed = 5L;
        t.side = Side.SELL;
        t.notional = new BigDecimal("-12345678901234567890.123456");
        t.tradeDate = LocalDate.of(2026, 11, 7);
        t.created = LocalDateTime.of(2026, 11, 7, 15, 51, 7, 123_456_789);
        t.at = Instant.parse("2026-11-07T15:51:07.987654321Z");
        t.uuid = UUID.randomUUID();
        t.blob = new byte[]{1, 2, 3};
        t.ints = new int[]{1, -2, 300};
        t.address = new Address("Zürich", "CH");
        t.address.next = new Address("Paris", "FR");
        Special s = new Special();
        s.city = "NYC";
        s.floor = 51;
        t.polymorphic = s;
        for (int i = 0; i < 3; i++) {
            Leg l = new Leg();
            l.number = i;
            l.rate = i * 0.25;
            l.flows.add(new Money(BigDecimal.valueOf(i * 100L, 2), "USD"));
            t.legs.add(l);
        }
        for (int i = 0; i < 100; i++) {
            t.attributes.put("k" + i, "v" + i);
        }
        t.sorted.put("b", 2);
        t.sorted.put("a", 1);
        t.tags = Set.of("x");
        t.anything = 3.5;
        t.mixed.put("s", "str");
        t.mixed.put("n", 7L);
        t.mixed.put("d", LocalDate.of(2020, 1, 1));
        t.mixed.put("list", List.of("a", 1L));
        t.mixed.put("null", null);
        t.money = new Money(BigDecimal.TEN, "EUR");
        return t;
    }

    @Test
    void roundTripsEveryKindOfField() {
        BinaryCodec codec = new BinaryCodec();
        Txn in = sample();
        in.skipped = "changed";
        Txn out = codec.decode(codec.encode(in), 1, Txn.class);

        assertEquals(in.id, out.id);
        assertEquals(in.quantity, out.quantity);
        assertEquals(in.count, out.count);
        assertEquals(in.price, out.price);
        assertEquals(in.ratio, out.ratio);
        assertEquals(in.live, out.live);
        assertEquals(in.flag, out.flag);
        assertEquals(in.small, out.small);
        assertEquals(in.boxed, out.boxed);
        assertNull(out.nullInt);
        assertEquals(Side.SELL, out.side);
        assertNull(out.noSide);
        assertEquals(in.notional, out.notional);
        assertEquals(in.tradeDate, out.tradeDate);
        assertEquals(in.created, out.created);
        assertEquals(in.at, out.at);
        assertEquals(in.uuid, out.uuid);
        assertArrayEquals(in.blob, out.blob);
        assertArrayEquals(in.ints, out.ints);
        assertNull(out.nothing);
        assertEquals("Zürich", out.address.city);
        assertEquals("Paris", out.address.next.city);
        assertNull(out.address.next.next);
        Special sp = assertInstanceOf(Special.class, out.polymorphic);
        assertEquals(51, sp.floor);
        assertEquals("NYC", sp.city);
        assertEquals(3, out.legs.size());
        assertEquals(0.5, out.legs.get(2).rate);
        assertEquals(new Money(new BigDecimal("2.00"), "USD"), out.legs.get(2).flows.get(0));
        assertEquals(in.attributes, out.attributes);
        assertEquals(List.of("k0", "k1"), new ArrayList<>(out.attributes.keySet()).subList(0, 2)); // order kept
        assertEquals(in.sorted, out.sorted);
        assertEquals(in.tags, out.tags);
        assertEquals(3.5, out.anything);
        assertEquals(in.mixed, out.mixed);
        assertEquals("transient", out.skipped); // not stored: keeps the constructor default
        assertEquals(in.money, out.money);
    }

    @Test
    void isMuchSmallerThanJson() {
        Txn t = sample();
        int binary = new BinaryCodec().encode(t).length;
        t.polymorphic = null; // the JSON codec cannot read subclasses back, keep the comparison fair
        int json = new JsonCodec().encode(t).length;
        assertTrue(binary < json * 0.7, "binary " + binary + " vs json " + json);
    }

    static class V1 {
        String a;
        long b;
    }

    static class V2 {
        String a;
        long b;
        String added;
    }

    @Test
    void rejectsPayloadOfDifferentClassShape() {
        BinaryCodec codec = new BinaryCodec();
        V1 v = new V1();
        v.a = "x";
        byte[] bytes = codec.encode(v);
        GalaxyException e = assertThrows(GalaxyException.class, () -> codec.decode(bytes, 1, V2.class));
        assertTrue(e.getMessage().contains("different class shape"));
        assertThrows(GalaxyException.class, () -> codec.decode("{\"a\":1}".getBytes(), 1, V1.class));
    }

    @Test
    void sameObjectEncodesToSameBytes() {
        Txn t = sample();
        t.tags = null; // Set.of iteration order is randomised per JVM run, not per call; keep it simple
        BinaryCodec codec = new BinaryCodec();
        assertArrayEquals(codec.encode(t), codec.encode(t));
        assertTrue(Objects.nonNull(codec.decode(codec.encode(t), 1, Txn.class)));
    }

    static class Edges {
        String empty = "";
        String nothing;
        String unicode = "Zürich ₹ 取引";
        long minLong = Long.MIN_VALUE;
        long maxLong = Long.MAX_VALUE;
        int minInt = Integer.MIN_VALUE;
        double nan = Double.NaN;
        double negZero = -0.0;
        boolean flag = true;
        java.time.LocalDate noDate;
        java.time.LocalDate early = java.time.LocalDate.of(1600, 2, 29);
        BigDecimal noAmount;
        BigDecimal zero = new BigDecimal("0.00");
        BigDecimal negative = new BigDecimal("-1.5");
        BigDecimal negativeScale = new BigDecimal("1E+5");
        BigDecimal longMin = new BigDecimal(Long.MIN_VALUE).movePointLeft(4);
        BigDecimal justOver8Bytes = new BigDecimal(java.math.BigInteger.ONE.shiftLeft(64)).movePointLeft(2);
        BigDecimal huge = new BigDecimal("-123456789012345678901234567890.123456789");
    }

    @Test
    void directFieldPathsHandleEdgeValues() {
        BinaryCodec codec = new BinaryCodec();
        Edges in = new Edges();
        in.minLong = Long.MIN_VALUE + 1; // differ from the constructor default, so the value must really be decoded
        in.flag = false;
        in.empty = "changed";
        in.zero = new BigDecimal("0.000");
        Edges out = new Edges();
        out = codec.decode(codec.encode(in), 1, Edges.class);
        assertEquals(in.empty, out.empty);
        assertNull(out.nothing);
        assertEquals(in.unicode, out.unicode);
        assertEquals(in.minLong, out.minLong);
        assertEquals(in.maxLong, out.maxLong);
        assertEquals(in.minInt, out.minInt);
        assertTrue(Double.isNaN(out.nan));
        assertEquals(Double.doubleToRawLongBits(-0.0), Double.doubleToRawLongBits(out.negZero));
        assertFalse(out.flag);
        assertNull(out.noDate);
        assertEquals(in.early, out.early);
        assertNull(out.noAmount);
        for (java.util.function.Function<Edges, BigDecimal> g : List.<java.util.function.Function<Edges, BigDecimal>>of(
                e -> e.zero, e -> e.negative, e -> e.negativeScale, e -> e.longMin, e -> e.justOver8Bytes, e -> e.huge)) {
            assertEquals(g.apply(in), g.apply(out)); // BigDecimal.equals: same value and same scale
        }
    }}
