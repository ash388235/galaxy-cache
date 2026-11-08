package io.galaxycache.common.codec;

import io.galaxycache.common.GalaxyException;

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.zip.CRC32;

/**
 * Compact binary codec for plain Java domain classes. No field names are written: fields are encoded
 * positionally (superclass first, then by name; records in component order), numbers as varints,
 * dates as epoch numbers, enums as ordinals, collections and maps with their size.
 *
 * <p>Layout of one payload: {@code [format=1][schema fingerprint, 4 bytes][fields...]}. The fingerprint is a
 * CRC of the whole reachable schema (class names, field names and types), so a record written by a different
 * class shape is rejected instead of being silently misread. Changing fields therefore needs a new
 * {@code @GalaxyDomain(version)}, the same rule the cache already enforces.
 *
 * <p>Domain classes need a no-arg constructor (any visibility) or must be records. Static and transient
 * fields are skipped. Fields declared as {@code Object}, an interface or an abstract class carry a type tag.
 */
public final class BinaryCodec implements Codec {

    private static final byte FORMAT = 1;
    private static final ConcurrentMap<Class<?>, Schema> SCHEMAS = new ConcurrentHashMap<>();
    private static final ConcurrentMap<Class<?>, Integer> FINGERPRINTS = new ConcurrentHashMap<>();

    @Override
    public String name() {
        return "binary";
    }

    @Override
    public byte[] encode(Object value) {
        try {
            Out out = new Out(4096);
            out.writeByte(FORMAT);
            out.writeInt(fingerprint(value.getClass()));
            schema(value.getClass()).write(out, value);
            return out.toByteArray();
        } catch (GalaxyException e) {
            throw e;
        } catch (Exception e) {
            throw new GalaxyException("Binary encode failed for " + value.getClass().getName(), e);
        }
    }

    @Override
    public <T> T decode(byte[] bytes, int domainVersion, Class<T> type) {
        In in = new In(bytes);
        byte format = in.readByte();
        if (format != FORMAT) {
            throw new GalaxyException("Not a binary-codec payload (format byte " + format + ") for " + type.getName());
        }
        int stored = in.readInt();
        if (stored != fingerprint(type)) {
            throw new GalaxyException("Binary payload for " + type.getName() + " (domain version " + domainVersion
                    + ") was written with a different class shape. Bump @GalaxyDomain(version) when fields change.");
        }
        try {
            return type.cast(schema(type).read(in));
        } catch (GalaxyException e) {
            throw e;
        } catch (Exception e) {
            throw new GalaxyException("Binary decode failed for " + type.getName(), e);
        }
    }

    // ================================================================== schema

    private static Schema schema(Class<?> c) {
        Schema s = SCHEMAS.get(c);
        if (s == null) {
            s = new Schema(c); // built outside computeIfAbsent: building may need other schemas
            Schema prev = SCHEMAS.putIfAbsent(c, s);
            if (prev != null) {
                s = prev;
            }
        }
        return s;
    }

    /** CRC of the reachable schema starting at {@code root}. */
    static int fingerprint(Class<?> root) {
        return FINGERPRINTS.computeIfAbsent(root, r -> {
            StringBuilder sb = new StringBuilder();
            describe(r, sb, new HashSet<>());
            CRC32 crc = new CRC32();
            crc.update(sb.toString().getBytes(StandardCharsets.UTF_8));
            return (int) crc.getValue();
        });
    }

    private static void describe(Type t, StringBuilder sb, Set<Class<?>> seen) {
        sb.append(t.getTypeName()).append(';');
        if (t instanceof ParameterizedType p) {
            for (Type a : p.getActualTypeArguments()) {
                describe(a, sb, seen);
            }
            return;
        }
        if (t instanceof GenericArrayType g) {
            describe(g.getGenericComponentType(), sb, seen);
            return;
        }
        if (!(t instanceof Class<?> c)) {
            return;
        }
        if (c.isArray()) {
            describe(c.getComponentType(), sb, seen);
            return;
        }
        if (c.isEnum()) {
            sb.append(Arrays.toString(c.getEnumConstants())).append(';');
            return;
        }
        if (!isUserClass(c) || !seen.add(c)) {
            return;
        }
        sb.append('{');
        for (Slot f : slotsOf(c)) {
            sb.append(f.name).append(':');
            describe(f.type, sb, seen);
        }
        sb.append('}');
    }

    private static boolean isUserClass(Class<?> c) {
        return !c.isPrimitive() && !c.isInterface() && !Modifier.isAbstract(c.getModifiers())
                && SCALARS.get(c) == null && !c.getName().startsWith("java.");
    }

    /** A field or record component with its generic type. */
    private record Slot(String name, Type type, Field field) {
    }

    private static List<Slot> slotsOf(Class<?> c) {
        List<Slot> out = new ArrayList<>();
        if (c.isRecord()) {
            for (RecordComponent rc : c.getRecordComponents()) {
                out.add(new Slot(rc.getName(), rc.getGenericType(), null));
            }
            return out;
        }
        List<Class<?>> hierarchy = new ArrayList<>();
        for (Class<?> k = c; k != null && k != Object.class; k = k.getSuperclass()) {
            hierarchy.add(0, k);
        }
        for (Class<?> k : hierarchy) {
            Field[] fs = k.getDeclaredFields();
            Arrays.sort(fs, Comparator.comparing(Field::getName));
            for (Field f : fs) {
                int m = f.getModifiers();
                if (Modifier.isStatic(m) || Modifier.isTransient(m) || f.isSynthetic()) {
                    continue;
                }
                out.add(new Slot(f.getName(), f.getGenericType(), f));
            }
        }
        return out;
    }

    /** How to write and read all fields of one concrete class. */
    private static final class Schema {
        private final Class<?> type;
        private final Field[] fields;
        private final Handler[] handlers;
        private final Constructor<?> ctor;
        private final RecordComponent[] components;
        /** Direct code path per field for the common types (same bytes as the handlers, no boxing or dispatch). */
        private final int[] kinds;

        Schema(Class<?> c) {
            this.type = c;
            List<Slot> slots = slotsOf(c);
            this.handlers = new Handler[slots.size()];
            this.fields = new Field[slots.size()];
            this.kinds = new int[slots.size()];
            for (int i = 0; i < slots.size(); i++) {
                handlers[i] = handlerFor(slots.get(i).type());
                fields[i] = slots.get(i).field();
                kinds[i] = kindOf(slots.get(i).type());
            }
            try {
                if (c.isRecord()) {
                    components = c.getRecordComponents();
                    Class<?>[] types = Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new);
                    ctor = c.getDeclaredConstructor(types);
                    for (RecordComponent rc : components) {
                        rc.getAccessor().setAccessible(true);
                    }
                } else {
                    components = null;
                    ctor = c.getDeclaredConstructor();
                    for (Field f : fields) {
                        f.setAccessible(true);
                    }
                }
                ctor.setAccessible(true);
            } catch (NoSuchMethodException e) {
                throw new GalaxyException(c.getName() + " needs a no-arg constructor for the binary codec", e);
            } catch (RuntimeException e) { // InaccessibleObjectException for JDK internals
                throw new GalaxyException("Binary codec cannot handle type " + c.getName(), e);
            }
        }

        void write(Out out, Object o) throws Exception {
            if (components != null) {
                for (int i = 0; i < components.length; i++) {
                    handlers[i].write(out, components[i].getAccessor().invoke(o));
                }
                return;
            }
            for (int i = 0; i < fields.length; i++) {
                Field f = fields[i];
                switch (kinds[i]) {
                    case K_STRING -> out.writeString((String) f.get(o));
                    case K_DOUBLE -> out.writeLong(Double.doubleToRawLongBits(f.getDouble(o)));
                    case K_LONG -> out.writeVarLong(zig(f.getLong(o)));
                    case K_INT -> out.writeVarLong(zig(f.getInt(o)));
                    case K_BOOLEAN -> out.writeByte(f.getBoolean(o) ? 1 : 0);
                    default -> handlers[i].write(out, f.get(o));
                }
            }
        }

        Object read(In in) throws Exception {
            if (components != null) {
                Object[] args = new Object[components.length];
                for (int i = 0; i < args.length; i++) {
                    args[i] = handlers[i].read(in);
                }
                return ctor.newInstance(args);
            }
            Object o = ctor.newInstance();
            for (int i = 0; i < fields.length; i++) {
                Field f = fields[i];
                switch (kinds[i]) {
                    case K_STRING -> f.set(o, in.readNullableString());
                    case K_DOUBLE -> f.setDouble(o, Double.longBitsToDouble(in.readLong()));
                    case K_LONG -> f.setLong(o, unzig(in.readVarLong()));
                    case K_INT -> f.setInt(o, (int) unzig(in.readVarLong()));
                    case K_BOOLEAN -> f.setBoolean(o, in.readByte() != 0);
                    case K_BIG_DECIMAL -> f.set(o, in.readByte() == 0 ? null : in.readBigDecimal());
                    case K_LOCAL_DATE -> f.set(o, in.readByte() == 0 ? null : LocalDate.ofEpochDay(unzig(in.readVarLong())));
                    default -> f.set(o, handlers[i].read(in));
                }
            }
            return o;
        }

        @Override
        public String toString() {
            return type.getName();
        }
    }

    private static final int K_OTHER = 0;
    private static final int K_STRING = 1;
    private static final int K_DOUBLE = 2;
    private static final int K_LONG = 3;
    private static final int K_INT = 4;
    private static final int K_BOOLEAN = 5;
    private static final int K_BIG_DECIMAL = 6;
    private static final int K_LOCAL_DATE = 7;

    private static int kindOf(Type t) {
        if (t == String.class) {
            return K_STRING;
        }
        if (t == double.class) {
            return K_DOUBLE;
        }
        if (t == long.class) {
            return K_LONG;
        }
        if (t == int.class) {
            return K_INT;
        }
        if (t == boolean.class) {
            return K_BOOLEAN;
        }
        if (t == BigDecimal.class) {
            return K_BIG_DECIMAL;
        }
        return t == LocalDate.class ? K_LOCAL_DATE : K_OTHER;
    }

    // ================================================================== handlers

    private interface Handler {
        void write(Out out, Object v) throws Exception;

        Object read(In in) throws Exception;
    }

    private interface Writer {
        void write(Out out, Object v) throws Exception;
    }

    private interface Reader {
        Object read(In in) throws Exception;
    }

    private static Handler of(Writer w, Reader r) {
        return new Handler() {
            public void write(Out out, Object v) throws Exception {
                w.write(out, v);
            }

            public Object read(In in) throws Exception {
                return r.read(in);
            }
        };
    }

    /** Wraps a non-null handler with a one-byte null marker. */
    private static Handler nullable(Handler h) {
        return of((out, v) -> {
            if (v == null) {
                out.writeByte(0);
            } else {
                out.writeByte(1);
                h.write(out, v);
            }
        }, in -> in.readByte() == 0 ? null : h.read(in));
    }

    // primitives (never null)
    private static final Handler BOOL = of((o, v) -> o.writeByte((Boolean) v ? 1 : 0), in -> in.readByte() != 0);
    private static final Handler BYTE = of((o, v) -> o.writeByte((Byte) v), In::readByte);
    private static final Handler SHORT = of((o, v) -> o.writeVarLong(zig((Short) v)), in -> (short) unzig(in.readVarLong()));
    private static final Handler INT = of((o, v) -> o.writeVarLong(zig((Integer) v)), in -> (int) unzig(in.readVarLong()));
    private static final Handler LONG = of((o, v) -> o.writeVarLong(zig((Long) v)), in -> unzig(in.readVarLong()));
    private static final Handler FLOAT = of((o, v) -> o.writeInt(Float.floatToRawIntBits((Float) v)),
            in -> Float.intBitsToFloat(in.readInt()));
    private static final Handler DOUBLE = of((o, v) -> o.writeLong(Double.doubleToRawLongBits((Double) v)),
            in -> Double.longBitsToDouble(in.readLong()));
    private static final Handler CHAR = of((o, v) -> o.writeVarLong((Character) v), in -> (char) in.readVarLong());

    /** Length + 1 (0 = null), then UTF-8 bytes. */
    private static final Handler STRING = of((o, v) -> {
        if (v == null) {
            o.writeVarLong(0);
            return;
        }
        byte[] b = ((String) v).getBytes(StandardCharsets.UTF_8);
        o.writeVarLong(b.length + 1L);
        o.writeBytes(b);
    }, in -> {
        int n = (int) in.readVarLong();
        return n == 0 ? null : in.readString(n - 1);
    });

    private static final Handler BYTES = of((o, v) -> {
        if (v == null) {
            o.writeVarLong(0);
            return;
        }
        byte[] b = (byte[]) v;
        o.writeVarLong(b.length + 1L);
        o.writeBytes(b);
    }, in -> {
        int n = (int) in.readVarLong();
        return n == 0 ? null : in.readBytes(n - 1);
    });

    private static final Handler BIG_INTEGER = nullable(of((o, v) -> BYTES.write(o, ((BigInteger) v).toByteArray()),
            in -> new BigInteger((byte[]) BYTES.read(in))));
    private static final Handler BIG_DECIMAL = nullable(of((o, v) -> {
        BigDecimal d = (BigDecimal) v;
        o.writeVarLong(zig(d.scale()));
        BYTES.write(o, d.unscaledValue().toByteArray());
    }, in -> {
        int scale = (int) unzig(in.readVarLong());
        return new BigDecimal(new BigInteger((byte[]) BYTES.read(in)), scale);
    }));
    private static final Handler LOCAL_DATE = nullable(of((o, v) -> o.writeVarLong(zig(((LocalDate) v).toEpochDay())),
            in -> LocalDate.ofEpochDay(unzig(in.readVarLong()))));
    private static final Handler LOCAL_TIME = nullable(of((o, v) -> o.writeVarLong(((LocalTime) v).toNanoOfDay()),
            in -> LocalTime.ofNanoOfDay(in.readVarLong())));
    private static final Handler LOCAL_DATE_TIME = nullable(of((o, v) -> {
        LocalDateTime t = (LocalDateTime) v;
        o.writeVarLong(zig(t.toLocalDate().toEpochDay()));
        o.writeVarLong(t.toLocalTime().toNanoOfDay());
    }, in -> LocalDateTime.of(LocalDate.ofEpochDay(unzig(in.readVarLong())), LocalTime.ofNanoOfDay(in.readVarLong()))));
    private static final Handler INSTANT = nullable(of((o, v) -> {
        Instant t = (Instant) v;
        o.writeVarLong(zig(t.getEpochSecond()));
        o.writeVarLong(t.getNano());
    }, in -> Instant.ofEpochSecond(unzig(in.readVarLong()), in.readVarLong())));
    private static final Handler DATE = nullable(of((o, v) -> o.writeVarLong(zig(((Date) v).getTime())),
            in -> new Date(unzig(in.readVarLong()))));
    private static final Handler UUID_H = nullable(of((o, v) -> {
        o.writeLong(((UUID) v).getMostSignificantBits());
        o.writeLong(((UUID) v).getLeastSignificantBits());
    }, in -> new UUID(in.readLong(), in.readLong())));

    /** Fixed-type values, keyed by class. Boxed types get a null marker. */
    private static final Map<Class<?>, Handler> SCALARS = new LinkedHashMap<>();

    static {
        SCALARS.put(boolean.class, BOOL);
        SCALARS.put(byte.class, BYTE);
        SCALARS.put(short.class, SHORT);
        SCALARS.put(int.class, INT);
        SCALARS.put(long.class, LONG);
        SCALARS.put(float.class, FLOAT);
        SCALARS.put(double.class, DOUBLE);
        SCALARS.put(char.class, CHAR);
        SCALARS.put(Boolean.class, nullable(BOOL));
        SCALARS.put(Byte.class, nullable(BYTE));
        SCALARS.put(Short.class, nullable(SHORT));
        SCALARS.put(Integer.class, nullable(INT));
        SCALARS.put(Long.class, nullable(LONG));
        SCALARS.put(Float.class, nullable(FLOAT));
        SCALARS.put(Double.class, nullable(DOUBLE));
        SCALARS.put(Character.class, nullable(CHAR));
        SCALARS.put(String.class, STRING);
        SCALARS.put(byte[].class, BYTES);
        SCALARS.put(BigInteger.class, BIG_INTEGER);
        SCALARS.put(BigDecimal.class, BIG_DECIMAL);
        SCALARS.put(LocalDate.class, LOCAL_DATE);
        SCALARS.put(LocalTime.class, LOCAL_TIME);
        SCALARS.put(LocalDateTime.class, LOCAL_DATE_TIME);
        SCALARS.put(Instant.class, INSTANT);
        SCALARS.put(Date.class, DATE);
        SCALARS.put(UUID.class, UUID_H);
    }

    /** One-byte tags for values whose declared type is Object, an interface or an abstract class. */
    private static final List<Class<?>> TAGGED = List.of(String.class, Long.class, Integer.class, Double.class,
            Boolean.class, BigDecimal.class, LocalDate.class, Instant.class, LocalDateTime.class, Short.class,
            Byte.class, Float.class, Character.class, BigInteger.class, UUID.class, Date.class, LocalTime.class,
            byte[].class, ArrayList.class, LinkedHashMap.class);

    private static final Handler DYNAMIC = new Handler() {
        @Override
        public void write(Out out, Object v) throws Exception {
            if (v == null) {
                out.writeByte(0);
                return;
            }
            Class<?> c = v instanceof List<?> ? ArrayList.class : v instanceof Map<?, ?> ? LinkedHashMap.class
                    : v.getClass();
            int tag = TAGGED.indexOf(c);
            if (tag >= 0) {
                out.writeByte(tag + 2);
                taggedHandler(c).write(out, v);
            } else {
                out.writeByte(1);
                STRING.write(out, c.getName());
                handlerFor(c).write(out, v);
            }
        }

        @Override
        public Object read(In in) throws Exception {
            int tag = in.readByte() & 0xFF;
            if (tag == 0) {
                return null;
            }
            Class<?> c = tag == 1 ? Class.forName((String) STRING.read(in), false,
                    Thread.currentThread().getContextClassLoader() != null
                            ? Thread.currentThread().getContextClassLoader() : BinaryCodec.class.getClassLoader())
                    : TAGGED.get(tag - 2);
            return (tag == 1 ? handlerFor(c) : taggedHandler(c)).read(in);
        }
    };

    private static Handler taggedHandler(Class<?> c) {
        if (c == ArrayList.class) {
            return collection(ArrayList.class, DYNAMIC);
        }
        if (c == LinkedHashMap.class) {
            return map(LinkedHashMap.class, DYNAMIC, DYNAMIC);
        }
        return SCALARS.get(c);
    }

    private static final ConcurrentMap<Type, Handler> HANDLERS = new ConcurrentHashMap<>();

    static Handler handlerFor(Type t) {
        Handler h = HANDLERS.get(t);
        if (h == null) {
            h = createHandler(t);
            HANDLERS.putIfAbsent(t, h);
        }
        return h;
    }

    private static Handler createHandler(Type t) {
        if (t instanceof ParameterizedType p && p.getRawType() instanceof Class<?> raw) {
            Type[] a = p.getActualTypeArguments();
            if (Map.class.isAssignableFrom(raw)) {
                return map(raw, handlerFor(a[0]), handlerFor(a[1]));
            }
            if (Collection.class.isAssignableFrom(raw)) {
                return collection(raw, handlerFor(a[0]));
            }
            return handlerFor(raw);
        }
        if (t instanceof GenericArrayType g) {
            Type comp = g.getGenericComponentType();
            Class<?> raw = comp instanceof ParameterizedType p ? (Class<?>) p.getRawType() : Object.class;
            return array(raw, handlerFor(comp));
        }
        if (!(t instanceof Class<?> c)) {
            return DYNAMIC; // type variables and wildcards
        }
        Handler scalar = SCALARS.get(c);
        if (scalar != null) {
            return scalar;
        }
        if (c.isEnum()) {
            Object[] constants = c.getEnumConstants();
            return of((o, v) -> o.writeVarLong(v == null ? 0 : ((Enum<?>) v).ordinal() + 1L), in -> {
                int n = (int) in.readVarLong();
                return n == 0 ? null : constants[n - 1];
            });
        }
        if (c.isArray()) {
            return array(c.getComponentType(), handlerFor(c.getComponentType()));
        }
        if (Map.class.isAssignableFrom(c)) {
            return map(c, DYNAMIC, DYNAMIC);
        }
        if (Collection.class.isAssignableFrom(c)) {
            return collection(c, DYNAMIC);
        }
        if (c == Object.class || c.isInterface() || Modifier.isAbstract(c.getModifiers())) {
            return DYNAMIC;
        }
        return object(c);
    }

    /** Nested object: 0 = null, 1 = exactly the declared class, 2 = a subclass whose name follows. */
    private static Handler object(Class<?> declared) {
        return of((o, v) -> {
            if (v == null) {
                o.writeByte(0);
            } else if (v.getClass() == declared) {
                o.writeByte(1);
                schema(declared).write(o, v);
            } else {
                o.writeByte(2);
                STRING.write(o, v.getClass().getName());
                schema(v.getClass()).write(o, v);
            }
        }, in -> {
            int mode = in.readByte();
            if (mode == 0) {
                return null;
            }
            Class<?> c = mode == 1 ? declared : Class.forName((String) STRING.read(in), false, declared.getClassLoader());
            return schema(c).read(in);
        });
    }

    /** Size + 1 (0 = null), then the elements. */
    private static Handler collection(Class<?> declared, Handler element) {
        return of((o, v) -> {
            if (v == null) {
                o.writeVarLong(0);
                return;
            }
            Collection<?> c = (Collection<?>) v;
            o.writeVarLong(c.size() + 1L);
            for (Object e : c) {
                element.write(o, e);
            }
        }, in -> {
            int n = (int) in.readVarLong();
            if (n == 0) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Collection<Object> c = (Collection<Object>) newCollection(declared, n - 1);
            for (int i = 0; i < n - 1; i++) {
                c.add(element.read(in));
            }
            return c;
        });
    }

    private static Handler map(Class<?> declared, Handler key, Handler value) {
        return of((o, v) -> {
            if (v == null) {
                o.writeVarLong(0);
                return;
            }
            Map<?, ?> m = (Map<?, ?>) v;
            o.writeVarLong(m.size() + 1L);
            for (Map.Entry<?, ?> e : m.entrySet()) {
                key.write(o, e.getKey());
                value.write(o, e.getValue());
            }
        }, in -> {
            int n = (int) in.readVarLong();
            if (n == 0) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<Object, Object> m = (Map<Object, Object>) newMap(declared, n - 1);
            for (int i = 0; i < n - 1; i++) {
                Object k = key.read(in);
                m.put(k, value.read(in));
            }
            return m;
        });
    }

    private static Handler array(Class<?> component, Handler element) {
        return of((o, v) -> {
            if (v == null) {
                o.writeVarLong(0);
                return;
            }
            int n = Array.getLength(v);
            o.writeVarLong(n + 1L);
            for (int i = 0; i < n; i++) {
                element.write(o, Array.get(v, i));
            }
        }, in -> {
            int n = (int) in.readVarLong();
            if (n == 0) {
                return null;
            }
            Object a = Array.newInstance(component, n - 1);
            for (int i = 0; i < n - 1; i++) {
                Array.set(a, i, element.read(in));
            }
            return a;
        });
    }

    private static Collection<?> newCollection(Class<?> c, int size) throws Exception {
        if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers())) {
            try {
                return (Collection<?>) c.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                // fall through to the interface defaults (e.g. immutable list types)
            }
        }
        if (SortedSet.class.isAssignableFrom(c) || NavigableSet.class.isAssignableFrom(c)) {
            return new TreeSet<>();
        }
        if (Set.class.isAssignableFrom(c)) {
            return new LinkedHashSet<>(Math.max(16, size * 2));
        }
        if (Queue.class.isAssignableFrom(c)) {
            return new ArrayDeque<>(Math.max(1, size));
        }
        return new ArrayList<>(size);
    }

    private static Map<?, ?> newMap(Class<?> c, int size) throws Exception {
        if (!c.isInterface() && !Modifier.isAbstract(c.getModifiers())) {
            try {
                return (Map<?, ?>) c.getDeclaredConstructor().newInstance();
            } catch (ReflectiveOperationException | RuntimeException ignored) {
                // fall through
            }
        }
        if (SortedMap.class.isAssignableFrom(c) || NavigableMap.class.isAssignableFrom(c)) {
            return new TreeMap<>();
        }
        if (ConcurrentMap.class.isAssignableFrom(c)) {
            return new ConcurrentHashMap<>(Math.max(16, size * 2));
        }
        return new LinkedHashMap<>(Math.max(16, size * 2));
    }

    private static long zig(long v) {
        return (v << 1) ^ (v >> 63);
    }

    private static long unzig(long v) {
        return (v >>> 1) ^ -(v & 1);
    }

    // ================================================================== byte buffers

    private static final java.lang.invoke.VarHandle LONG_BE = java.lang.invoke.MethodHandles
            .byteArrayViewVarHandle(long[].class, java.nio.ByteOrder.BIG_ENDIAN);

    private static final class Out {
        private byte[] buf;
        private int pos;

        Out(int initial) {
            buf = new byte[initial];
        }

        private void ensure(int n) {
            if (pos + n > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, pos + n));
            }
        }

        void writeByte(int b) {
            ensure(1);
            buf[pos++] = (byte) b;
        }

        void writeBytes(byte[] b) {
            ensure(b.length);
            System.arraycopy(b, 0, buf, pos, b.length);
            pos += b.length;
        }

        void writeInt(int v) {
            ensure(4);
            for (int s = 24; s >= 0; s -= 8) {
                buf[pos++] = (byte) (v >>> s);
            }
        }

        void writeLong(long v) {
            ensure(8);
            LONG_BE.set(buf, pos, v);
            pos += 8;
        }

        /** Same bytes as the STRING handler: length + 1 (0 = null), then UTF-8. */
        void writeString(String s) {
            if (s == null) {
                writeVarLong(0);
                return;
            }
            byte[] b = s.getBytes(StandardCharsets.UTF_8);
            writeVarLong(b.length + 1L);
            writeBytes(b);
        }

        /** Unsigned LEB128. */
        void writeVarLong(long v) {
            ensure(10);
            while ((v & ~0x7FL) != 0) {
                buf[pos++] = (byte) ((v & 0x7F) | 0x80);
                v >>>= 7;
            }
            buf[pos++] = (byte) v;
        }

        byte[] toByteArray() {
            return Arrays.copyOf(buf, pos);
        }
    }

    private static final class In {
        private final byte[] buf;
        private int pos;

        In(byte[] buf) {
            this.buf = buf;
        }

        private void need(int n) {
            if (pos + n > buf.length) {
                throw new GalaxyException("Binary payload truncated at byte " + pos);
            }
        }

        byte readByte() {
            need(1);
            return buf[pos++];
        }

        int readInt() {
            need(4);
            int v = 0;
            for (int i = 0; i < 4; i++) {
                v = (v << 8) | (buf[pos++] & 0xFF);
            }
            return v;
        }

        long readLong() {
            need(8);
            long v = (long) LONG_BE.get(buf, pos);
            pos += 8;
            return v;
        }

        String readNullableString() {
            int n = (int) readVarLong();
            return n == 0 ? null : readString(n - 1);
        }

        /** Same bytes as the BigDecimal handler; values up to 8 bytes are built from a long, without arrays. */
        BigDecimal readBigDecimal() {
            int scale = (int) unzig(readVarLong());
            int len = (int) readVarLong() - 1;
            if (len < 1 || len > 8) {
                return new BigDecimal(new BigInteger(readBytes(len)), scale);
            }
            need(len);
            long v = buf[pos]; // sign-extends
            for (int i = 1; i < len; i++) {
                v = (v << 8) | (buf[pos + i] & 0xFF);
            }
            pos += len;
            return BigDecimal.valueOf(v, scale);
        }

        long readVarLong() {
            if (pos < buf.length && buf[pos] >= 0) {
                return buf[pos++]; // most values fit in one byte
            }
            long v = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                byte b = readByte();
                v |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return v;
                }
            }
            throw new GalaxyException("Malformed varint at byte " + pos);
        }

        byte[] readBytes(int n) {
            need(n);
            byte[] b = Arrays.copyOfRange(buf, pos, pos + n);
            pos += n;
            return b;
        }

        String readString(int n) {
            need(n);
            String s = new String(buf, pos, n, StandardCharsets.UTF_8);
            pos += n;
            return s;
        }
    }
}
