package io.galaxycache.common.domain;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a class that can be stored in Galaxy Cache. The version is written next to every record
 * and is used to pick the right deserialization when domain classes evolve.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface GalaxyDomain {
    /** Logical domain name; defaults to the simple class name. */
    String name() default "";

    /** Domain version, 1..32767. */
    int version() default 1;

    /**
     * Storage format for this domain: {@code "json"}, {@code "java"} (standard java.io serialization; the class
     * tree must implement Serializable) or {@code "binary"} (compact, positional; see {@code BinaryCodec}).
     * Empty means the client's codec, which is JSON unless the client was built with another.
     */
    String codec() default "";

    /**
     * For a domain class renamed to keep reading old data (for example {@code TradeV1} during a migration): the fully
     * qualified name the class had when its records were written. The binary codec's stored schema fingerprint
     * includes class names, so without this a renamed class could not read them. Nested classes follow the outer one.
     */
    String originalClass() default "";
}
