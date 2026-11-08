package io.galaxycache.common.codec;

import io.galaxycache.common.GalaxyException;

/** Built-in codecs by name, as used in {@code @GalaxyDomain(codec = ...)} and in dataset metadata. */
public final class Codecs {

    private static final Codec JSON = new JsonCodec();
    private static final Codec BINARY = new BinaryCodec();
    private static final Codec JAVA = new JavaSerializationCodec();

    private Codecs() {
    }

    public static Codec byName(String name) {
        return switch (name) {
            case "json" -> JSON;
            case "binary" -> BINARY;
            case "java" -> JAVA;
            default -> throw new GalaxyException("Unknown codec '" + name + "'. Built-in codecs: json, binary, java");
        };
    }
}
