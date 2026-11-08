package io.galaxycache.common.codec;

import io.galaxycache.common.Json;

/** Default codec: JSON via the shared Jackson mapper. Fields are read directly, no getters needed. */
public final class JsonCodec implements Codec {

    private static final com.fasterxml.jackson.databind.ObjectMapper FIELD_MAPPER = Json.MAPPER.copy()
            .setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.FIELD,
                    com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.ANY)
            .setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.GETTER,
                    com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE)
            .setVisibility(com.fasterxml.jackson.annotation.PropertyAccessor.IS_GETTER,
                    com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility.NONE);

    @Override
    public String name() {
        return "json";
    }

    @Override
    public byte[] encode(Object value) {
        try {
            return FIELD_MAPPER.writeValueAsBytes(value);
        } catch (Exception e) {
            throw new io.galaxycache.common.GalaxyException("Encode failed for " + value.getClass().getName(), e);
        }
    }

    @Override
    public <T> T decode(byte[] bytes, int domainVersion, Class<T> type) {
        try {
            return FIELD_MAPPER.readValue(bytes, type);
        } catch (Exception e) {
            throw new io.galaxycache.common.GalaxyException("Decode failed for " + type.getName()
                    + " (domain version " + domainVersion + ")", e);
        }
    }
}
