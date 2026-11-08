package io.galaxycache.common.codec;

/**
 * Turns domain objects into bytes and back. Implementations must be thread-safe.
 * The domain version is stored by the caller next to the bytes, so a codec can choose
 * a different decoding path for older versions.
 */
public interface Codec {

    /** Short identifier written to dataset metadata, for example {@code json}. */
    String name();

    byte[] encode(Object value);

    <T> T decode(byte[] bytes, int domainVersion, Class<T> type);
}
