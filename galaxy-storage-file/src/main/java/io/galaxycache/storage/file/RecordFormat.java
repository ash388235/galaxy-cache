package io.galaxycache.storage.file;

import io.galaxycache.common.GalaxyException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

/**
 * Binary layout of one record inside a page.
 *
 * <pre>
 * offset size field
 *  0      1   magic 0xA7 (a 0x00 byte here means: rest of the page is padding)
 *  1      1   type  1 = PUT, 2 = DELETE
 *  2      2   schemaVersion (domain version)
 *  4      2   idLength (UTF-8 bytes)
 *  6      4   valueLength
 * 10      4   crc32 over bytes [1,10) + id + value
 * 14      n   id bytes, then value bytes
 * </pre>
 */
final class RecordFormat {

    static final byte MAGIC = (byte) 0xA7;
    static final byte PUT = 1;
    static final byte DELETE = 2;
    static final int HEADER = 14;
    static final int MAX_ID_BYTES = 1024;

    private RecordFormat() {
    }

    static byte[] encode(byte type, String id, int schemaVersion, byte[] value) {
        byte[] idBytes = id.getBytes(StandardCharsets.UTF_8);
        if (idBytes.length > MAX_ID_BYTES) {
            throw new GalaxyException("Id longer than " + MAX_ID_BYTES + " bytes: " + id.substring(0, 32) + "...");
        }
        if (schemaVersion < 0 || schemaVersion > Short.MAX_VALUE) {
            throw new GalaxyException("schemaVersion out of range: " + schemaVersion);
        }
        ByteBuffer b = ByteBuffer.allocate(HEADER + idBytes.length + value.length);
        b.put(MAGIC).put(type).putShort((short) schemaVersion).putShort((short) idBytes.length).putInt(value.length);
        b.putInt(0); // crc placeholder
        b.put(idBytes).put(value);
        byte[] out = b.array();
        ByteBuffer.wrap(out, 10, 4).putInt(crc(out, 0, out.length));
        return out;
    }

    static int crc(byte[] rec, int start, int total) {
        CRC32 c = new CRC32();
        c.update(rec, start + 1, 9);
        c.update(rec, start + HEADER, total - HEADER);
        return (int) c.getValue();
    }

    /** Total size of the record whose header starts at {@code pos}, or -1 if the header is invalid. */
    static int totalLength(ByteBuffer buf, int pos) {
        if (buf.get(pos) != MAGIC) {
            return -1;
        }
        int idLen = Short.toUnsignedInt(buf.getShort(pos + 4));
        int valLen = buf.getInt(pos + 6);
        if (idLen == 0 || idLen > MAX_ID_BYTES || valLen < 0) {
            return -1;
        }
        return HEADER + idLen + valLen;
    }

    /** Decoded view of a record. */
    record Decoded(byte type, int schemaVersion, String id, byte[] value) {
    }

    /** Decodes and verifies the record at {@code pos}; returns null if the bytes are not a valid record. */
    static Decoded decode(ByteBuffer buf, int pos, int available) {
        if (available < HEADER) {
            return null;
        }
        int total = totalLength(buf, pos);
        if (total < 0 || total > available) {
            return null;
        }
        byte[] rec = new byte[total];
        buf.get(pos, rec, 0, total);
        int stored = ByteBuffer.wrap(rec, 10, 4).getInt();
        if (stored != crc(rec, 0, total)) {
            return null;
        }
        byte type = rec[1];
        int version = ByteBuffer.wrap(rec, 2, 2).getShort();
        int idLen = Short.toUnsignedInt(ByteBuffer.wrap(rec, 4, 2).getShort());
        String id = new String(rec, HEADER, idLen, StandardCharsets.UTF_8);
        byte[] value = new byte[total - HEADER - idLen];
        System.arraycopy(rec, HEADER + idLen, value, 0, value.length);
        return new Decoded(type, version, id, value);
    }
}
