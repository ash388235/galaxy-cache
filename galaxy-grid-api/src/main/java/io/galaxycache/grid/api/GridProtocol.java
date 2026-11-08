package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Binary frames over TCP. Many requests share one connection; responses carry the request id, so they can
 * arrive in any order, and a response may be split over several frames (streaming).
 *
 * <pre>
 * request : [int length][long requestId][byte op]    [body]   length = 9 + body
 * response: [int length][long requestId][byte status][body]   status = PART | LAST | ERROR
 * </pre>
 * Record payloads travel as the stored bytes: nodes never decode domain objects.
 */
public final class GridProtocol {

    public static final int MAGIC = 0x47414C58; // "GALX", sent once by the client after connecting
    public static final int VERSION = 1;
    public static final int MAX_FRAME = 64 * 1024 * 1024;
    /** Nodes cut streamed responses into frames of about this size. */
    public static final int STREAM_FRAME_BYTES = 4 * 1024 * 1024;

    // ---- ops
    public static final byte PING = 1;
    public static final byte ENSURE_DATASET = 2;
    public static final byte METADATA = 3;
    public static final byte PUT_RECORDS = 4;
    public static final byte GET_ALL = 5;
    public static final byte DELETE_RECORDS = 6;
    public static final byte SCAN = 7;
    public static final byte DATASET_STATS = 8;
    public static final byte LIST_DATASETS = 9;
    public static final byte DROP_DATASET = 10;
    public static final byte INDEX_UPSERT = 11;
    public static final byte INDEX_DELETE = 12;
    public static final byte INDEX_QUERY = 13;
    public static final byte INDEX_DROP = 14;
    public static final byte NODE_STATS = 15;

    // ---- response status
    public static final byte PART = 0;
    public static final byte LAST = 1;
    public static final byte ERROR = 2;

    private GridProtocol() {
    }

    public static void writeFrame(DataOutputStream out, long id, byte opOrStatus, byte[] body, int off, int len)
            throws IOException {
        out.writeInt(9 + len);
        out.writeLong(id);
        out.writeByte(opOrStatus);
        out.write(body, off, len);
    }

    /** A received frame: request id, op (request) or status (response), and the body. */
    public record Frame(long id, byte code, byte[] body) {
    }

    public static Frame readFrame(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 9 || len > MAX_FRAME + 9) {
            throw new IOException("bad frame length " + len);
        }
        long id = in.readLong();
        byte code = in.readByte();
        byte[] body = new byte[len - 9];
        in.readFully(body);
        return new Frame(id, code, body);
    }

    // ================================================================== body encoding

    /** Growable big-endian body writer. */
    public static final class Out {
        private byte[] buf;
        private int pos;

        public Out() {
            this(256);
        }

        public Out(int initial) {
            buf = new byte[Math.max(16, initial)];
        }

        private void ensure(int n) {
            if (pos + n > buf.length) {
                buf = Arrays.copyOf(buf, Math.max(buf.length * 2, pos + n));
            }
        }

        public Out putInt(int v) {
            ensure(4);
            buf[pos++] = (byte) (v >>> 24);
            buf[pos++] = (byte) (v >>> 16);
            buf[pos++] = (byte) (v >>> 8);
            buf[pos++] = (byte) v;
            return this;
        }

        public Out putBytes(byte[] b) {
            putInt(b.length);
            ensure(b.length);
            System.arraycopy(b, 0, buf, pos, b.length);
            pos += b.length;
            return this;
        }

        public Out putString(String s) {
            return putBytes(s == null ? new byte[0] : s.getBytes(StandardCharsets.UTF_8));
        }

        /** Overwrites a 4-byte int written earlier at {@code at} (for counts known only at the end). */
        public void setInt(int at, int v) {
            buf[at] = (byte) (v >>> 24);
            buf[at + 1] = (byte) (v >>> 16);
            buf[at + 2] = (byte) (v >>> 8);
            buf[at + 3] = (byte) v;
        }

        public int size() {
            return pos;
        }

        public byte[] array() {
            return buf;
        }

        public byte[] toBytes() {
            return Arrays.copyOf(buf, pos);
        }

        public void reset() {
            pos = 0;
        }
    }

    /** Big-endian body reader. */
    public static final class In {
        private final ByteBuffer b;

        public In(byte[] body) {
            b = ByteBuffer.wrap(body);
        }

        public int getInt() {
            return b.getInt();
        }

        public byte[] getBytes() {
            int n = b.getInt();
            if (n < 0 || n > b.remaining()) {
                throw new GalaxyException("corrupt grid frame (length " + n + ")");
            }
            byte[] out = new byte[n];
            b.get(out);
            return out;
        }

        public String getString() {
            return new String(getBytes(), StandardCharsets.UTF_8);
        }

        public boolean hasRemaining() {
            return b.hasRemaining();
        }
    }
}
