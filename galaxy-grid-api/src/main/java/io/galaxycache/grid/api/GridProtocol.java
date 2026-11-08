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
    public static final int VERSION = 2;
    public static final int MAX_FRAME = 64 * 1024 * 1024;
    /** Ids per frame of a streamed INDEX_QUERY answer. */
    public static final int IDS_PER_FRAME = 50_000;
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
    /** Node moves every record (and index document) it does not own to the owner; answers [moved: long]. */
    public static final byte REBALANCE = 16;
    /** Node rewrites its datasets without old versions and deleted records; answers [bytes reclaimed: long]. */
    public static final byte COMPACT = 17;

    // ---- response status
    public static final byte PART = 0;
    public static final byte LAST = 1;
    public static final byte ERROR = 2;

    private GridProtocol() {
    }

    // ================================================================== handshake
    //
    //   client -> node : [MAGIC][VERSION]
    //   node -> client : [MAGIC][VERSION][node nonce: 16][node fingerprint: UTF][auth required: byte]
    //   client -> node : [client fingerprint: UTF][client nonce: 16][proof length: int][proof]
    //   node -> client : [status: int][message: UTF][proof length: int][proof]    status 0 = accepted
    //
    // Both sides compare fingerprints, so a node or client running with another grid configuration (other nodes,
    // ports or partition count) is refused before any data moves. With a token each side proves it knows it:
    //   client proof = HMAC-SHA256(token, "galaxy-client" | node nonce | client nonce | fingerprint)
    //   node proof   = HMAC-SHA256(token, "galaxy-node"   | client nonce | node nonce | fingerprint)
    // The token never crosses the network, a captured proof is useless on another connection, and a client will not
    // talk to something that does not know the token. Without TLS the frames that follow are neither encrypted nor
    // integrity-protected.

    public static final int HS_OK = 0;
    public static final int HS_VERSION = 1;
    public static final int HS_FINGERPRINT = 2;
    public static final int HS_AUTH = 3;
    private static final int NONCE_BYTES = 16;
    private static final int MAX_PROOF_BYTES = 64;
    private static final java.security.SecureRandom RANDOM = new java.security.SecureRandom();

    /** Client side of the handshake; throws with the node's reason if refused. */
    public static void clientHandshake(DataInputStream in, DataOutputStream out, String fingerprint, byte[] token,
                                       String node) throws IOException {
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.flush();
        if (in.readInt() != MAGIC) {
            throw new GalaxyException(node + " is not a Galaxy grid node");
        }
        int version = in.readInt();
        if (version != VERSION) {
            throw new GalaxyException(node + " speaks grid protocol v" + version + ", this client v" + VERSION);
        }
        byte[] nodeNonce = new byte[NONCE_BYTES];
        in.readFully(nodeNonce);
        String nodeFingerprint = in.readUTF();
        boolean authRequired = in.readByte() != 0;
        if (!nodeFingerprint.equals(fingerprint)) {
            throw new GalaxyException(node + " runs a different grid configuration (" + nodeFingerprint + " vs "
                    + fingerprint + "); all nodes and clients must use the same nodes/partitions");
        }
        if (authRequired && token == null) {
            throw new GalaxyException(node + " requires authentication: set authToken for this grid");
        }
        if (!authRequired && token != null) {
            throw new GalaxyException(node + " does not use authentication but this client is configured with a token; "
                    + "refusing to talk to it");
        }
        byte[] clientNonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(clientNonce);
        byte[] proof = token == null ? new byte[0] : proof(token, "galaxy-client", nodeNonce, clientNonce, fingerprint);
        out.writeUTF(fingerprint);
        out.write(clientNonce);
        out.writeInt(proof.length);
        out.write(proof);
        out.flush();
        int status = in.readInt();
        String message = in.readUTF();
        byte[] nodeProof = readProof(in);
        if (status != HS_OK) {
            throw new GalaxyException(node + " refused the connection: " + message);
        }
        if (token != null && !java.security.MessageDigest.isEqual(nodeProof,
                proof(token, "galaxy-node", clientNonce, nodeNonce, fingerprint))) {
            throw new GalaxyException(node + " refused the connection: it could not prove it knows the authentication token");
        }
    }

    /** Node side of the handshake; returns null if accepted, otherwise the reason (already sent to the client). */
    public static String nodeHandshake(DataInputStream in, DataOutputStream out, String fingerprint, byte[] token)
            throws IOException {
        if (in.readInt() != MAGIC) {
            return "not a grid client";
        }
        int version = in.readInt();
        byte[] nodeNonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(nodeNonce);
        out.writeInt(MAGIC);
        out.writeInt(VERSION);
        out.write(nodeNonce);
        out.writeUTF(fingerprint);
        out.writeByte(token == null ? 0 : 1);
        out.flush();
        if (version != VERSION) {
            return "client speaks protocol v" + version; // the client gives up after reading our version
        }
        String clientFingerprint = in.readUTF();
        byte[] clientNonce = new byte[NONCE_BYTES];
        in.readFully(clientNonce);
        byte[] proof = readProof(in);
        int status = HS_OK;
        String message = "ok";
        if (!fingerprint.equals(clientFingerprint)) {
            status = HS_FINGERPRINT;
            message = "different grid configuration (" + clientFingerprint + " vs " + fingerprint + ")";
        } else if (token != null && !java.security.MessageDigest.isEqual(proof,
                proof(token, "galaxy-client", nodeNonce, clientNonce, fingerprint))) {
            status = HS_AUTH;
            message = "authentication failed";
        }
        byte[] nodeProof = status == HS_OK && token != null
                ? proof(token, "galaxy-node", clientNonce, nodeNonce, fingerprint) : new byte[0];
        out.writeInt(status);
        out.writeUTF(message);
        out.writeInt(nodeProof.length);
        out.write(nodeProof);
        out.flush();
        return status == HS_OK ? null : message;
    }

    private static byte[] readProof(DataInputStream in) throws IOException {
        int n = in.readInt();
        if (n < 0 || n > MAX_PROOF_BYTES) {
            throw new IOException("bad handshake proof length " + n);
        }
        byte[] proof = new byte[n];
        in.readFully(proof);
        return proof;
    }

    static byte[] proof(byte[] key, String label, byte[] first, byte[] second, String fingerprint) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            mac.update(label.getBytes(StandardCharsets.UTF_8));
            mac.update(first);
            mac.update(second);
            return mac.doFinal(fingerprint.getBytes(StandardCharsets.UTF_8));
        } catch (java.security.GeneralSecurityException e) {
            throw new GalaxyException("HMAC not available", e);
        }
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
        return readBody(in, readLength(in));
    }

    /** The validated length of the next frame. Nothing has been allocated yet. */
    public static int readLength(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 9 || len > MAX_FRAME + 9) {
            throw new IOException("bad frame length " + len);
        }
        return len;
    }

    /** The rest of a frame whose length came from {@link #readLength}. */
    public static Frame readBody(DataInputStream in, int len) throws IOException {
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

        public Out putLong(long v) {
            putInt((int) (v >>> 32));
            return putInt((int) v);
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

        /**
         * A count of items that take at least {@code minBytes} each. It must be possible for them to fit in what is
         * left of the frame, so a request claiming billions of items cannot make the reader allocate for them.
         */
        public int getCount(int minBytes) {
            int n = b.getInt();
            if (n < 0 || n > b.remaining() / minBytes) {
                throw new GalaxyException("bad item count " + n + " in grid frame");
            }
            return n;
        }

        public long getLong() {
            return b.getLong();
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
