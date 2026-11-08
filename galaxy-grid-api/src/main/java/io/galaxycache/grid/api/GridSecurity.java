package io.galaxycache.grid.api;

import io.galaxycache.common.GalaxyException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Map;

/**
 * Transport security of a grid, from its storage settings:
 * <pre>
 * authToken              shared secret (use ${env:NAME}); every connection proves it with HMAC-SHA256 over a
 *                        nonce from the node, so the token itself never crosses the network
 * tls                    true to encrypt all grid traffic (JDK TLS, no extra libraries)
 * tlsKeyStore            PKCS12 file with the node's key and certificate          (nodes)
 * tlsKeyStorePassword
 * tlsTrustStore          PKCS12 file with the certificates (or CA) to trust       (clients and nodes)
 * tlsTrustStorePassword
 * tlsVerifyHostname      true: the node certificate must match the host in 'nodes'
 * bindAddress            address nodes listen on; default: the host given for the node in 'nodes'
 * </pre>
 */
public record GridSecurity(byte[] token, SSLContext ssl, boolean verifyHostname) {

    public static final GridSecurity NONE = new GridSecurity(null, null, false);

    public static GridSecurity from(Map<String, Object> s) {
        String token = GridSettings.str(s, "authToken", null);
        if (token != null && token.length() < 16) {
            throw new GalaxyException("grid authToken must be at least 16 characters");
        }
        boolean tls = Boolean.parseBoolean(GridSettings.str(s, "tls", "false"));
        SSLContext ctx = null;
        if (tls) {
            try {
                KeyManagerFactory kmf = null;
                String ks = GridSettings.str(s, "tlsKeyStore", null);
                if (ks != null) {
                    char[] pw = GridSettings.str(s, "tlsKeyStorePassword", "").toCharArray();
                    kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    kmf.init(load(ks, pw), pw);
                }
                TrustManagerFactory tmf = null;
                String ts = GridSettings.str(s, "tlsTrustStore", null);
                if (ts != null) {
                    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    tmf.init(load(ts, GridSettings.str(s, "tlsTrustStorePassword", "").toCharArray()));
                }
                ctx = SSLContext.getInstance("TLS");
                ctx.init(kmf == null ? null : kmf.getKeyManagers(), tmf == null ? null : tmf.getTrustManagers(), null);
            } catch (IOException | java.security.GeneralSecurityException e) {
                throw new GalaxyException("cannot set up grid TLS: " + e.getMessage(), e);
            }
        }
        return new GridSecurity(token == null ? null : token.getBytes(StandardCharsets.UTF_8), ctx,
                Boolean.parseBoolean(GridSettings.str(s, "tlsVerifyHostname", "true")));
    }

    private static KeyStore load(String file, char[] password) throws IOException, java.security.GeneralSecurityException {
        KeyStore ks = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(Path.of(file))) {
            ks.load(in, password);
        }
        return ks;
    }

    /** A connected client socket: plain, or TLS with the handshake done (and the host checked if configured). */
    public Socket clientSocket(String host, int port, int timeoutMs) throws IOException {
        Socket plain = new Socket();
        plain.setTcpNoDelay(true);
        plain.setReceiveBufferSize(4 * 1024 * 1024);
        plain.setSendBufferSize(1024 * 1024);
        plain.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
        if (ssl == null) {
            return plain;
        }
        SSLSocket s = (SSLSocket) ssl.getSocketFactory().createSocket(plain, host, port, true);
        if (verifyHostname) {
            SSLParameters p = s.getSSLParameters();
            p.setEndpointIdentificationAlgorithm("HTTPS");
            s.setSSLParameters(p);
        }
        s.setSoTimeout(timeoutMs);
        s.startHandshake();
        s.setSoTimeout(0);
        return s;
    }

    /** A server socket bound to {@code bindAddress:port}: plain, or TLS. */
    public java.net.ServerSocket serverSocket(String bindAddress, int port) throws IOException {
        java.net.ServerSocket ss = ssl == null ? new java.net.ServerSocket() : ssl.getServerSocketFactory().createServerSocket();
        ss.setReuseAddress(true);
        ss.setReceiveBufferSize(1024 * 1024);
        ss.bind(new java.net.InetSocketAddress(bindAddress, port), 256);
        return ss;
    }

    public boolean tls() {
        return ssl != null;
    }
}
