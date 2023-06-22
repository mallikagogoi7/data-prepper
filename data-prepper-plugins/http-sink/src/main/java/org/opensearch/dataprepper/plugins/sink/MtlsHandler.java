package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.plugins.sink.configuration.HttpSinkConfiguration;

import javax.net.ssl.*;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Collection;

public class MtlsHandler {

    private HttpSinkConfiguration httpSinkConfiguration;

    public MtlsHandler(final HttpSinkConfiguration httpSinkConfiguration) {
        this.httpSinkConfiguration = httpSinkConfiguration;
    }

    public KeyManager[] getKeyManagers() throws NoSuchAlgorithmException, KeyStoreException, CertificateException, IOException, InvalidKeySpecException, UnrecoverableKeyException {
        final byte[] publicData = Files.readAllBytes(httpSinkConfiguration.getSslCertificateFile());
        final byte[] privateData = Files.readAllBytes(httpSinkConfiguration.getSslKeyFile());

        String privateString = new String(privateData, Charset.defaultCharset())
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");

        byte[] encoded = Base64.getDecoder().decode(privateString);

        final CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        final Collection<? extends Certificate> chain = certificateFactory.generateCertificates(
                new ByteArrayInputStream(publicData));

        Key key = KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(encoded));

        KeyStore clientKeyStore = KeyStore.getInstance("jks");
        final char[] pwdChars = "test".toCharArray();
        clientKeyStore.load(null, null);
        clientKeyStore.setKeyEntry("test", key, pwdChars, chain.toArray(new Certificate[0]));

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(clientKeyStore, pwdChars);
        return keyManagerFactory.getKeyManagers();
    }

    public SSLContext getSSLConnection() throws NoSuchAlgorithmException, UnrecoverableKeyException, CertificateException, KeyStoreException, IOException, InvalidKeySpecException, KeyManagementException {

        TrustManager[] acceptAllTrustManager = {
                new X509TrustManager() {
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[0];
                    }

                    public void checkClientTrusted(
                            X509Certificate[] certs, String authType) {
                    }

                    public void checkServerTrusted(
                            X509Certificate[] certs, String authType) {
                    }
                }
        };

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(getKeyManagers(), acceptAllTrustManager, new java.security.SecureRandom());
        return sslContext;
    }

    public static void main(String[] args) {
        try {
            MtlsHandler handler = new MtlsHandler(new HttpSinkConfiguration());
            HttpClient client = HttpClient.newBuilder()
                    .sslContext(handler.getSSLConnection())
                    .build();

            HttpRequest exactRequest = HttpRequest.newBuilder()
                    .uri(URI.create("https://127.0.0.1"))
                    .GET()
                    .build();

            var exactResponse = client.sendAsync(exactRequest, HttpResponse.BodyHandlers.ofString())
                    .join();
            System.out.println(exactResponse.statusCode());
        }catch (Exception e) {

        }
    }
}
