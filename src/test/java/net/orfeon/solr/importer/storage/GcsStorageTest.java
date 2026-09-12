package net.orfeon.solr.importer.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class GcsStorageTest {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private HttpServer server;
    private String base;
    private final HttpClient http = HttpClient.newHttpClient();

    private final AtomicInteger tokenRequests = new AtomicInteger();
    private final List<Map<String, String>> tokenForms = new ArrayList<>();
    private final List<String> authorizationHeaders = new ArrayList<>();
    private final List<String> metadataFlavors = new ArrayList<>();
    private final AtomicInteger storageRequests = new AtomicInteger();
    private int failFirstWithStatus = 0;

    @Before
    public void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        base = "http://127.0.0.1:" + server.getAddress().getPort();

        server.createContext("/token", exchange -> {
            tokenRequests.incrementAndGet();
            tokenForms.add(parseForm(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            respond(exchange, 200, "{\"access_token\":\"tok-" + tokenRequests.get() + "\",\"expires_in\":3600}");
        });
        server.createContext("/computeMetadata/v1/instance/service-accounts/default/token", exchange -> {
            metadataFlavors.add(exchange.getRequestHeaders().getFirst("Metadata-Flavor"));
            respond(exchange, 200, "{\"access_token\":\"meta-tok\",\"expires_in\":1800}");
        });
        server.createContext("/storage/v1/b/bucket/o", exchange -> {
            storageRequests.incrementAndGet();
            authorizationHeaders.add(exchange.getRequestHeaders().getFirst("Authorization"));
            if (failFirstWithStatus != 0 && storageRequests.get() == 1) {
                respond(exchange, failFirstWithStatus, "{\"error\":\"try later\"}");
                return;
            }
            final String path = exchange.getRequestURI().getRawPath();
            final String query = exchange.getRequestURI().getRawQuery();
            if (path.equals("/storage/v1/b/bucket/o")) {
                final Map<String, String> params = parseForm(query);
                assertEquals("dir/", params.get("prefix"));
                if (params.containsKey("pageToken")) {
                    assertEquals("page2", params.get("pageToken"));
                    respond(exchange, 200, "{\"items\":[{\"name\":\"dir/c.avro\"}]}");
                } else {
                    respond(exchange, 200, "{\"items\":[{\"name\":\"dir/\"},{\"name\":\"dir/a.avro\"},{\"name\":\"dir/sub/b.avro\"}],\"nextPageToken\":\"page2\"}");
                }
            } else if (path.equals("/storage/v1/b/bucket/o/dir%2Fa.avro") && "alt=media".equals(query)) {
                respond(exchange, 200, "avro-bytes");
            } else {
                respond(exchange, 404, "{\"error\":\"not found\"}");
            }
        });
        server.start();
    }

    @After
    public void stopServer() {
        server.stop(0);
    }

    @Test
    public void serviceAccountKeyIsExchangedForTokenAndUsed() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final GcsStorage storage = new GcsStorage(base, GoogleCredentials.fromFile(serviceAccountFile(keyPair), http), http);

        final List<String> objects = storage.listObjects("gs://bucket/dir/");

        assertEquals(List.of("gs://bucket/dir/a.avro", "gs://bucket/dir/sub/b.avro", "gs://bucket/dir/c.avro"), objects);
        assertEquals(1, tokenRequests.get());
        assertEquals("Bearer tok-1", authorizationHeaders.get(0));

        final Map<String, String> form = tokenForms.get(0);
        assertEquals("urn:ietf:params:oauth:grant-type:jwt-bearer", form.get("grant_type"));
        final String[] jwt = form.get("assertion").split("\\.");
        assertEquals(3, jwt.length);
        final Signature verifier = Signature.getInstance("SHA256withRSA");
        verifier.initVerify(keyPair.getPublic());
        verifier.update((jwt[0] + "." + jwt[1]).getBytes(StandardCharsets.US_ASCII));
        assertTrue("JWT signature must verify with the public key", verifier.verify(Base64.getUrlDecoder().decode(jwt[2])));
        final JsonNode claims = JSON.readTree(Base64.getUrlDecoder().decode(jwt[1]));
        assertEquals("importer@example.iam.gserviceaccount.com", claims.get("iss").asText());
        assertEquals(GoogleCredentials.STORAGE_READ_SCOPE, claims.get("scope").asText());
        assertEquals(base + "/token", claims.get("aud").asText());
    }

    @Test
    public void tokenIsCachedAcrossRequests() throws Exception {
        final GcsStorage storage = new GcsStorage(base, GoogleCredentials.fromFile(authorizedUserFile(), http), http);

        storage.listObjects("gs://bucket/dir/");
        try (final InputStream in = storage.openObject("gs://bucket/dir/a.avro")) {
            assertEquals("avro-bytes", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        assertEquals(1, tokenRequests.get());
        assertEquals("refresh_token", tokenForms.get(0).get("grant_type"));
        assertEquals("rt-1", tokenForms.get(0).get("refresh_token"));
        assertEquals(3, authorizationHeaders.size());
        assertTrue(authorizationHeaders.stream().allMatch("Bearer tok-1"::equals));
    }

    @Test
    public void metadataServerProvidesToken() throws Exception {
        final GcsStorage storage = new GcsStorage(base, GoogleCredentials.metadataServer("127.0.0.1:" + server.getAddress().getPort(), http), http);

        try (final InputStream in = storage.openObject("gs://bucket/dir/a.avro")) {
            assertEquals("avro-bytes", new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }

        assertEquals(List.of("Google"), metadataFlavors);
        assertEquals("Bearer meta-tok", authorizationHeaders.get(0));
    }

    @Test
    public void serverErrorsAreRetried() throws Exception {
        failFirstWithStatus = 503;
        final GcsStorage storage = new GcsStorage(base, GoogleCredentials.fromFile(authorizedUserFile(), http), http);

        assertEquals(3, storage.listObjects("gs://bucket/dir/").size());
        assertEquals(3, storageRequests.get());
    }

    @Test
    public void clientErrorsAreNotRetried() throws Exception {
        final GcsStorage storage = new GcsStorage(base, GoogleCredentials.fromFile(authorizedUserFile(), http), http);

        try {
            storage.openObject("gs://bucket/dir/missing.avro");
            fail("expected IOException");
        } catch (final IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("HTTP 404"));
        }
        assertEquals(1, storageRequests.get());
    }

    @Test
    public void unsupportedCredentialsFileIsRejected() throws Exception {
        final Path file = folder.newFile("external.json").toPath();
        Files.writeString(file, "{\"type\":\"external_account\"}");
        try {
            GoogleCredentials.fromFile(file, http);
            fail("expected IOException");
        } catch (final IOException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("external_account"));
        }
    }

    @Test
    public void gcsPathsAreParsed() {
        assertTrue(GcsStorage.isGcsPath("gs://bucket/dir/file.avro"));
        assertTrue(!GcsStorage.isGcsPath("/local/file.avro"));
        assertEquals("bucket", GcsStorage.parse("gs://bucket/dir/file.avro").bucket());
        assertEquals("dir/file.avro", GcsStorage.parse("gs://bucket/dir/file.avro").name());
        assertEquals("", GcsStorage.parse("gs://bucket").name());
        assertEquals("", GcsStorage.parse("gs://bucket/").name());
        assertEquals("gs://bucket/dir/", GcsStorage.toUri(GcsStorage.parse("gs://bucket/dir/")));
    }

    // ---- helpers -----------------------------------------------------------------------------------------

    private Path serviceAccountFile(final KeyPair keyPair) throws IOException {
        final String pem = "-----BEGIN PRIVATE KEY-----\n"
                + Base64.getMimeEncoder(64, "\n".getBytes(StandardCharsets.US_ASCII)).encodeToString(keyPair.getPrivate().getEncoded())
                + "\n-----END PRIVATE KEY-----\n";
        final Map<String, String> json = new HashMap<>();
        json.put("type", "service_account");
        json.put("client_email", "importer@example.iam.gserviceaccount.com");
        json.put("private_key", pem);
        json.put("token_uri", base + "/token");
        final Path file = folder.newFile("sa.json").toPath();
        JSON.writeValue(file.toFile(), json);
        return file;
    }

    private Path authorizedUserFile() throws IOException {
        final Map<String, String> json = new HashMap<>();
        json.put("type", "authorized_user");
        json.put("client_id", "cid");
        json.put("client_secret", "secret");
        json.put("refresh_token", "rt-1");
        json.put("token_uri", base + "/token");
        final Path file = folder.newFile("adc.json").toPath();
        JSON.writeValue(file.toFile(), json);
        return file;
    }

    private static Map<String, String> parseForm(final String encoded) {
        final Map<String, String> params = new HashMap<>();
        if (encoded == null || encoded.isEmpty()) {
            return params;
        }
        for (final String pair : encoded.split("&")) {
            final String[] kv = pair.split("=", 2);
            params.put(URLDecoder.decode(kv[0], StandardCharsets.UTF_8), kv.length == 2 ? URLDecoder.decode(kv[1], StandardCharsets.UTF_8) : "");
        }
        return params;
    }

    private static void respond(final HttpExchange exchange, final int status, final String body) throws IOException {
        final byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
        assertNotNull(exchange);
    }

}
