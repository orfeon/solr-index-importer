package net.orfeon.solr.importer.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal Cloud Storage client over the JSON API, using only the JDK HTTP client.
 * Supports what the importer needs: listing objects under a prefix and streaming an object's content.
 * Credentials come from {@link GoogleCredentials}. Transient failures (connection errors, HTTP 429 and 5xx)
 * are retried a few times; an interrupted download is not resumed.
 */
public final class GcsStorage {

    private static final Logger LOG = LoggerFactory.getLogger(GcsStorage.class);

    private static final String SCHEME = "gs://";
    static final String DEFAULT_ENDPOINT = "https://storage.googleapis.com";
    /** Conventional override for emulators, e.g. "localhost:4443" or "http://localhost:4443". */
    static final String ENDPOINT_ENV = "STORAGE_EMULATOR_HOST";

    private static final int MAX_ATTEMPTS = 3;
    private static final Duration RETRY_DELAY = Duration.ofMillis(500);
    private static final Duration RESPONSE_TIMEOUT = Duration.ofMinutes(5);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static volatile GcsStorage defaultInstance;

    public record ObjectId(String bucket, String name) {
    }

    private final String endpoint;
    private final GoogleCredentials credentials;
    private final HttpClient http;

    public GcsStorage(final String endpoint, final GoogleCredentials credentials, final HttpClient http) {
        this.endpoint = endpoint.endsWith("/") ? endpoint.substring(0, endpoint.length() - 1) : endpoint;
        this.credentials = credentials;
        this.http = http;
    }

    /**
     * Client bound to the real endpoint (or STORAGE_EMULATOR_HOST) and Application Default Credentials.
     */
    public static GcsStorage defaultInstance() throws IOException {
        if (defaultInstance == null) {
            synchronized (GcsStorage.class) {
                if (defaultInstance == null) {
                    final HttpClient http = HttpClient.newBuilder()
                            .connectTimeout(Duration.ofSeconds(20))
                            .followRedirects(HttpClient.Redirect.NORMAL)
                            .build();
                    final GoogleCredentials credentials = GoogleCredentials.applicationDefault(http);
                    LOG.info("Cloud Storage credentials: {}", credentials.describe());
                    defaultInstance = new GcsStorage(endpointFromEnv(), credentials, http);
                }
            }
        }
        return defaultInstance;
    }

    private static String endpointFromEnv() {
        final String env = System.getenv(ENDPOINT_ENV);
        if (env == null || env.isBlank()) {
            return DEFAULT_ENDPOINT;
        }
        return env.contains("://") ? env : "http://" + env;
    }

    // ---- static conveniences used by Sources ---------------------------------------------------------------

    public static boolean isGcsPath(final String path) {
        return path != null && path.startsWith(SCHEME);
    }

    /**
     * Parses gs://bucket/object into bucket and object name. The object part may be empty (gs://bucket or gs://bucket/).
     */
    public static ObjectId parse(final String gcsPath) {
        if (!isGcsPath(gcsPath)) {
            throw new IllegalArgumentException("gcsPath must start with " + SCHEME + ": " + gcsPath);
        }
        final String[] parts = gcsPath.substring(SCHEME.length()).split("/", 2);
        if (parts[0].isEmpty()) {
            throw new IllegalArgumentException("Illegal gcsPath (bucket is empty): " + gcsPath);
        }
        return new ObjectId(parts[0], parts.length == 2 ? parts[1] : "");
    }

    public static String toUri(final ObjectId id) {
        return SCHEME + id.bucket() + "/" + id.name();
    }

    public static List<String> list(final String gcsPrefix) throws IOException {
        return defaultInstance().listObjects(gcsPrefix);
    }

    public static InputStream open(final String gcsPath) throws IOException {
        return defaultInstance().openObject(gcsPath);
    }

    // ---- instance API ----------------------------------------------------------------------------------------

    /**
     * Lists object URIs under the prefix, following pagination. Directory placeholder objects
     * (names ending with "/") are skipped.
     */
    public List<String> listObjects(final String gcsPrefix) throws IOException {
        final ObjectId prefix = parse(gcsPrefix);
        final List<String> uris = new ArrayList<>();
        String pageToken = null;
        do {
            final StringBuilder url = new StringBuilder(endpoint)
                    .append("/storage/v1/b/").append(encode(prefix.bucket()))
                    .append("/o?fields=nextPageToken,items(name)&prefix=").append(encode(prefix.name()));
            if (pageToken != null) {
                url.append("&pageToken=").append(encode(pageToken));
            }
            final HttpResponse<byte[]> response = send(url.toString(), HttpResponse.BodyHandlers.ofByteArray());
            final JsonNode body = JSON.readTree(response.body());
            for (final JsonNode item : body.path("items")) {
                final String name = item.path("name").asText();
                if (!name.endsWith("/")) {
                    uris.add(toUri(new ObjectId(prefix.bucket(), name)));
                }
            }
            pageToken = body.hasNonNull("nextPageToken") ? body.get("nextPageToken").asText() : null;
        } while (pageToken != null);
        return uris;
    }

    /**
     * Streams the content of one object. The caller closes the stream.
     */
    public InputStream openObject(final String gcsPath) throws IOException {
        final ObjectId id = parse(gcsPath);
        if (id.name().isEmpty()) {
            throw new IllegalArgumentException("object name is empty: " + gcsPath);
        }
        final String url = endpoint + "/storage/v1/b/" + encode(id.bucket()) + "/o/" + encode(id.name()) + "?alt=media";
        return send(url, HttpResponse.BodyHandlers.ofInputStream()).body();
    }

    private <T> HttpResponse<T> send(final String url, final HttpResponse.BodyHandler<T> handler) throws IOException {
        IOException last = null;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            final HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .header("Authorization", "Bearer " + credentials.accessToken())
                    .timeout(RESPONSE_TIMEOUT)
                    .GET()
                    .build();
            try {
                final HttpResponse<T> response = http.send(request, handler);
                final int status = response.statusCode();
                if (status / 100 == 2) {
                    return response;
                }
                final String message = "GET " + url + " failed with HTTP " + status + ": " + excerpt(response.body());
                if (status != 429 && status < 500) {
                    throw new IOException(message);
                }
                last = new IOException(message);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupted while requesting " + url, e);
            } catch (final IOException e) {
                if (!isTransient(e)) {
                    throw e;
                }
                last = e;
            }
            if (attempt < MAX_ATTEMPTS) {
                LOG.warn("retrying ({}/{}) after failure: {}", attempt, MAX_ATTEMPTS, last.getMessage());
                sleep(RETRY_DELAY.multipliedBy(attempt));
            }
        }
        throw last;
    }

    private static boolean isTransient(final IOException e) {
        // Our own non-retryable failures are raised with a message starting with "GET"; everything else
        // (connection reset, timeout, unreachable host) is worth retrying.
        return e.getMessage() == null || !e.getMessage().startsWith("GET ");
    }

    private static String excerpt(final Object body) throws IOException {
        final String text;
        if (body instanceof byte[] bytes) {
            text = new String(bytes, StandardCharsets.UTF_8);
        } else if (body instanceof InputStream stream) {
            try (stream) {
                text = new String(stream.readNBytes(300), StandardCharsets.UTF_8);
            }
        } else {
            text = String.valueOf(body);
        }
        return text.length() > 300 ? text.substring(0, 300) + "..." : text;
    }

    private static String encode(final String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static void sleep(final Duration duration) throws IOException {
        try {
            Thread.sleep(duration.toMillis());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted", e);
        }
    }

}
