package net.orfeon.solr.importer.storage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Obtains OAuth2 access tokens for Google APIs the way Application Default Credentials do, without the
 * Google client libraries. Credentials are looked up in this order:
 * <ol>
 *   <li>the file named by the GOOGLE_APPLICATION_CREDENTIALS environment variable,</li>
 *   <li>the file written by {@code gcloud auth application-default login},</li>
 *   <li>the metadata server of the Google Cloud runtime (GCE, Cloud Run, Cloud Build, GKE).</li>
 * </ol>
 * Supported file types are {@code service_account} (a JWT signed with the key is exchanged for a token)
 * and {@code authorized_user} (the refresh token is exchanged for a token). External account and
 * impersonated credentials are not supported.
 */
public final class GoogleCredentials {

    public static final String STORAGE_READ_SCOPE = "https://www.googleapis.com/auth/devstorage.read_only";

    static final String CREDENTIALS_ENV = "GOOGLE_APPLICATION_CREDENTIALS";
    static final String METADATA_HOST_ENV = "GCE_METADATA_HOST";
    static final String DEFAULT_METADATA_HOST = "metadata.google.internal";
    static final String DEFAULT_TOKEN_URI = "https://oauth2.googleapis.com/token";

    private static final Duration REFRESH_MARGIN = Duration.ofSeconds(60);
    private static final Duration TOKEN_LIFETIME = Duration.ofHours(1);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final Base64.Encoder BASE64_URL = Base64.getUrlEncoder().withoutPadding();

    private final TokenSource source;
    private final HttpClient http;
    private final Object lock = new Object();
    private AccessToken token;

    private GoogleCredentials(final TokenSource source, final HttpClient http) {
        this.source = source;
        this.http = http;
    }

    public static GoogleCredentials applicationDefault(final HttpClient http) throws IOException {
        final String env = System.getenv(CREDENTIALS_ENV);
        if (env != null && !env.isBlank()) {
            return fromFile(Path.of(env), http);
        }
        final Path wellKnown = wellKnownFile();
        if (Files.isRegularFile(wellKnown)) {
            return fromFile(wellKnown, http);
        }
        final String host = System.getenv(METADATA_HOST_ENV);
        return metadataServer(host == null || host.isBlank() ? DEFAULT_METADATA_HOST : host, http);
    }

    public static GoogleCredentials fromFile(final Path file, final HttpClient http) throws IOException {
        final JsonNode json = JSON.readTree(Files.readAllBytes(file));
        final String type = json.path("type").asText("");
        final TokenSource source = switch (type) {
            case "service_account" -> new ServiceAccountSource(
                    required(json, "client_email", file),
                    parsePrivateKey(required(json, "private_key", file)),
                    json.path("token_uri").asText(DEFAULT_TOKEN_URI));
            case "authorized_user" -> new AuthorizedUserSource(
                    required(json, "client_id", file),
                    required(json, "client_secret", file),
                    required(json, "refresh_token", file),
                    json.path("token_uri").asText(DEFAULT_TOKEN_URI));
            default -> throw new IOException("unsupported credentials type '" + type + "' in " + file
                    + " (supported: service_account, authorized_user)");
        };
        return new GoogleCredentials(source, http);
    }

    public static GoogleCredentials metadataServer(final String host, final HttpClient http) {
        return new GoogleCredentials(new MetadataSource(host), http);
    }

    /**
     * Returns a valid access token, fetching a new one when none is cached or the cached one is about to expire.
     */
    public String accessToken() throws IOException {
        synchronized (lock) {
            if (token == null || token.expiresAt().isBefore(Instant.now().plus(REFRESH_MARGIN))) {
                token = source.fetch(http);
            }
            return token.value();
        }
    }

    public String describe() {
        return source.describe();
    }

    static Path wellKnownFile() {
        final String configDir = System.getenv("CLOUDSDK_CONFIG");
        if (configDir != null && !configDir.isBlank()) {
            return Path.of(configDir, "application_default_credentials.json");
        }
        final String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
        final String appData = System.getenv("APPDATA");
        if (os.contains("win") && appData != null && !appData.isBlank()) {
            return Path.of(appData, "gcloud", "application_default_credentials.json");
        }
        return Path.of(System.getProperty("user.home"), ".config", "gcloud", "application_default_credentials.json");
    }

    private static String required(final JsonNode json, final String field, final Path file) throws IOException {
        final JsonNode node = json.get(field);
        if (node == null || node.asText().isBlank()) {
            throw new IOException("credentials file " + file + " has no '" + field + "'");
        }
        return node.asText();
    }

    static PrivateKey parsePrivateKey(final String pem) throws IOException {
        final String base64 = pem
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        try {
            return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(base64)));
        } catch (final GeneralSecurityException | IllegalArgumentException e) {
            throw new IOException("invalid private_key in service account credentials", e);
        }
    }

    // ---- token sources -------------------------------------------------------------------------------------

    record AccessToken(String value, Instant expiresAt) {
    }

    interface TokenSource {
        AccessToken fetch(HttpClient http) throws IOException;

        String describe();
    }

    /** Signs a JWT with the service account key and exchanges it for an access token. */
    record ServiceAccountSource(String clientEmail, PrivateKey privateKey, String tokenUri) implements TokenSource {

        @Override
        public AccessToken fetch(final HttpClient http) throws IOException {
            final Instant now = Instant.now();
            final String header = BASE64_URL.encodeToString("{\"alg\":\"RS256\",\"typ\":\"JWT\"}".getBytes(StandardCharsets.UTF_8));
            final Map<String, Object> claims = new LinkedHashMap<>();
            claims.put("iss", clientEmail);
            claims.put("scope", STORAGE_READ_SCOPE);
            claims.put("aud", tokenUri);
            claims.put("iat", now.getEpochSecond());
            claims.put("exp", now.plus(TOKEN_LIFETIME).getEpochSecond());
            final String payload = BASE64_URL.encodeToString(JSON.writeValueAsBytes(claims));
            final String signingInput = header + "." + payload;
            final String signature;
            try {
                final Signature signer = Signature.getInstance("SHA256withRSA");
                signer.initSign(privateKey);
                signer.update(signingInput.getBytes(StandardCharsets.US_ASCII));
                signature = BASE64_URL.encodeToString(signer.sign());
            } catch (final GeneralSecurityException e) {
                throw new IOException("failed to sign the service account JWT", e);
            }
            return postTokenRequest(http, tokenUri, Map.of(
                    "grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion", signingInput + "." + signature));
        }

        @Override
        public String describe() {
            return "service account " + clientEmail;
        }
    }

    /** Exchanges the refresh token of a gcloud user login for an access token. */
    record AuthorizedUserSource(String clientId, String clientSecret, String refreshToken, String tokenUri) implements TokenSource {

        @Override
        public AccessToken fetch(final HttpClient http) throws IOException {
            return postTokenRequest(http, tokenUri, Map.of(
                    "grant_type", "refresh_token",
                    "client_id", clientId,
                    "client_secret", clientSecret,
                    "refresh_token", refreshToken));
        }

        @Override
        public String describe() {
            return "authorized user (gcloud application-default credentials)";
        }
    }

    /** Asks the metadata server of the Google Cloud runtime for the token of the attached service account. */
    record MetadataSource(String host) implements TokenSource {

        @Override
        public AccessToken fetch(final HttpClient http) throws IOException {
            final String base = host.contains("://") ? host : "http://" + host;
            final HttpRequest request = HttpRequest.newBuilder(URI.create(base + "/computeMetadata/v1/instance/service-accounts/default/token"))
                    .header("Metadata-Flavor", "Google")
                    .timeout(Duration.ofSeconds(10))
                    .GET()
                    .build();
            try {
                return parseTokenResponse(send(http, request));
            } catch (final IOException e) {
                throw new IOException("could not get an access token from the metadata server at " + host + ": " + e.getMessage()
                        + ". Set " + CREDENTIALS_ENV + " to a service account key file, or run 'gcloud auth application-default login'.", e);
            }
        }

        @Override
        public String describe() {
            return "metadata server " + host;
        }
    }

    private static AccessToken postTokenRequest(final HttpClient http, final String tokenUri, final Map<String, String> form) throws IOException {
        final String body = form.entrySet().stream()
                .map(e -> URLEncoder.encode(e.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(e.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));
        final HttpRequest request = HttpRequest.newBuilder(URI.create(tokenUri))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(REQUEST_TIMEOUT)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return parseTokenResponse(send(http, request));
    }

    private static HttpResponse<String> send(final HttpClient http, final HttpRequest request) throws IOException {
        final HttpResponse<String> response;
        try {
            response = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while requesting an access token", e);
        }
        if (response.statusCode() / 100 != 2) {
            throw new IOException("token request to " + request.uri() + " failed with HTTP " + response.statusCode() + ": " + excerpt(response.body()));
        }
        return response;
    }

    private static AccessToken parseTokenResponse(final HttpResponse<String> response) throws IOException {
        final JsonNode json = JSON.readTree(response.body());
        final String accessToken = json.path("access_token").asText("");
        if (accessToken.isEmpty()) {
            throw new IOException("token response has no access_token: " + excerpt(response.body()));
        }
        final long expiresIn = json.path("expires_in").asLong(TOKEN_LIFETIME.toSeconds());
        return new AccessToken(accessToken, Instant.now().plusSeconds(expiresIn));
    }

    private static String excerpt(final String body) {
        if (body == null) {
            return "";
        }
        return body.length() > 300 ? body.substring(0, 300) + "..." : body;
    }

}
