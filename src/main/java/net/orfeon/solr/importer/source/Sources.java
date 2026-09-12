package net.orfeon.solr.importer.source;

import net.orfeon.solr.importer.storage.GcsStorage;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Resolves where input files live. A source URI is either a local path (file or directory)
 * or a Cloud Storage URI (gs://bucket/object or gs://bucket/prefix/).
 */
public final class Sources {

    private Sources() {
    }

    /**
     * Lists every regular file under the URI. A URI that points to a single file yields that file only.
     */
    public static List<String> list(final String uri) throws IOException {
        if (uri == null || uri.isBlank()) {
            throw new IllegalArgumentException("source uri must not be empty");
        }
        if (GcsStorage.isGcsPath(uri)) {
            return GcsStorage.list(uri);
        }
        final Path path = Path.of(uri);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("path not found: " + uri);
        }
        if (Files.isDirectory(path)) {
            try (final Stream<Path> paths = Files.walk(path)) {
                return paths
                        .filter(Files::isRegularFile)
                        .map(Path::toString)
                        .sorted()
                        .toList();
            }
        }
        return List.of(path.toString());
    }

    public static InputStream open(final String uri) throws IOException {
        if (GcsStorage.isGcsPath(uri)) {
            return GcsStorage.open(uri);
        }
        return new BufferedInputStream(Files.newInputStream(Path.of(uri)));
    }

}
