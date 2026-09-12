package net.orfeon.solr.importer.source;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;

/**
 * Entry point for reading input files. Picks the reader implementation from the file extension,
 * so adding a format (e.g. Parquet) means adding a case here and a RecordReader implementation.
 */
public final class RecordReaders {

    private RecordReaders() {
    }

    public static boolean isSupported(final String uri) {
        if (uri == null) {
            return false;
        }
        return uri.toLowerCase(Locale.ROOT).endsWith(".avro");
    }

    /**
     * Lists the supported input files under the URI (local file, local directory, gs:// object or prefix).
     */
    public static List<String> listSupported(final String uri) throws IOException {
        return Sources.list(uri).stream()
                .filter(RecordReaders::isSupported)
                .toList();
    }

    public static RecordReader open(final String uri) throws IOException {
        if (!isSupported(uri)) {
            throw new IllegalArgumentException("unsupported input file: " + uri + " (supported: .avro)");
        }
        final InputStream input = Sources.open(uri);
        try {
            return new AvroRecordReader(input);
        } catch (final IOException | RuntimeException e) {
            input.close();
            throw e;
        }
    }

}
