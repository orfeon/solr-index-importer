package net.orfeon.solr.importer.cli;

import net.orfeon.solr.importer.index.IndexImporter;
import net.orfeon.solr.importer.index.IndexWriters;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Builds the index of a core from Avro files, bypassing the HTTP update path.
 * Input is a local file, a local directory, or a gs:// object / prefix.
 * Runs against SOLR_HOME (env SOLR_HOME, default /var/solr/data/) so that it can be executed
 * inside the Solr image at build time and the resulting index baked into the image.
 * Records that do not satisfy the schema stop the import unless env IMPORT_ON_INVALID=skip.
 */
public final class AvroImport {

    private static final String DEFAULT_SOLR_HOME = "/var/solr/data/";
    private static final String ON_INVALID_ENV = "IMPORT_ON_INVALID";

    private static final Logger LOG = LoggerFactory.getLogger(AvroImport.class);

    private AvroImport() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("importAvro requires 2 args: <coreName> <source>. but " + args.length);
        }
        final String coreName = args[0];
        final String source = args[1];

        final List<String> files = RecordReaders.listSupported(source);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("no supported input files found under: " + source);
        }
        LOG.info("found {} input files under {}", files.size(), source);

        final Path solrHome = solrHome();
        if (!Files.isDirectory(solrHome)) {
            throw new IllegalArgumentException("solr home not found: " + solrHome);
        }

        final CoreContainer container = CoreContainer.createAndLoad(solrHome);
        Path indexDir = null;
        Path parkedDir = null;
        try {
            if (!container.getAllCoreNames().contains(coreName)) {
                throw new IllegalArgumentException("core " + coreName + " does not exist. cores: " + container.getAllCoreNames());
            }
            try (final SolrCore core = container.getCore(coreName);
                 final SolrIndexWriter writer = IndexWriters.create(core, true)) {

                final IndexImporter importer = new IndexImporter(core, writer, invalidRecordPolicy());
                final long total = importer.importFiles(files);
                final long generation = importer.commit();
                writer.flush();
                writer.forceMerge(1);
                LOG.info("committed {} documents into core {} (generation {}, skipped {} invalid records)",
                        total, coreName, generation, importer.getSkipped());

                indexDir = Path.of(core.getIndexDir());
                parkedDir = Path.of(core.getDataDir(), "index.import");
            }
        } finally {
            shutdownPreservingIndex(container, indexDir, parkedDir);
        }
        LOG.info("index written to {}", indexDir);
    }

    private static IndexImporter.InvalidRecordPolicy invalidRecordPolicy() {
        try {
            return IndexImporter.InvalidRecordPolicy.parse(System.getenv(ON_INVALID_ENV));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(ON_INVALID_ENV + " must be fail or skip", e);
        }
    }

    private static Path solrHome() {
        final String env = System.getenv("SOLR_HOME");
        return Path.of(env == null || env.isBlank() ? DEFAULT_SOLR_HOME : env);
    }

    /**
     * Shuts the container down without letting it touch the index that was just written.
     *
     * The index was written by an IndexWriter opened outside the core's update handler. When the container
     * shuts down, the core's own update handler may still write to the index directory (for example an empty
     * commit), which would replace the segments written here. So the index is moved aside for the duration of
     * the shutdown, whatever shutdown left behind is discarded, and the index is moved back.
     * When indexDir is null nothing was written and the container is simply shut down.
     */
    private static void shutdownPreservingIndex(final CoreContainer container, final Path indexDir, final Path parkedDir) throws IOException {
        if (indexDir == null) {
            container.shutdown();
            return;
        }
        Files.move(indexDir, parkedDir);
        Files.createDirectory(indexDir);
        try {
            container.shutdown();
        } finally {
            deleteRecursively(indexDir);
            Files.move(parkedDir, indexDir);
        }
    }

    private static void deleteRecursively(final Path dir) throws IOException {
        try (final Stream<Path> paths = Files.walk(dir)) {
            for (final Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(path);
            }
        }
    }

}
