package net.orfeon.solr.importer.cli;

import net.orfeon.solr.importer.index.IndexImporter;
import net.orfeon.solr.importer.index.IndexWriters;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
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
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Builds the index of a core from Avro files, bypassing the HTTP update path.
 * Input is a local file, a local directory, or a gs:// object / prefix.
 * Runs against SOLR_HOME (env SOLR_HOME, default /var/solr/data/) so that it can be executed
 * inside the Solr image at build time and the resulting index baked into the image.
 * Records that do not satisfy the schema stop the import unless env IMPORT_ON_INVALID=skip.
 * IMPORT_THREADS sets the number of indexer threads (default: available processors) and IMPORT_DEDUP=false
 * skips the per-document uniqueKey replacement when the input is known to have unique keys.
 */
public final class AvroImport {

    private static final String DEFAULT_SOLR_HOME = "/var/solr/data/";
    private static final String ON_INVALID_ENV = "IMPORT_ON_INVALID";
    private static final String THREADS_ENV = "IMPORT_THREADS";
    private static final String DEDUP_ENV = "IMPORT_DEDUP";

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

                final IndexImporter importer = new IndexImporter(core, writer, options());
                LOG.info("importing with {}", importer.getOptions());
                // The index is merged down to one segment at the end anyway, so merging while indexing would
                // only rewrite segments that the final merge rewrites again. Merges are disabled during the
                // import and the configured policy is restored for the forced merge.
                final MergePolicy mergePolicy = writer.getConfig().getMergePolicy();
                writer.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
                final long total = importer.importFiles(files);
                writer.getConfig().setMergePolicy(mergePolicy);
                // Merge first and commit once, so the committed generation is the merged index
                // and the intermediate segments are not fsynced twice.
                final long mergeStart = System.nanoTime();
                writer.forceMerge(1);
                LOG.info("merged the index into one segment in {} s", seconds(mergeStart));
                final long commitStart = System.nanoTime();
                final long generation = importer.commit();
                LOG.info("committed {} documents into core {} in {} s (generation {}, skipped {} invalid records)",
                        total, coreName, seconds(commitStart), generation, importer.getSkipped());

                indexDir = Path.of(core.getIndexDir());
                parkedDir = Path.of(core.getDataDir(), "index.import");
            }
        } finally {
            shutdownPreservingIndex(container, indexDir, parkedDir);
        }
        LOG.info("index written to {}", indexDir);
    }

    private static IndexImporter.Options options() {
        final IndexImporter.InvalidRecordPolicy policy;
        try {
            policy = IndexImporter.InvalidRecordPolicy.parse(System.getenv(ON_INVALID_ENV));
        } catch (final IllegalArgumentException e) {
            throw new IllegalArgumentException(ON_INVALID_ENV + " must be fail or skip", e);
        }
        final int threads;
        try {
            threads = IndexImporter.Options.parseThreads(System.getenv(THREADS_ENV));
        } catch (final NumberFormatException e) {
            throw new IllegalArgumentException(THREADS_ENV + " must be a positive integer", e);
        }
        return new IndexImporter.Options(policy, threads, IndexImporter.Options.parseDedup(System.getenv(DEDUP_ENV)));
    }

    private static String seconds(final long startNanos) {
        return String.format(Locale.ROOT, "%.1f", (System.nanoTime() - startNanos) / 1e9);
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
