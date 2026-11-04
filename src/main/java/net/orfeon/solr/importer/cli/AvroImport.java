package net.orfeon.solr.importer.cli;

import net.orfeon.solr.importer.index.IndexImporter;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.UpdateParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.util.RefCounted;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

/**
 * Builds the index of a core from Avro files without going through HTTP: the documents are sent through the
 * core's update processor chain in-process and written by the core's own IndexWriter.
 * Input is a local file, a local directory, or a gs:// object / prefix.
 * Runs against SOLR_HOME (env SOLR_HOME, default /var/solr/data/) so that it can be executed
 * inside the Solr image at build time and the resulting index baked into the image.
 * The existing documents of the core are replaced, and the index is merged into one segment at the end.
 * When the import fails, the uncommitted changes are rolled back and the committed index stays as it was.
 * Records that do not satisfy the schema stop the import unless env IMPORT_ON_INVALID=skip.
 * IMPORT_THREADS sets the number of indexer threads (default: available processors), IMPORT_DEDUP=false
 * skips the per-document uniqueKey replacement when the input is known to have unique keys, and
 * IMPORT_UPDATE_CHAIN names the update chain to use (default: the core's default chain).
 */
public final class AvroImport {

    private static final String DEFAULT_SOLR_HOME = "/var/solr/data/";
    private static final String ON_INVALID_ENV = "IMPORT_ON_INVALID";
    private static final String THREADS_ENV = "IMPORT_THREADS";
    private static final String DEDUP_ENV = "IMPORT_DEDUP";
    private static final String UPDATE_CHAIN_ENV = "IMPORT_UPDATE_CHAIN";

    private static final Logger LOG = LoggerFactory.getLogger(AvroImport.class);

    private AvroImport() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 2) {
            throw new IllegalArgumentException("importAvro requires 2 args: <coreName> <source>. but " + args.length);
        }
        run(solrHome(), args[0], args[1], options(), System.getenv(UPDATE_CHAIN_ENV));
    }

    /**
     * Replaces the documents of the core with those of the Avro files under source, merges the index into one
     * segment and commits. Returns the number of documents imported.
     *
     * @param updateChain name of the update chain to use; null or blank for the core's default chain
     */
    public static long run(
            final Path solrHome,
            final String coreName,
            final String source,
            final IndexImporter.Options options,
            final String updateChain) throws IOException {

        final List<String> files = RecordReaders.listSupported(source);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("no supported input files found under: " + source);
        }
        LOG.info("found {} input files under {}", files.size(), source);
        if (!Files.isDirectory(solrHome)) {
            throw new IllegalArgumentException("solr home not found: " + solrHome);
        }

        final ModifiableSolrParams params = new ModifiableSolrParams();
        if (updateChain != null && !updateChain.isBlank()) {
            params.set(UpdateParams.UPDATE_CHAIN, updateChain.trim());
        }

        final CoreContainer container = CoreContainer.createAndLoad(solrHome);
        try {
            if (!container.getAllCoreNames().contains(coreName)) {
                throw new IllegalArgumentException("core " + coreName + " does not exist. cores: " + container.getAllCoreNames());
            }
            try (final SolrCore core = container.getCore(coreName)) {
                if (core.getUpdateHandler().getUpdateLog() != null) {
                    LOG.warn("core {} has an <updateLog>: every document is also written to the transaction log, which "
                            + "slows the import and leaves tlog files next to the index; remove <updateLog> from the "
                            + "solrconfig.xml used for the import", coreName);
                }
                // autoCommit is ignored while importing: the import commits once, after the final merge.
                final IndexImporter importer = new IndexImporter(core, params, options, true);
                LOG.info("importing with {} through update chain {}", importer.getOptions(),
                        params.get(UpdateParams.UPDATE_CHAIN, "(default)"));
                try {
                    return replaceAndCommit(core, coreName, importer, files);
                } catch (final Throwable e) {
                    // Closing the core commits whatever its IndexWriter holds (Lucene commits on close), which would
                    // replace the committed index with the deletion and the documents added so far.
                    try {
                        importer.rollback();
                        LOG.warn("import failed; rolled back, the committed index of core {} is unchanged", coreName);
                    } catch (final Throwable rollbackFailure) {
                        e.addSuppressed(rollbackFailure);
                    }
                    throw e;
                }
            }
        } finally {
            container.shutdown();
        }
    }

    private static long replaceAndCommit(
            final SolrCore core,
            final String coreName,
            final IndexImporter importer,
            final List<String> files) throws IOException {

        importer.deleteAll();
        // The index is merged down to one segment at the end anyway, so merging while indexing would
        // only rewrite segments that the final merge rewrites again. Merges are disabled during the
        // import and the configured policy is restored for the forced merge. After a failure the caller
        // rolls back, which replaces the writer with a new one that has the configured policy.
        final MergePolicy mergePolicy = setMergePolicy(core, NoMergePolicy.INSTANCE);
        final long total = importer.importFiles(files);
        setMergePolicy(core, mergePolicy);
        // An optimize commit merges first and then commits once, so the committed generation is the
        // merged index and the intermediate segments are not fsynced twice. No searcher is opened:
        // the container is shut down right after.
        final long commitStart = System.nanoTime();
        importer.commit(true, false);
        LOG.info("merged and committed {} documents into core {} in {} s (skipped {} invalid records)",
                total, coreName, seconds(commitStart), importer.getSkipped());
        LOG.info("index written to {}", core.getIndexDir());
        return total;
    }

    /**
     * Sets the merge policy of the core's IndexWriter (a live setting) and returns the previous one.
     */
    private static MergePolicy setMergePolicy(final SolrCore core, final MergePolicy policy) throws IOException {
        final RefCounted<IndexWriter> writer = core.getSolrCoreState().getIndexWriter(core);
        try {
            final MergePolicy previous = writer.get().getConfig().getMergePolicy();
            writer.get().getConfig().setMergePolicy(policy);
            return previous;
        } finally {
            writer.decref();
        }
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

}
