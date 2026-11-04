package net.orfeon.solr.importer.index;

import net.orfeon.solr.importer.convert.AvroToSolrDocumentConverter;
import net.orfeon.solr.importer.source.RecordReader;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.avro.generic.GenericRecord;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.RollbackUpdateCommand;
import org.apache.solr.update.UpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorChain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The import loop shared by the CLI and the request handler:
 * read records from each input file, convert them to Solr documents restricted to the fields
 * declared in the core's schema, and send them through the core's update processor chain, so that the
 * core's own update handler and IndexWriter write them (the path of an update request, minus HTTP).
 * The chain is the core's default one unless the params name another with "update.chain".
 *
 * With more than one thread the import is a pipeline: reader threads decode the Avro files into batches of
 * records, indexer threads convert the batches and add them, each through its own processor instance (Lucene's
 * IndexWriter indexes concurrently, one segment buffer per thread). The order in which documents reach the
 * index is then not defined, so with duplicated unique keys it is not defined either which of the duplicates
 * survives; use a single thread when that matters.
 */
public class IndexImporter {

    /**
     * What to do with a record that cannot be turned into a valid document
     * (typically a missing required field, or a value the field type cannot parse).
     */
    public enum InvalidRecordPolicy {
        /** Stop the import with an error. The default. */
        FAIL,
        /** Log the record and continue; the count is reported at the end. */
        SKIP;

        public static InvalidRecordPolicy parse(final String value) {
            if (value == null || value.isBlank()) {
                return FAIL;
            }
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        }
    }

    /**
     * Import settings.
     *
     * @param invalidRecordPolicy what to do with a record that violates the schema
     * @param threads number of indexer threads; 1 imports sequentially in file and record order
     * @param dedup true to replace an earlier document with the same uniqueKey (Lucene updateDocument);
     *              false to add every document as is, which is faster and correct when the input is known to
     *              have unique keys
     */
    public record Options(InvalidRecordPolicy invalidRecordPolicy, int threads, boolean dedup) {

        public static final Options DEFAULT = new Options(InvalidRecordPolicy.FAIL, defaultThreads(), true);

        public Options {
            Objects.requireNonNull(invalidRecordPolicy, "invalidRecordPolicy");
            if (threads < 1) {
                throw new IllegalArgumentException("threads must be at least 1: " + threads);
            }
        }

        public static int defaultThreads() {
            return Runtime.getRuntime().availableProcessors();
        }

        public static int parseThreads(final String value) {
            if (value == null || value.isBlank()) {
                return defaultThreads();
            }
            return Integer.parseInt(value.trim());
        }

        /**
         * Parses the dedup flag. Only "true" and "false" are accepted: silently treating any other
         * value as false would leave duplicates in the index without a warning.
         */
        public static boolean parseDedup(final String value) {
            if (value == null || value.isBlank()) {
                return true;
            }
            final String trimmed = value.trim();
            if (trimmed.equalsIgnoreCase("true")) {
                return true;
            }
            if (trimmed.equalsIgnoreCase("false")) {
                return false;
            }
            throw new IllegalArgumentException("dedup must be true or false: " + value);
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(IndexImporter.class);
    private static final int MAX_LOGGED_INVALID_RECORDS = 20;
    private static final int BATCH_SIZE = 256;
    private static final int QUEUED_BATCHES_PER_THREAD = 4;
    private static final long PROGRESS_EVERY = 100_000;
    private static final long QUEUE_WAIT_MILLIS = 100;

    /** Marks the end of the record stream for one indexer thread. */
    private static final Batch END = new Batch(null, List.of());

    private record Batch(String file, List<GenericRecord> records) {
    }

    private final SolrCore core;
    private final SolrParams params;
    private final UpdateRequestProcessorChain chain;
    private final boolean ignoreAutoCommit;
    private final Set<String> fieldNames;
    private final Options options;

    private final AtomicLong skipped = new AtomicLong();

    /**
     * @param core             the core to import into
     * @param params           request parameters for the update chain; "update.chain" selects a chain other than the
     *                         core's default one
     * @param options          import settings
     * @param ignoreAutoCommit true to keep the core's autoCommit / autoSoftCommit from committing while importing
     *                         (the CLI commits once at the end); false to let them work as for any update request
     */
    public IndexImporter(final SolrCore core, final SolrParams params, final Options options, final boolean ignoreAutoCommit) {
        this.core = core;
        this.params = params == null ? new ModifiableSolrParams() : params;
        // Resolved here so that an unknown chain fails before any file is read.
        this.chain = core.getUpdateProcessorChain(this.params);
        this.ignoreAutoCommit = ignoreAutoCommit;
        this.fieldNames = fieldNames(core.getLatestSchema());
        this.options = options;
    }

    public Options getOptions() {
        return options;
    }

    /**
     * Imports every file and returns the number of documents added. Does not commit.
     */
    public long importFiles(final List<String> files) throws IOException {
        final long start = System.nanoTime();
        final long skippedBefore = skipped.get();
        final long total = options.threads() == 1 ? importSequentially(files) : importInParallel(files);
        final long skippedNow = skipped.get() - skippedBefore;
        final double seconds = (System.nanoTime() - start) / 1e9;
        LOG.info("indexed {} documents from {} files in {} s ({} docs/s, {} threads, skipped {} invalid records)",
                total, files.size(), String.format(Locale.ROOT, "%.1f", seconds), rate(total, seconds), options.threads(), skippedNow);
        if (skippedNow > 0) {
            LOG.warn("skipped {} invalid records in total", skippedNow);
        }
        return total;
    }

    private long importSequentially(final List<String> files) throws IOException {
        long total = 0;
        try (final ChainWriter writer = new ChainWriter()) {
            for (final String file : files) {
                final long before = skipped.get();
                final long count = importFile(writer, file);
                LOG.info("indexed {} documents from {} (skipped {} invalid records)", count, file, skipped.get() - before);
                total += count;
            }
            writer.finish();
        }
        return total;
    }

    /**
     * Imports one file on the calling thread and returns the number of documents added.
     */
    private long importFile(final ChainWriter writer, final String file) throws IOException {
        long count = 0;
        try (final RecordReader reader = RecordReaders.open(file)) {
            while (reader.hasNext()) {
                if (indexRecord(writer, file, reader.next())) {
                    count++;
                    if (count % PROGRESS_EVERY == 0) {
                        LOG.info("indexed {} documents from {} so far", count, file);
                    }
                }
            }
        }
        return count;
    }

    private long importInParallel(final List<String> files) throws IOException {
        if (files.isEmpty()) {
            return 0;
        }
        final int threads = options.threads();
        final int readers = Math.min(threads, files.size());
        final BlockingQueue<Batch> queue = new ArrayBlockingQueue<>(threads * QUEUED_BATCHES_PER_THREAD);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final Queue<String> pending = new ConcurrentLinkedQueue<>(files);
        final Map<String, AtomicLong> perFile = new ConcurrentHashMap<>();
        final AtomicLong progress = new AtomicLong();
        final long start = System.nanoTime();

        final ExecutorService readerPool = Executors.newFixedThreadPool(readers, threadFactory("import-reader"));
        final ExecutorService indexerPool = Executors.newFixedThreadPool(threads, threadFactory("import-indexer"));
        try {
            final List<Future<Long>> indexing = new ArrayList<>(threads);
            for (int i = 0; i < threads; i++) {
                indexing.add(indexerPool.submit(() -> indexBatches(queue, failure, perFile, progress, start)));
            }
            final List<Future<?>> reading = new ArrayList<>(readers);
            for (int i = 0; i < readers; i++) {
                reading.add(readerPool.submit(() -> readFiles(pending, queue, failure)));
            }
            for (final Future<?> future : reading) {
                future.get();
            }
            for (int i = 0; i < threads; i++) {
                offer(queue, END, failure);
            }
            long total = 0;
            for (final Future<Long> future : indexing) {
                total += future.get();
            }
            rethrow(failure.get());
            for (final String file : files) {
                final AtomicLong count = perFile.get(file);
                LOG.info("indexed {} documents from {}", count == null ? 0 : count.get(), file);
            }
            return total;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("import interrupted", e);
        } catch (final ExecutionException e) {
            // Tasks record their failures in `failure`; a cause here is unexpected (e.g. an Error).
            rethrow(e.getCause());
            throw new IOException(e);
        } finally {
            readerPool.shutdownNow();
            indexerPool.shutdownNow();
        }
    }

    private void readFiles(final Queue<String> pending, final BlockingQueue<Batch> queue, final AtomicReference<Throwable> failure) {
        String file;
        while (failure.get() == null && (file = pending.poll()) != null) {
            try (final RecordReader reader = RecordReaders.open(file)) {
                List<GenericRecord> records = new ArrayList<>(BATCH_SIZE);
                while (failure.get() == null && reader.hasNext()) {
                    records.add(reader.next());
                    if (records.size() == BATCH_SIZE) {
                        offer(queue, new Batch(file, records), failure);
                        records = new ArrayList<>(BATCH_SIZE);
                    }
                }
                if (!records.isEmpty()) {
                    offer(queue, new Batch(file, records), failure);
                }
            } catch (final Throwable e) {
                failure.compareAndSet(null, e);
                return;
            }
        }
    }

    private long indexBatches(
            final BlockingQueue<Batch> queue,
            final AtomicReference<Throwable> failure,
            final Map<String, AtomicLong> perFile,
            final AtomicLong progress,
            final long start) {

        long count = 0;
        // Processor instances are not thread safe, so every indexer thread has its own.
        try (final ChainWriter writer = new ChainWriter()) {
            while (failure.get() == null) {
                final Batch batch = queue.poll(QUEUE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
                if (batch == null) {
                    continue;
                }
                if (batch == END) {
                    break;
                }
                long added = 0;
                for (final GenericRecord record : batch.records()) {
                    if (indexRecord(writer, batch.file(), record)) {
                        added++;
                    }
                }
                count += added;
                perFile.computeIfAbsent(batch.file(), k -> new AtomicLong()).addAndGet(added);
                logProgress(progress, added, start);
            }
            if (failure.get() == null) {
                writer.finish();
            }
        } catch (final Throwable e) {
            failure.compareAndSet(null, e);
        }
        return count;
    }

    private static void logProgress(final AtomicLong progress, final long added, final long start) {
        final long before = progress.getAndAdd(added);
        final long after = before + added;
        if (before / PROGRESS_EVERY != after / PROGRESS_EVERY) {
            final double seconds = (System.nanoTime() - start) / 1e9;
            LOG.info("indexed {} documents so far ({} docs/s)", after, rate(after, seconds));
        }
    }

    /**
     * Blocks until the batch is queued, giving up when another thread has recorded a failure.
     */
    private static void offer(final BlockingQueue<Batch> queue, final Batch batch, final AtomicReference<Throwable> failure)
            throws InterruptedException {
        while (failure.get() == null) {
            if (queue.offer(batch, QUEUE_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
                return;
            }
        }
    }

    private static void rethrow(final Throwable failure) throws IOException {
        if (failure == null) {
            return;
        }
        if (failure instanceof IOException e) {
            throw e;
        }
        if (failure instanceof RuntimeException e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        throw new IOException(failure);
    }

    /**
     * Converts and indexes one record. Returns false when the record was skipped under the SKIP policy.
     */
    private boolean indexRecord(final ChainWriter writer, final String file, final GenericRecord record) throws IOException {
        try {
            writer.add(AvroToSolrDocumentConverter.convert(record, fieldNames));
            return true;
        } catch (final RuntimeException e) {
            if (isServerError(e)) {
                // Not a fault of the record (e.g. the IndexWriter was closed by a tragic event, or the transaction
                // log could not be written): skipping would count every following record as invalid.
                throw e;
            }
            // The schema is applied (DocumentBuilder) and Lucene rejects some documents (e.g. a term longer than
            // 32766 bytes) inside the update handler, so the whole processAdd call is guarded.
            handleInvalidRecord(file, record, e);
            return false;
        }
    }

    /**
     * The update handler reports problems of a document as 4xx (BAD_REQUEST) and failures of the index itself as 5xx.
     */
    private static boolean isServerError(final RuntimeException e) {
        return e instanceof SolrException solrException && solrException.code() >= 500;
    }

    /**
     * Deletes every document of the core through the update chain (the CLI replaces the whole index).
     */
    public void deleteAll() throws IOException {
        try (final ChainWriter writer = new ChainWriter()) {
            writer.deleteAll();
            writer.finish();
        }
    }

    /**
     * Commits through the update chain.
     *
     * @param mergeToOneSegment true to merge the index down to one segment before the commit (an optimize)
     * @param openSearcher      true to open a new searcher so that the imported documents become visible
     */
    public void commit(final boolean mergeToOneSegment, final boolean openSearcher) throws IOException {
        try (final ChainWriter writer = new ChainWriter()) {
            writer.commit(mergeToOneSegment, openSearcher);
            writer.finish();
        }
    }

    /**
     * Discards every change since the last commit through the update chain. The CLI's way out of a failed import:
     * closing the core would otherwise commit the deletion and the documents added so far (Lucene's IndexWriter
     * commits on close). Not for the handler: the core's writer also holds the uncommitted changes of every other
     * update request.
     */
    public void rollback() throws IOException {
        try (final ChainWriter writer = new ChainWriter()) {
            writer.rollback();
            writer.finish();
        }
    }

    /**
     * One processor instance of the update chain together with the request it belongs to; used by one thread.
     * finish() is called after successful processing only, close() always (as Solr's update handlers do).
     */
    private final class ChainWriter implements Closeable {

        private final SolrQueryRequest request;
        private final UpdateRequestProcessor processor;

        ChainWriter() {
            this.request = new SolrQueryRequestBase(core, params);
            boolean created = false;
            try {
                this.processor = Objects.requireNonNull(
                        chain.createProcessor(request, new SolrQueryResponse()), "the update chain created no processor");
                created = true;
            } finally {
                if (!created) {
                    request.close();
                }
            }
        }

        void add(final SolrInputDocument document) throws IOException {
            final AddUpdateCommand add = flagged(new AddUpdateCommand(request));
            add.solrDoc = document;
            // overwrite: a later document with the same uniqueKey replaces the earlier one (Lucene updateDocument);
            // without it the update handler adds the document as is (addDocument).
            add.overwrite = options.dedup();
            processor.processAdd(add);
        }

        void deleteAll() throws IOException {
            final DeleteUpdateCommand delete = flagged(new DeleteUpdateCommand(request));
            delete.query = "*:*";
            processor.processDelete(delete);
        }

        void commit(final boolean mergeToOneSegment, final boolean openSearcher) throws IOException {
            final CommitUpdateCommand commit = new CommitUpdateCommand(request, mergeToOneSegment);
            if (mergeToOneSegment) {
                commit.maxOptimizeSegments = 1;
            }
            commit.openSearcher = openSearcher;
            commit.waitSearcher = openSearcher;
            processor.processCommit(commit);
        }

        void rollback() throws IOException {
            processor.processRollback(new RollbackUpdateCommand(request));
        }

        /**
         * Applies the import-wide flags. Every add and delete needs them: a delete without IGNORE_AUTOCOMMIT schedules
         * an autoCommit / autoSoftCommit just as an add does.
         */
        private <T extends UpdateCommand> T flagged(final T command) {
            if (ignoreAutoCommit) {
                command.setFlags(command.getFlags() | UpdateCommand.IGNORE_AUTOCOMMIT);
            }
            return command;
        }

        void finish() throws IOException {
            processor.finish();
        }

        @Override
        public void close() throws IOException {
            try {
                processor.close();
            } finally {
                request.close();
            }
        }
    }

    /**
     * Number of records skipped so far under the SKIP policy.
     */
    public long getSkipped() {
        return skipped.get();
    }

    private void handleInvalidRecord(final String file, final GenericRecord record, final RuntimeException cause) {
        if (options.invalidRecordPolicy() == InvalidRecordPolicy.FAIL) {
            throw new IllegalArgumentException("invalid record in " + file + ": " + cause.getMessage() + " record=" + record, cause);
        }
        final long count = skipped.incrementAndGet();
        if (count <= MAX_LOGGED_INVALID_RECORDS) {
            LOG.warn("skipping invalid record in {}: {} record={}", file, cause.getMessage(), record);
        } else if (count == MAX_LOGGED_INVALID_RECORDS + 1) {
            LOG.warn("further invalid records are not logged individually");
        }
    }

    private static String rate(final long count, final double seconds) {
        return seconds <= 0 ? "-" : String.format(Locale.ROOT, "%.0f", count / seconds);
    }

    private static ThreadFactory threadFactory(final String prefix) {
        final AtomicInteger n = new AtomicInteger();
        return runnable -> {
            final Thread thread = new Thread(runnable, prefix + "-" + n.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * Explicit field names of the schema. Dynamic fields are not expanded, so only explicitly declared
     * fields are imported.
     */
    static Set<String> fieldNames(final IndexSchema schema) {
        return Set.copyOf(schema.getFields().keySet());
    }

}
