package net.orfeon.solr.importer.index;

import net.orfeon.solr.importer.convert.AvroToSolrDocumentConverter;
import net.orfeon.solr.importer.source.RecordReader;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.avro.generic.GenericRecord;
import org.apache.lucene.document.Document;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.DocumentBuilder;
import org.apache.solr.update.SolrIndexWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * The import loop shared by the CLI and the request handler:
 * read records from each input file, convert them to Solr documents restricted to the fields
 * declared in the core's schema, and add them to the index writer.
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

    private static final Logger LOG = LoggerFactory.getLogger(IndexImporter.class);
    private static final int MAX_LOGGED_INVALID_RECORDS = 20;

    private final SolrCore core;
    private final SolrIndexWriter writer;
    private final Set<String> fieldNames;
    private final InvalidRecordPolicy invalidRecordPolicy;

    private long skipped = 0;

    public IndexImporter(final SolrCore core, final SolrIndexWriter writer) {
        this(core, writer, InvalidRecordPolicy.FAIL);
    }

    public IndexImporter(final SolrCore core, final SolrIndexWriter writer, final InvalidRecordPolicy invalidRecordPolicy) {
        this.core = core;
        this.writer = writer;
        this.fieldNames = fieldNames(core.getLatestSchema());
        this.invalidRecordPolicy = invalidRecordPolicy;
    }

    /**
     * Imports every file and returns the number of documents added. Does not commit.
     */
    public long importFiles(final List<String> files) throws IOException {
        long total = 0;
        for (final String file : files) {
            final long before = skipped;
            final long count = importFile(file);
            LOG.info("indexed {} documents from {} (skipped {} invalid records)", count, file, skipped - before);
            total += count;
        }
        if (skipped > 0) {
            LOG.warn("skipped {} invalid records in total", skipped);
        }
        return total;
    }

    public long importFile(final String file) throws IOException {
        final IndexSchema schema = core.getLatestSchema();
        long count = 0;
        try (final RecordReader reader = RecordReaders.open(file)) {
            while (reader.hasNext()) {
                final GenericRecord record = reader.next();
                final Document doc;
                try {
                    final SolrInputDocument solrDoc = AvroToSolrDocumentConverter.convert(record, fieldNames);
                    doc = DocumentBuilder.toDocument(solrDoc, schema);
                } catch (final RuntimeException e) {
                    handleInvalidRecord(file, record, e);
                    continue;
                }
                writer.addDocument(doc);
                count++;
            }
        }
        return count;
    }

    public long commit() throws IOException {
        return writer.commit();
    }

    /**
     * Number of records skipped so far under the SKIP policy.
     */
    public long getSkipped() {
        return skipped;
    }

    private void handleInvalidRecord(final String file, final GenericRecord record, final RuntimeException cause) {
        if (invalidRecordPolicy == InvalidRecordPolicy.FAIL) {
            throw new IllegalArgumentException("invalid record in " + file + ": " + cause.getMessage() + " record=" + record, cause);
        }
        skipped++;
        if (skipped <= MAX_LOGGED_INVALID_RECORDS) {
            LOG.warn("skipping invalid record in {}: {} record={}", file, cause.getMessage(), record);
        } else if (skipped == MAX_LOGGED_INVALID_RECORDS + 1) {
            LOG.warn("further invalid records are not logged individually");
        }
    }

    /**
     * Explicit field names of the schema. Dynamic fields are not expanded, so only explicitly declared
     * fields are imported.
     */
    static Set<String> fieldNames(final IndexSchema schema) {
        return Set.copyOf(schema.getFields().keySet());
    }

}
