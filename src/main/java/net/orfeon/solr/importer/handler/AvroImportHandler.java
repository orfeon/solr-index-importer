package net.orfeon.solr.importer.handler;

import net.orfeon.solr.importer.index.IndexImporter;
import net.orfeon.solr.importer.index.IndexWriters;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.update.SolrIndexWriter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Request handler variant of the importer: imports the Avro files under the "path" parameter
 * (local path or gs:// prefix) into the core the request was made against, writing to the index directly.
 * Optional parameters: onInvalid (fail or skip), threads (indexer threads, default: available processors),
 * dedup (false to add documents without replacing earlier ones with the same uniqueKey).
 */
public class AvroImportHandler extends RequestHandlerBase {

    /** One IndexWriter per core: opening a second writer on the same index would corrupt it. */
    private transient Map<String, SolrIndexWriter> writers;

    @Override
    public void init(final NamedList<?> args) {
        super.init(args);
        this.writers = new HashMap<>();
    }

    @Override
    public void handleRequestBody(final SolrQueryRequest request, final SolrQueryResponse response) throws Exception {
        final String source = request.getParams() == null ? null : request.getParams().get("path");
        if (source == null) {
            throw new IllegalArgumentException("Required parameter path is missing");
        }
        final List<String> files = RecordReaders.listSupported(source);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("no supported input files found under: " + source);
        }

        final IndexImporter.Options options = new IndexImporter.Options(
                IndexImporter.InvalidRecordPolicy.parse(request.getParams().get("onInvalid")),
                IndexImporter.Options.parseThreads(request.getParams().get("threads")),
                IndexImporter.Options.parseDedup(request.getParams().get("dedup")));

        final SolrIndexWriter writer = writer(request.getCore());
        final long count;
        final long skipped;
        // The writer is shared by every request against this core, so imports into one core are serialised.
        synchronized (writer) {
            final IndexImporter importer = new IndexImporter(request.getCore(), writer, options);
            count = importer.importFiles(files);
            importer.commit();
            skipped = importer.getSkipped();
        }
        response.add("source", source);
        response.add("files", files.size());
        response.add("documents", count);
        response.add("skipped", skipped);
    }

    @Override
    public String getDescription() {
        return "imports Avro files into the index";
    }

    @Override
    public Name getPermissionName(final AuthorizationContext authorizationContext) {
        return Name.ALL;
    }

    private synchronized SolrIndexWriter writer(final SolrCore core) throws Exception {
        SolrIndexWriter writer = this.writers.get(core.getName());
        if (writer == null) {
            writer = IndexWriters.create(core, false);
            this.writers.put(core.getName(), writer);
        }
        return writer;
    }

}
