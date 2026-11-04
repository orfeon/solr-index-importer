package net.orfeon.solr.importer.handler;

import net.orfeon.solr.importer.index.IndexImporter;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.security.AuthorizationContext;

import java.util.List;

/**
 * Request handler variant of the importer: imports the Avro files under the "path" parameter
 * (local path or gs:// prefix) into the core the request was made against. The documents go through the
 * core's update processor chain like those of an update request, are added to the existing ones and are
 * committed (with a new searcher, so they are visible when the request returns).
 * Optional parameters: onInvalid (fail or skip), threads (indexer threads, default: available processors),
 * dedup (false to add documents without replacing earlier ones with the same uniqueKey),
 * update.chain (the update chain to use, default: the core's default chain).
 * The core's autoCommit settings apply while importing, as for any update request.
 */
public class AvroImportHandler extends RequestHandlerBase {

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

        final IndexImporter importer = new IndexImporter(request.getCore(), request.getParams(), options, false);
        final long count = importer.importFiles(files);
        importer.commit(false, true);

        response.add("source", source);
        response.add("files", files.size());
        response.add("documents", count);
        response.add("skipped", importer.getSkipped());
    }

    @Override
    public String getDescription() {
        return "imports Avro files into the index";
    }

    @Override
    public Name getPermissionName(final AuthorizationContext authorizationContext) {
        return Name.ALL;
    }

}
