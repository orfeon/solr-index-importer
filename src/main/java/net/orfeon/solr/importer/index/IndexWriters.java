package net.orfeon.solr.importer.index;

import org.apache.solr.core.SolrCore;
import org.apache.solr.update.SolrIndexWriter;

import java.io.IOException;

public final class IndexWriters {

    private IndexWriters() {
    }

    /**
     * Opens an IndexWriter on the core's index directory, outside Solr's update handler.
     *
     * @param create true to start a new index (existing segments are discarded), false to append
     */
    public static SolrIndexWriter create(final SolrCore core, final boolean create) throws IOException {
        return SolrIndexWriter.create(
                core,
                core.getName(),
                core.getIndexDir(),
                core.getDirectoryFactory(),
                create,
                core.getLatestSchema(),
                core.getSolrConfig().indexConfig,
                core.getDeletionPolicy(),
                core.getCodec());
    }

}
