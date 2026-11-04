package net.orfeon.solr.importer.index;

import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;

/**
 * Test processor of the "server-error" chain: fails the document with id "server-error" with a 500, the way the
 * update handler reports an IndexWriter closed by a tragic event.
 */
public class ServerErrorUpdateProcessorFactory extends UpdateRequestProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(
            final SolrQueryRequest request, final SolrQueryResponse response, final UpdateRequestProcessor next) {
        return new UpdateRequestProcessor(next) {
            @Override
            public void processAdd(final AddUpdateCommand command) throws IOException {
                if ("server-error".equals(command.getSolrInputDocument().getFieldValue("id"))) {
                    throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "simulated index failure");
                }
                super.processAdd(command);
            }
        };
    }

}
