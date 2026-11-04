package net.orfeon.solr.importer.index;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.request.SolrQueryRequestBase;
import org.apache.solr.response.SolrQueryResponse;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Imports into a real core (src/test/resources/solr-home, Solr's default native lock) through the update chain.
 */
public class IndexImporterTest {

    @ClassRule
    public static final TemporaryFolder TEMP = new TemporaryFolder();

    private static final Schema ITEM = SchemaBuilder.record("Item").fields()
            .requiredString("id")
            .optionalString("name")
            .optionalLong("price")
            .endRecord();

    private static CoreContainer container;
    private static SolrCore core;

    @BeforeClass
    public static void startCore() throws IOException {
        final Path home = TEMP.newFolder("solr-home").toPath();
        copyTree(Path.of("src/test/resources/solr-home"), home);
        container = CoreContainer.createAndLoad(home);
        core = container.getCore("items");
    }

    @AfterClass
    public static void stopCore() {
        if (core != null) {
            core.close();
        }
        if (container != null) {
            container.shutdown();
        }
    }

    @Before
    public void emptyIndex() throws IOException {
        final IndexImporter importer = importer(new ModifiableSolrParams(), IndexImporter.InvalidRecordPolicy.FAIL, 1, true);
        importer.deleteAll();
        importer.commit(false, true);
    }

    @Test
    public void laterRecordReplacesEarlierOneWithSameKey() throws IOException {
        final String file = avro(item("a", "first", 1L), item("b", "other", 2L), item("a", "second", 3L));
        final IndexImporter importer = importer(new ModifiableSolrParams(), IndexImporter.InvalidRecordPolicy.FAIL, 1, true);

        assertEquals(3, importer.importFiles(List.of(file)));
        importer.commit(false, true);

        assertEquals(2, numDocs());
        assertEquals(0, count(new TermQuery(new Term("name", "first"))));
        assertEquals(1, count(new TermQuery(new Term("name", "second"))));
    }

    @Test
    public void withoutDedupEveryRecordIsAdded() throws IOException {
        final String file = avro(item("a", "first", 1L), item("a", "second", 2L));
        final IndexImporter importer = importer(new ModifiableSolrParams(), IndexImporter.InvalidRecordPolicy.FAIL, 1, false);

        importer.importFiles(List.of(file));
        importer.commit(false, true);

        assertEquals(2, numDocs());
    }

    @Test
    public void invalidRecordFailsTheImportByDefault() throws IOException {
        // name is required by the schema
        final String file = avro(item("a", "ok", 1L), item("b", null, 2L));
        final IndexImporter importer = importer(new ModifiableSolrParams(), IndexImporter.InvalidRecordPolicy.FAIL, 1, true);

        final IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> importer.importFiles(List.of(file)));
        assertTrue(e.getMessage(), e.getMessage().contains("invalid record"));
    }

    @Test
    public void invalidRecordIsSkippedAndCountedUnderSkipPolicy() throws IOException {
        final String file = avro(item("a", "ok", 1L), item("b", null, 2L), item("c", "ok", 3L));
        final IndexImporter importer = importer(new ModifiableSolrParams(), IndexImporter.InvalidRecordPolicy.SKIP, 2, true);

        assertEquals(2, importer.importFiles(List.of(file)));
        importer.commit(false, true);

        assertEquals(1, importer.getSkipped());
        assertEquals(2, numDocs());
    }

    @Test
    public void parallelImportOfSeveralFiles() throws IOException {
        final List<String> files = new ArrayList<>();
        for (int f = 0; f < 3; f++) {
            final List<GenericRecord> records = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                records.add(item("f" + f + "-" + i, "name" + i, (long) i));
            }
            files.add(avro(records.toArray(GenericRecord[]::new)));
        }
        final IndexImporter importer = importer(new ModifiableSolrParams(), IndexImporter.InvalidRecordPolicy.FAIL, 4, true);

        assertEquals(3000, importer.importFiles(files));
        importer.commit(true, true);

        assertEquals(3000, numDocs());
        assertEquals(1, (int) core.withSearcher(s -> s.getIndexReader().leaves().size()));
    }

    @Test
    public void namedUpdateChainIsApplied() throws IOException {
        final String file = avro(item("a", "one", 1L), item("b", "two", 2L));

        final ModifiableSolrParams defaultChain = new ModifiableSolrParams();
        final IndexImporter plain = importer(defaultChain, IndexImporter.InvalidRecordPolicy.FAIL, 1, true);
        plain.importFiles(List.of(file));
        plain.commit(false, true);
        assertEquals(0, count(new TermQuery(new Term("origin", "avro"))));

        final ModifiableSolrParams withOrigin = new ModifiableSolrParams();
        withOrigin.set("update.chain", "with-origin");
        final IndexImporter chained = importer(withOrigin, IndexImporter.InvalidRecordPolicy.FAIL, 2, true);
        chained.importFiles(List.of(file));
        chained.commit(false, true);
        assertEquals(2, count(new TermQuery(new Term("origin", "avro"))));
    }

    @Test
    public void unknownUpdateChainIsRejectedUpFront() {
        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("update.chain", "no-such-chain");
        assertThrows(SolrException.class,
                () -> importer(params, IndexImporter.InvalidRecordPolicy.FAIL, 1, true));
    }

    @Test
    public void handlerImportIsVisibleWhenTheRequestReturns() throws IOException {
        final String file = avro(item("a", "one", 1L), item("b", "two", 2L), item("c", "three", 3L));
        final ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("path", file);
        params.set("threads", "2");
        final SolrQueryResponse response = new SolrQueryResponse();
        try (SolrQueryRequest request = new SolrQueryRequestBase(core, params)) {
            core.getRequestHandler("/import/avro").handleRequest(request, response);
        }

        assertNull(response.getException());
        final NamedList<Object> values = response.getValues();
        assertEquals(3L, values.get("documents"));
        assertEquals(0L, values.get("skipped"));
        assertEquals(3, numDocs());
    }

    private static IndexImporter importer(
            final ModifiableSolrParams params,
            final IndexImporter.InvalidRecordPolicy policy,
            final int threads,
            final boolean dedup) {
        return new IndexImporter(core, params, new IndexImporter.Options(policy, threads, dedup), true);
    }

    private static int numDocs() throws IOException {
        return core.withSearcher(s -> s.getIndexReader().numDocs());
    }

    private static int count(final Query query) throws IOException {
        return core.withSearcher(s -> s.count(query));
    }

    private static GenericRecord item(final String id, final String name, final Long price) {
        final GenericRecord record = new GenericData.Record(ITEM);
        record.put("id", id);
        record.put("name", name);
        record.put("price", price);
        return record;
    }

    private static String avro(final GenericRecord... records) throws IOException {
        final File file = TEMP.newFile("items-" + System.nanoTime() + ".avro");
        try (DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(ITEM))) {
            writer.create(ITEM, file);
            for (final GenericRecord record : records) {
                writer.append(record);
            }
        }
        return file.getAbsolutePath();
    }

    static void copyTree(final Path from, final Path to) throws IOException {
        try (Stream<Path> paths = Files.walk(from)) {
            for (final Path path : paths.toList()) {
                final Path target = to.resolve(from.relativize(path).toString());
                if (Files.isDirectory(path)) {
                    Files.createDirectories(target);
                } else {
                    Files.copy(path, target);
                }
            }
        }
    }

}
