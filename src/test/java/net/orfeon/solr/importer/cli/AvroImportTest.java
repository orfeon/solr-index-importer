package net.orfeon.solr.importer.cli;

import net.orfeon.solr.importer.index.IndexImporter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.store.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Runs the CLI import against the example "books" core (example/conf, Solr's default native lock) and checks the
 * index it leaves behind after the container has been shut down.
 */
public class AvroImportTest {

    private static final String SAMPLE = "example/data/books.avro";
    private static final int SAMPLE_RECORDS = 1000;

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    @Test
    public void importReplacesTheIndexWithOneMergedSegment() throws IOException {
        final Path home = temp.newFolder("solr-home").toPath();
        Files.copy(Path.of("docker/solr.xml"), home.resolve("solr.xml"));
        final Path core = Files.createDirectories(home.resolve("books"));
        Files.writeString(core.resolve("core.properties"), "name=books\n");
        copyTree(Path.of("example/conf"), core.resolve("conf"));
        final IndexImporter.Options options = new IndexImporter.Options(IndexImporter.InvalidRecordPolicy.FAIL, 4, true);

        assertEquals(SAMPLE_RECORDS, AvroImport.run(home, "books", SAMPLE, options, null));
        assertIndex(core.resolve("data/index"));

        // A second run replaces the documents instead of adding to them.
        assertEquals(SAMPLE_RECORDS, AvroImport.run(home, "books", SAMPLE, options, ""));
        assertIndex(core.resolve("data/index"));
        assertFalse(Files.exists(core.resolve("data/tlog")));
    }

    private static void assertIndex(final Path indexDir) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(indexDir))) {
            assertEquals(SAMPLE_RECORDS, reader.numDocs());
            assertEquals(SAMPLE_RECORDS, reader.maxDoc());
            assertEquals(1, reader.leaves().size());
        }
    }

    private static void copyTree(final Path from, final Path to) throws IOException {
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
