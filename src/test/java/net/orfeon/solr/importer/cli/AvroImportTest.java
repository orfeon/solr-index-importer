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
import static org.junit.Assert.assertThrows;

/**
 * Runs the CLI import against the example "books" core (example/conf, Solr's default native lock) and checks the
 * index it leaves behind after the container has been shut down.
 */
public class AvroImportTest {

    private static final String SAMPLE = "example/data/books.avro";
    private static final int SAMPLE_RECORDS = 1000;

    @Rule
    public final TemporaryFolder temp = new TemporaryFolder();

    private final IndexImporter.Options options = new IndexImporter.Options(IndexImporter.InvalidRecordPolicy.FAIL, 4, true);

    @Test
    public void importReplacesTheIndexWithOneMergedSegment() throws IOException {
        final Path home = booksHome();
        final Path core = home.resolve("books");

        assertEquals(SAMPLE_RECORDS, AvroImport.run(home, "books", SAMPLE, options, null));
        assertIndex(core.resolve("data/index"));

        // A second run replaces the documents instead of adding to them.
        assertEquals(SAMPLE_RECORDS, AvroImport.run(home, "books", SAMPLE, options, ""));
        assertIndex(core.resolve("data/index"));
        assertFalse(Files.exists(core.resolve("data/tlog")));
    }

    @Test
    public void failedImportLeavesTheCommittedIndexUntouched() throws IOException {
        final Path home = booksHome();
        assertEquals(SAMPLE_RECORDS, AvroImport.run(home, "books", SAMPLE, options, null));

        // The second run deletes everything and adds some documents before the broken file fails it. Closing the
        // core must not commit that state (Lucene's IndexWriter commits on close).
        final Path data = temp.newFolder("data").toPath();
        Files.copy(Path.of(SAMPLE), data.resolve("books.avro"));
        Files.write(data.resolve("broken.avro"), new byte[] {1, 2, 3, 4});
        assertThrows(IOException.class, () -> AvroImport.run(home, "books", data.toString(), options, null));

        assertIndex(home.resolve("books/data/index"));
    }

    private Path booksHome() throws IOException {
        final Path home = temp.newFolder("solr-home").toPath();
        Files.copy(Path.of("docker/solr.xml"), home.resolve("solr.xml"));
        final Path core = Files.createDirectories(home.resolve("books"));
        Files.writeString(core.resolve("core.properties"), "name=books\n");
        copyTree(Path.of("example/conf"), core.resolve("conf"));
        return home;
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
