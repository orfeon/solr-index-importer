package net.orfeon.solr.importer.source;

import net.orfeon.solr.importer.storage.GcsStorage;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RecordReadersTest {

    private static final Schema SCHEMA = SchemaBuilder.record("doc").fields()
            .requiredString("id")
            .requiredLong("value")
            .endRecord();

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void listSupportedWalksDirectoryAndFiltersByExtension() throws IOException {
        final File root = folder.newFolder("input");
        final File nested = new File(root, "nested");
        assertTrue(nested.mkdirs());
        writeAvro(new File(root, "a.avro"), 2);
        writeAvro(new File(nested, "b.avro"), 3);
        Files.writeString(new File(root, "ignore.txt").toPath(), "not avro");
        Files.writeString(new File(root, "skipped.avro_").toPath(), "renamed away");

        final List<String> files = RecordReaders.listSupported(root.getPath());

        assertEquals(2, files.size());
        assertTrue(files.get(0).endsWith("a.avro"));
        assertTrue(files.get(1).endsWith("b.avro"));
    }

    @Test
    public void listSupportedAcceptsSingleFile() throws IOException {
        final File file = new File(folder.newFolder("single"), "only.avro");
        writeAvro(file, 1);

        assertEquals(List.of(file.getPath()), RecordReaders.listSupported(file.getPath()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void listSupportedRejectsMissingPath() throws IOException {
        RecordReaders.listSupported(new File(folder.getRoot(), "missing").getPath());
    }

    @Test
    public void openStreamsRecordsFromAvroFile() throws IOException {
        final File file = new File(folder.newFolder("read"), "data.avro");
        writeAvro(file, 5);

        final List<GenericRecord> records = new ArrayList<>();
        try (final RecordReader reader = RecordReaders.open(file.getPath())) {
            assertEquals(SCHEMA, reader.getSchema());
            while (reader.hasNext()) {
                records.add(reader.next());
            }
        }

        assertEquals(5, records.size());
        assertEquals("id-3", records.get(3).get("id").toString());
        assertEquals(3L, records.get(3).get("value"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void openRejectsUnsupportedExtension() throws IOException {
        final File file = new File(folder.newFolder("bad"), "data.parquet");
        Files.writeString(file.toPath(), "x");
        RecordReaders.open(file.getPath());
    }

    @Test
    public void gcsUrisAreRoutedToCloudStorage() {
        assertTrue(GcsStorage.isGcsPath("gs://bucket/dir/file.avro"));
        assertFalse(GcsStorage.isGcsPath("/local/file.avro"));
        assertTrue(RecordReaders.isSupported("gs://bucket/dir/file.avro"));
    }

    private static void writeAvro(final File file, final int count) throws IOException {
        try (final DataFileWriter<GenericRecord> writer = new DataFileWriter<>(new GenericDatumWriter<>(SCHEMA))) {
            writer.create(SCHEMA, file);
            for (int i = 0; i < count; i++) {
                final GenericRecord record = new GenericData.Record(SCHEMA);
                record.put("id", "id-" + i);
                record.put("value", (long) i);
                writer.append(record);
            }
        }
    }

}
