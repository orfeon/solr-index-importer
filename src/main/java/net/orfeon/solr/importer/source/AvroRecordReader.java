package net.orfeon.solr.importer.source;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads an Avro container file record by record without loading the whole file into memory.
 */
public class AvroRecordReader implements RecordReader {

    private final DataFileStream<GenericRecord> stream;

    public AvroRecordReader(final InputStream input) throws IOException {
        this.stream = new DataFileStream<>(input, new GenericDatumReader<>());
    }

    @Override
    public Schema getSchema() {
        return stream.getSchema();
    }

    @Override
    public boolean hasNext() {
        return stream.hasNext();
    }

    @Override
    public GenericRecord next() {
        return stream.next();
    }

    @Override
    public void close() throws IOException {
        stream.close();
    }

}
