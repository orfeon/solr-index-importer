package net.orfeon.solr.importer.source;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Streams records out of a single data file.
 * Implementations exist per file format (currently Avro); the schema is exposed as an Avro schema
 * so that the rest of the importer stays format agnostic.
 */
public interface RecordReader extends Iterator<GenericRecord>, Closeable {

    Schema getSchema();

}
