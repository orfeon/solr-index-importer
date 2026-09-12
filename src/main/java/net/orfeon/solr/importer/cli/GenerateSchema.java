package net.orfeon.solr.importer.cli;

import net.orfeon.solr.importer.schema.SchemaGenerator;
import net.orfeon.solr.importer.source.RecordReader;
import net.orfeon.solr.importer.source.RecordReaders;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.List;

/**
 * Prints a schema.xml derived from the Avro schema of the first input file under the source.
 */
public final class GenerateSchema {

    private GenerateSchema() {
    }

    public static void main(final String[] args) throws IOException {
        if (args.length < 1) {
            throw new IllegalArgumentException("generateSchema requires args: <source> [coreName]");
        }
        final String source = args[0];
        final String coreName = args.length > 1 ? args[1] : "core";

        final List<String> files = RecordReaders.listSupported(source);
        if (files.isEmpty()) {
            throw new IllegalArgumentException("no supported input files found under: " + source);
        }

        final Schema schema;
        try (final RecordReader reader = RecordReaders.open(files.get(0))) {
            schema = reader.getSchema();
        }
        System.out.println(SchemaGenerator.generate(schema, coreName));
    }

}
