package net.orfeon.solr.importer.convert;

import org.apache.avro.Schema;

/**
 * Small helpers over Avro schemas shared by the document converter and the schema generator.
 */
public final class AvroSchemas {

    private AvroSchemas() {
    }

    /**
     * Returns the non-null branch of a nullable union, or the schema itself if it is not a union.
     */
    public static Schema unnestUnion(final Schema schema) {
        if (schema.getType() != Schema.Type.UNION) {
            return schema;
        }
        return schema.getTypes().stream()
                .filter(s -> s.getType() != Schema.Type.NULL)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("UNION has no non-null branch: " + schema));
    }

    public static boolean isNullable(final Schema schema) {
        return schema.getType() == Schema.Type.UNION
                && schema.getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.NULL);
    }

}
