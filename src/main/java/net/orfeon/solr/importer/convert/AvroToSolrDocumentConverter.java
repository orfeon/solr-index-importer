package net.orfeon.solr.importer.convert;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.solr.common.SolrInputDocument;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;

/**
 * Converts Avro records into SolrInputDocuments.
 *
 * Field naming: nested records become child documents whose fields are named "parent.child".
 * Null values (and null elements of arrays) are omitted, so a document without a value for a field
 * simply has no such field in the index.
 * When a collection of field names is given, only leaf fields whose (dotted) name is in it are emitted;
 * this is used to restrict output to fields that exist in the Solr schema. Nested records are always
 * descended into, and a child document that ends up with no fields is dropped.
 */
public final class AvroToSolrDocumentConverter {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ISO_LOCAL_TIME;

    private AvroToSolrDocumentConverter() {
    }

    public static SolrInputDocument convert(final GenericRecord record) {
        return convert(record.getSchema(), record, null, null);
    }

    public static SolrInputDocument convert(final GenericRecord record, final Collection<String> fieldNames) {
        return convert(record.getSchema(), record, null, fieldNames);
    }

    public static SolrInputDocument convert(
            final Schema schema,
            final GenericRecord record,
            final String parentName,
            final Collection<String> fieldNames) {

        final SolrInputDocument doc = new SolrInputDocument();
        for (final Schema.Field field : schema.getFields()) {
            final String name = parentName == null ? field.name() : parentName + "." + field.name();
            final Object value = record.hasField(field.name()) ? record.get(field.name()) : null;
            if (value == null) {
                continue;
            }
            addValue(doc, name, AvroSchemas.unnestUnion(field.schema()), value, fieldNames);
        }
        return doc;
    }

    private static void addValue(
            final SolrInputDocument doc,
            final String name,
            final Schema schema,
            final Object value,
            final Collection<String> fieldNames) {

        switch (schema.getType()) {
            case RECORD -> {
                final SolrInputDocument child = convert(schema, (GenericRecord) value, name, fieldNames);
                if (!child.isEmpty() || child.hasChildDocuments()) {
                    doc.addChildDocument(child);
                }
            }
            case ARRAY -> {
                final Schema elementSchema = AvroSchemas.unnestUnion(schema.getElementType());
                for (final Object element : (Collection<?>) value) {
                    if (element != null) {
                        addValue(doc, name, elementSchema, element, fieldNames);
                    }
                }
            }
            case MAP, NULL, UNION -> {
                // Maps have no natural Solr field representation; unions were unnested by the caller.
            }
            default -> {
                // The field-name filter applies to leaf fields only, so nested records are always descended into.
                if (fieldNames == null || fieldNames.contains(name)) {
                    doc.addField(name, toSolrValue(schema, value));
                }
            }
        }
    }

    /**
     * Converts a scalar Avro value to the Java type Solr expects for the matching field type.
     * Logical date and timestamp types become java.util.Date (UTC); time types become ISO local time strings.
     */
    static Object toSolrValue(final Schema schema, final Object value) {
        final LogicalType logicalType = schema.getLogicalType();
        return switch (schema.getType()) {
            case BOOLEAN, FLOAT, DOUBLE -> value;
            case ENUM, STRING -> value.toString();
            case FIXED -> ((GenericData.Fixed) value).bytes();
            case BYTES -> toBytes((ByteBuffer) value);
            case INT -> {
                final int intValue = (Integer) value;
                if (LogicalTypes.date().equals(logicalType)) {
                    yield Date.from(LocalDate.ofEpochDay(intValue).atStartOfDay().toInstant(ZoneOffset.UTC));
                }
                if (LogicalTypes.timeMillis().equals(logicalType)) {
                    yield LocalTime.ofNanoOfDay(intValue * 1_000_000L).format(TIME_FORMAT);
                }
                yield intValue;
            }
            case LONG -> {
                final long longValue = (Long) value;
                if (LogicalTypes.timestampMillis().equals(logicalType)) {
                    yield new Date(longValue);
                }
                if (LogicalTypes.timestampMicros().equals(logicalType)) {
                    yield new Date(longValue / 1000L);
                }
                if (LogicalTypes.timeMicros().equals(logicalType)) {
                    yield LocalTime.ofNanoOfDay(longValue * 1_000L).format(TIME_FORMAT);
                }
                yield longValue;
            }
            default -> throw new IllegalArgumentException("not a scalar schema: " + schema);
        };
    }

    private static byte[] toBytes(final ByteBuffer buffer) {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] bytes = new byte[duplicate.remaining()];
        duplicate.get(bytes);
        return bytes;
    }

}
