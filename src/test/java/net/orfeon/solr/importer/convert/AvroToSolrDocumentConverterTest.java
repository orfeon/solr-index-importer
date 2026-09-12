package net.orfeon.solr.importer.convert;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AvroToSolrDocumentConverterTest {

    private static final Schema CHILD = SchemaBuilder.record("child").fields()
            .requiredString("name")
            .optionalInt("age")
            .endRecord();

    private static final Schema SCHEMA = SchemaBuilder.record("doc").fields()
            .requiredString("id")
            .optionalString("title")
            .optionalLong("count")
            .name("date").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
            .name("ts").type().optional().type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
            .name("tags").type().array().items().stringType().noDefault()
            .name("nullableTags").type().optional().array().items().nullable().stringType()
            .name("payload").type().optional().bytesType()
            .name("price").type().optional().type(LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES)))
            .name("child").type().optional().type(CHILD)
            .name("children").type().array().items(CHILD).noDefault()
            .endRecord();

    @Test
    public void nullFieldsAreOmittedInsteadOfDefaulted() {
        final GenericRecord record = base();
        record.put("title", null);
        record.put("count", null);
        record.put("ts", null);
        record.put("nullableTags", null);
        record.put("payload", null);
        record.put("child", null);

        final SolrInputDocument doc = AvroToSolrDocumentConverter.convert(record);

        assertEquals("1", doc.getFieldValue("id"));
        assertFalse(doc.containsKey("title"));
        assertFalse(doc.containsKey("count"));
        assertFalse(doc.containsKey("ts"));
        assertFalse(doc.containsKey("nullableTags"));
        assertFalse(doc.containsKey("payload"));
        assertFalse(doc.containsKey("child"));
        assertFalse(doc.containsKey("child.name"));
        assertNull(doc.getChildDocuments());
    }

    @Test
    public void scalarsAndLogicalTypesAreConverted() {
        final GenericRecord record = base();
        record.put("title", "hello");
        record.put("count", 7L);
        record.put("ts", 1_700_000_000_000_000L);
        record.put("payload", ByteBuffer.wrap(new byte[]{1, 2, 3}));
        // BigQuery NUMERIC 12.34 exported as decimal(38, 9): unscaled 12340000000 in two's complement.
        record.put("price", ByteBuffer.wrap(new BigDecimal("12.340000000").unscaledValue().toByteArray()));

        final SolrInputDocument doc = AvroToSolrDocumentConverter.convert(record);

        assertEquals(new BigDecimal("12.340000000"), doc.getFieldValue("price"));
        assertEquals("hello", doc.getFieldValue("title"));
        assertEquals(7L, doc.getFieldValue("count"));
        assertEquals(new Date(19_000L * 24 * 60 * 60 * 1000), doc.getFieldValue("date"));
        assertEquals(new Date(1_700_000_000_000L), doc.getFieldValue("ts"));
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) doc.getFieldValue("payload"));
    }

    @Test
    public void arraysBecomeMultiValuedFieldsAndSkipNullElements() {
        final GenericRecord record = base();
        record.put("tags", List.of("a", "b"));
        record.put("nullableTags", Arrays.asList("x", null, "y"));

        final SolrInputDocument doc = AvroToSolrDocumentConverter.convert(record);

        assertEquals(List.of("a", "b"), List.copyOf(doc.getFieldValues("tags")));
        assertEquals(List.of("x", "y"), List.copyOf(doc.getFieldValues("nullableTags")));
    }

    @Test
    public void nestedRecordsAreFlattenedWithDottedNames() {
        final GenericRecord record = base();
        record.put("child", child("c", 3));
        record.put("children", List.of(child("d", null), child("e", 5)));

        final SolrInputDocument doc = AvroToSolrDocumentConverter.convert(record);

        // A plain IndexWriter cannot index Solr child documents, so nesting is expressed by field names only.
        assertNull(doc.getChildDocuments());
        assertEquals("c", doc.getFieldValue("child.name"));
        assertEquals(3, doc.getFieldValue("child.age"));
        assertEquals(List.of("d", "e"), List.copyOf(doc.getFieldValues("children.name")));
        assertEquals(List.of(5), List.copyOf(doc.getFieldValues("children.age")));
    }

    @Test
    public void fieldNamesRestrictOutputToSchemaFields() {
        final GenericRecord record = base();
        record.put("title", "hello");
        record.put("count", 7L);
        record.put("child", child("c", 3));

        final SolrInputDocument doc = AvroToSolrDocumentConverter.convert(record, Set.of("id", "count", "child.name"));

        assertTrue(doc.containsKey("id"));
        assertTrue(doc.containsKey("count"));
        assertFalse(doc.containsKey("title"));
        assertTrue(doc.containsKey("child.name"));
        assertFalse(doc.containsKey("child.age"));
    }

    private static GenericRecord base() {
        final GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", "1");
        record.put("date", 19_000);
        record.put("tags", List.of());
        record.put("children", List.of());
        return record;
    }

    private static GenericRecord child(final String name, final Integer age) {
        final GenericRecord record = new GenericData.Record(CHILD);
        record.put("name", name);
        record.put("age", age);
        return record;
    }

}
