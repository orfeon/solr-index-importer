package net.orfeon.solr.importer.schema;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SchemaGeneratorTest {

    private static final Schema CHILD = SchemaBuilder.record("child").fields()
            .requiredString("name")
            .endRecord();

    private static final Schema SCHEMA = SchemaBuilder.record("doc").fields()
            .requiredString("id")
            .optionalString("title")
            .requiredBoolean("flag")
            .name("date").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
            .name("ts").type().optional().type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
            .name("tags").type().array().items().stringType().noDefault()
            .name("payload").type().optional().bytesType()
            .name("price").type(LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()
            .name("meta").type(CHILD).noDefault()
            .name("child").type().optional().type(CHILD)
            .name("children").type().array().items(CHILD).noDefault()
            .endRecord();

    @Test
    public void generatesFieldsMatchingConverterNaming() {
        final Document document = SchemaGenerator.toDocument(SCHEMA, "docs");
        final Map<String, Element> fields = elementsByName(document, "field");

        assertEquals(Set.of("id", "title", "flag", "date", "ts", "tags", "payload", "price", "meta.name", "child.name", "children.name"), fields.keySet());
        assertEquals("string", fields.get("id").getAttribute("type"));
        assertEquals("true", fields.get("id").getAttribute("required"));
        assertEquals("textja", fields.get("title").getAttribute("type"));
        assertEquals("false", fields.get("title").getAttribute("required"));
        assertEquals("boolean", fields.get("flag").getAttribute("type"));
        assertEquals("date", fields.get("date").getAttribute("type"));
        assertEquals("date", fields.get("ts").getAttribute("type"));
        assertEquals("binary", fields.get("payload").getAttribute("type"));
        assertEquals("double", fields.get("price").getAttribute("type"));
        assertEquals("true", fields.get("tags").getAttribute("multiValued"));
        assertEquals("true", fields.get("children.name").getAttribute("multiValued"));
        assertEquals("", fields.get("child.name").getAttribute("multiValued"));

        // A required leaf is required only when every record above it is required and none is repeated.
        assertEquals("true", fields.get("meta.name").getAttribute("required"));
        assertEquals("false", fields.get("child.name").getAttribute("required"));
        assertEquals("false", fields.get("children.name").getAttribute("required"));
        assertEquals("false", fields.get("tags").getAttribute("required"));

        assertEquals("docs", document.getDocumentElement().getAttribute("name"));
        assertEquals("id", document.getElementsByTagName("uniqueKey").item(0).getTextContent());
    }

    @Test
    public void defaultTypesCoverEveryGeneratedFieldTypeAndHaveUniqueNames() {
        final Document document = SchemaGenerator.toDocument(SCHEMA, "docs");
        final NodeList typeNodes = document.getElementsByTagName("fieldType");
        final Set<String> typeNames = new HashSet<>();
        for (int i = 0; i < typeNodes.getLength(); i++) {
            final String name = ((Element) typeNodes.item(i)).getAttribute("name");
            assertTrue("duplicate fieldType " + name, typeNames.add(name));
        }
        for (final Element field : elementsByName(document, "field").values()) {
            assertTrue("missing fieldType for " + field.getAttribute("type"), typeNames.contains(field.getAttribute("type")));
        }
    }

    @Test
    public void rendersAsXml() {
        final String xml = SchemaGenerator.generate(SCHEMA, "docs");
        assertTrue(xml.startsWith("<?xml"));
        assertTrue(xml.contains("<schema name=\"docs\" version=\"1.6\">"));
        assertTrue(xml.contains("solr.JapaneseTokenizerFactory"));
    }

    private static Map<String, Element> elementsByName(final Document document, final String tag) {
        final NodeList nodes = document.getElementsByTagName(tag);
        final Map<String, Element> byName = new HashMap<>();
        for (int i = 0; i < nodes.getLength(); i++) {
            final Element element = (Element) nodes.item(i);
            byName.put(element.getAttribute("name"), element);
        }
        return byName;
    }

}
