package net.orfeon.solr.importer.schema;

import net.orfeon.solr.importer.convert.AvroSchemas;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Map;

/**
 * Generates a starting-point schema.xml for a core from an Avro schema.
 *
 * Field names follow the same convention as AvroToSolrDocumentConverter: nested records produce
 * "parent.child" fields, arrays produce multiValued fields. A top-level "id" field becomes the uniqueKey.
 * The output is meant to be edited (analyzers, stored/indexed flags, vector dimensions) before use.
 */
public final class SchemaGenerator {

    private static final String UNIQUE_KEY_FIELD = "id";

    private SchemaGenerator() {
    }

    public static String generate(final Schema avroSchema, final String coreName) {
        return XmlDocuments.toString(toDocument(avroSchema, coreName));
    }

    public static Document toDocument(final Schema avroSchema, final String coreName) {
        if (avroSchema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("top level Avro schema must be a record: " + avroSchema);
        }
        final Document document = XmlDocuments.create("schema");
        final Element root = document.getDocumentElement();
        root.setAttribute("name", coreName);
        root.setAttribute("version", "1.6");

        final Element fields = document.createElement("fields");
        addFields(document, fields, null, avroSchema, false);
        root.appendChild(fields);

        if (avroSchema.getField(UNIQUE_KEY_FIELD) != null) {
            final Element uniqueKey = document.createElement("uniqueKey");
            uniqueKey.setTextContent(UNIQUE_KEY_FIELD);
            root.appendChild(uniqueKey);
        }

        final Element types = document.createElement("types");
        addDefaultTypes(document, types);
        root.appendChild(types);

        return document;
    }

    private static void addFields(
            final Document document,
            final Element fields,
            final String parentName,
            final Schema recordSchema,
            final boolean repeated) {

        for (final Schema.Field field : recordSchema.getFields()) {
            final String name = parentName == null ? field.name() : parentName + "." + field.name();
            addField(document, fields, name, field.schema(), AvroSchemas.isNullable(field.schema()), repeated);
        }
    }

    private static void addField(
            final Document document,
            final Element fields,
            final String name,
            final Schema fieldSchema,
            final boolean nullable,
            final boolean repeated) {

        final Schema schema = AvroSchemas.unnestUnion(fieldSchema);
        switch (schema.getType()) {
            case RECORD -> addFields(document, fields, name, schema, repeated);
            case ARRAY -> addField(document, fields, name, schema.getElementType(), true, true);
            case MAP, NULL, UNION -> {
                // no Solr representation
            }
            default -> {
                final Element element = document.createElement("field");
                element.setAttribute("name", name);
                // The uniqueKey must not be tokenized, so it gets the plain string type regardless of its Avro type.
                element.setAttribute("type", UNIQUE_KEY_FIELD.equals(name) ? "string" : fieldType(schema));
                element.setAttribute("indexed", "true");
                element.setAttribute("stored", "true");
                element.setAttribute("required", nullable ? "false" : "true");
                if (repeated) {
                    element.setAttribute("multiValued", "true");
                }
                fields.appendChild(element);
            }
        }
    }

    private static String fieldType(final Schema schema) {
        return switch (schema.getType()) {
            case BOOLEAN -> "boolean";
            case STRING -> "textja";
            case ENUM -> "string";
            case BYTES, FIXED -> "binary";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case INT -> {
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    yield "date";
                }
                if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    yield "string";
                }
                yield "int";
            }
            case LONG -> {
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    yield "date";
                }
                if (LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    yield "string";
                }
                yield "long";
            }
            default -> throw new IllegalArgumentException("not a scalar schema: " + schema);
        };
    }

    private static void addDefaultTypes(final Document document, final Element types) {
        types.appendChild(fieldType(document, "string", "solr.StrField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "boolean", "solr.BoolField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "binary", "solr.BinaryField", Map.of()));
        types.appendChild(fieldType(document, "int", "solr.IntPointField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "long", "solr.LongPointField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "float", "solr.FloatPointField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "double", "solr.DoublePointField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "date", "solr.DatePointField", Map.of("sortMissingLast", "true")));
        types.appendChild(fieldType(document, "vector", "solr.DenseVectorField",
                Map.of("vectorDimension", "4", "similarityFunction", "cosine")));
        types.appendChild(fieldType(document, "random", "solr.RandomSortField", Map.of()));
        types.appendChild(fieldType(document, "ignored", "solr.StrField", Map.of("indexed", "false", "stored", "false")));

        // Japanese text: morphological analysis.
        final Element textJa = fieldType(document, "textja", "solr.TextField", Map.of("positionIncrementGap", "100"));
        final Element jaAnalyzer = document.createElement("analyzer");
        jaAnalyzer.appendChild(analyzerPart(document, "tokenizer", "solr.JapaneseTokenizerFactory", Map.of("mode", "search")));
        jaAnalyzer.appendChild(analyzerPart(document, "filter", "solr.JapaneseBaseFormFilterFactory", Map.of()));
        jaAnalyzer.appendChild(analyzerPart(document, "filter", "solr.CJKWidthFilterFactory", Map.of()));
        jaAnalyzer.appendChild(analyzerPart(document, "filter", "solr.LowerCaseFilterFactory", Map.of()));
        jaAnalyzer.appendChild(analyzerPart(document, "filter", "solr.JapaneseKatakanaStemFilterFactory", Map.of("minimumLength", "2")));
        textJa.appendChild(jaAnalyzer);
        types.appendChild(textJa);

        // Japanese text: bigram, for recall on terms the tokenizer does not know.
        final Element textBi = fieldType(document, "textbi", "solr.TextField", Map.of("positionIncrementGap", "100"));
        final Element biAnalyzer = document.createElement("analyzer");
        biAnalyzer.appendChild(analyzerPart(document, "tokenizer", "solr.NGramTokenizerFactory", Map.of("minGramSize", "2", "maxGramSize", "2")));
        biAnalyzer.appendChild(analyzerPart(document, "filter", "solr.LowerCaseFilterFactory", Map.of()));
        textBi.appendChild(biAnalyzer);
        types.appendChild(textBi);
    }

    private static Element fieldType(final Document document, final String name, final String className, final Map<String, String> attributes) {
        final Element element = document.createElement("fieldType");
        element.setAttribute("name", name);
        element.setAttribute("class", className);
        attributes.forEach(element::setAttribute);
        return element;
    }

    private static Element analyzerPart(final Document document, final String tag, final String className, final Map<String, String> attributes) {
        final Element element = document.createElement(tag);
        element.setAttribute("class", className);
        attributes.forEach(element::setAttribute);
        return element;
    }

}
