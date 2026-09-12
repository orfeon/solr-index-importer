package net.orfeon.solr.importer.schema;

import org.w3c.dom.Document;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;

final class XmlDocuments {

    private XmlDocuments() {
    }

    static Document create(final String rootElement) {
        try {
            return DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .getDOMImplementation()
                    .createDocument(null, rootElement, null);
        } catch (final ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

    static String toString(final Document document) {
        final StringWriter writer = new StringWriter();
        try {
            final Transformer transformer = TransformerFactory.newInstance().newTransformer();
            transformer.setOutputProperty(OutputKeys.INDENT, "yes");
            transformer.setOutputProperty(OutputKeys.METHOD, "xml");
            transformer.setOutputProperty("{http://xml.apache.org/xalan}indent-amount", "2");
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            return writer.toString();
        } catch (final TransformerException e) {
            throw new IllegalStateException(e);
        }
    }

}
