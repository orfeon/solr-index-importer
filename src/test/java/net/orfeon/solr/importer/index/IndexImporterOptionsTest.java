package net.orfeon.solr.importer.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IndexImporterOptionsTest {

    @Test
    public void invalidRecordPolicyDefaultsToFail() {
        assertEquals(IndexImporter.InvalidRecordPolicy.FAIL, IndexImporter.InvalidRecordPolicy.parse(null));
        assertEquals(IndexImporter.InvalidRecordPolicy.FAIL, IndexImporter.InvalidRecordPolicy.parse(" "));
        assertEquals(IndexImporter.InvalidRecordPolicy.SKIP, IndexImporter.InvalidRecordPolicy.parse(" skip "));
        assertThrows(IllegalArgumentException.class, () -> IndexImporter.InvalidRecordPolicy.parse("ignore"));
    }

    @Test
    public void threadsDefaultToAvailableProcessors() {
        assertEquals(Runtime.getRuntime().availableProcessors(), IndexImporter.Options.parseThreads(null));
        assertEquals(Runtime.getRuntime().availableProcessors(), IndexImporter.Options.parseThreads(""));
        assertEquals(4, IndexImporter.Options.parseThreads(" 4 "));
        assertThrows(NumberFormatException.class, () -> IndexImporter.Options.parseThreads("many"));
        assertThrows(IllegalArgumentException.class,
                () -> new IndexImporter.Options(IndexImporter.InvalidRecordPolicy.FAIL, 0, true));
    }

    @Test
    public void dedupDefaultsToTrue() {
        assertTrue(IndexImporter.Options.parseDedup(null));
        assertTrue(IndexImporter.Options.parseDedup(""));
        assertTrue(IndexImporter.Options.parseDedup("true"));
        assertFalse(IndexImporter.Options.parseDedup(" false "));
        assertTrue(IndexImporter.Options.DEFAULT.dedup());
        assertEquals(IndexImporter.InvalidRecordPolicy.FAIL, IndexImporter.Options.DEFAULT.invalidRecordPolicy());
    }

}
