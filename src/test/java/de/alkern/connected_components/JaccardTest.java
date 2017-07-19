package de.alkern.connected_components;

import edu.mit.ll.graphulo.util.DebugUtil;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Jaccard creates a table which contains the percentage of equal nodes in the neighbourhoods of two nodes
 */
public class JaccardTest {

    private final static String TABLE = "jaccard";
    private static final String RESULT_TABLE = "jaccard_res";

    @BeforeClass
    public static void init() {
        TestUtils.createUndirectedExampleMatrix(TABLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(TABLE);
        TestUtils.deleteTable(RESULT_TABLE);
    }

    @Test
    public void testJaccard() {
        TestUtils.graphulo.Jaccard_Client(TABLE, RESULT_TABLE, "", Authorizations.EMPTY, null);
        DebugUtil.printTable("Jaccard", TestUtils.conn, RESULT_TABLE);
        BatchScanner bs;
        try {
            bs = TestUtils.conn.createBatchScanner(RESULT_TABLE, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not create scanner for table " + RESULT_TABLE);
        }
        bs.setRanges(Collections.singleton(new Range()));
        Iterator<Map.Entry<Key, Value>> it = bs.iterator();

        testNextEquals(it, 1.0);
        testNextEquals(it, 0.5);
        testNextEquals(it, 0.333);
        testNextEquals(it, 0.5);
        assertFalse(it.hasNext());
    }

    private void testNextEquals(Iterator<Map.Entry<Key, Value>> it, double expected) {
        assertEquals(expected, (double) Double.valueOf(it.next().getValue().toString()), 0.005d);
    }
}
