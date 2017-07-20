package de.alkern.connected_components;

import de.alkern.infrastructure.ExampleData;
import edu.mit.ll.graphulo.util.DebugUtil;
import edu.mit.ll.graphulo.util.GraphuloUtil;
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

    private final static String UNDIRECTED_TABLE = "jaccard_undir";
    private final static String DIRECTED_TABLE = "jaccard_dir";
    private static final String UNDIRECTED_RESULT_TABLE = "jaccard_dir_res";
    private static final String DIRECTED_RESULT_TABLE = "jaccard_undir_res";
    private static final String CC_EXAMPLE = "jaccard_cc";
    private static final String CC_EXAMPLE_RESULT = "jaccard_cc_res";

    @BeforeClass
    public static void init() {
        TestUtils.createUndirectedExampleMatrix(UNDIRECTED_TABLE);
        TestUtils.createDirectedJaccardExampleMatrix(DIRECTED_TABLE);
        TestUtils.createFromResource(CC_EXAMPLE, ExampleData.CC_EXAMPLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(UNDIRECTED_TABLE);
        TestUtils.deleteTable(DIRECTED_TABLE);
        TestUtils.deleteTable(UNDIRECTED_RESULT_TABLE);
        TestUtils.deleteTable(DIRECTED_RESULT_TABLE);
        TestUtils.deleteTable(CC_EXAMPLE);
        TestUtils.deleteTable(CC_EXAMPLE_RESULT);
    }

    @Test
    public void testJaccard() {
        TestUtils.graphulo.Jaccard_Client(UNDIRECTED_TABLE, UNDIRECTED_RESULT_TABLE, "", Authorizations.EMPTY, null);
        BatchScanner bs;
        try {
            bs = TestUtils.conn.createBatchScanner(UNDIRECTED_RESULT_TABLE, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not create scanner for table " + UNDIRECTED_RESULT_TABLE);
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
        assertEquals(expected, Double.valueOf(it.next().getValue().toString()), 0.005d);
    }

    @Test
    public void testJaccardOfDirectedGraphIsEmpty() {
        TestUtils.graphulo.Jaccard_Client(DIRECTED_TABLE, DIRECTED_RESULT_TABLE, "", Authorizations.EMPTY, null);
        BatchScanner bs;
        try {
            bs = TestUtils.conn.createBatchScanner(DIRECTED_RESULT_TABLE, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not create scanner for table " + UNDIRECTED_RESULT_TABLE);
        }
        bs.setRanges(Collections.singleton(new Range()));
        assertFalse(bs.iterator().hasNext());
    }

    @Test
    public void testJaccardForAuthorRelationExampleWorks() {
        TestUtils.graphulo.Jaccard_Client(CC_EXAMPLE, CC_EXAMPLE_RESULT, "", Authorizations.EMPTY, null);
        DebugUtil.printTable("CC", TestUtils.conn, CC_EXAMPLE, 7);
        DebugUtil.printTable("CC", TestUtils.conn, CC_EXAMPLE_RESULT, 7);
        assertEquals(11, TestUtils.graphulo.countEntries(CC_EXAMPLE_RESULT));
    }

}
