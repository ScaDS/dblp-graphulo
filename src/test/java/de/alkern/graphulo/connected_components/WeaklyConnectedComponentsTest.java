package de.alkern.graphulo.connected_components;

import de.alkern.graphulo.connected_components.data.VisitedNodesList;
import de.alkern.graphulo.connected_components.weak.WeaklyConnectedComponents;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WeaklyConnectedComponentsTest {

    private static final String TABLE = "WEAK";
    private static final String ERROR_CASE = "ERROR";

    private static WeaklyConnectedComponents wcc;

    @BeforeClass
    public static void init() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        try {
            TestUtils.tops.delete("WEAK_cc1");
            TestUtils.tops.delete("WEAK_cc2");
            TestUtils.tops.delete("ERROR_cc1");
            TestUtils.tops.delete("ERROR_cc2");
        } catch (TableNotFoundException e) {}
        TestUtils.createWeakExampleMatrix(TABLE);
        TestUtils.createDirectedErrorCase(ERROR_CASE);
        wcc = new WeaklyConnectedComponents(TestUtils.graphulo, new VisitedNodesList());
    }

    @Test
    public void calculateConnectedComponents() throws Exception {
        wcc.calculateConnectedComponents(TABLE);
        assertEquals(14, TestUtils.graphulo.countEntries(TABLE + "_cc1"));
        assertEquals(1, TestUtils.graphulo.countEntries(TABLE + "_cc2"));
        assertEquals(TestUtils.graphulo.countEntries(TABLE),
                TestUtils.graphulo.countEntries(TABLE + "_cc1") + TestUtils.graphulo.countEntries(TABLE + "_cc2"));
    }

    @Test
    public void testErrorCaseWithDirectedGraph() throws Exception {
        wcc.calculateConnectedComponents(ERROR_CASE);
        assertTrue(TestUtils.tops.exists(ERROR_CASE + "_cc1"));
        assertFalse(TestUtils.tops.exists(ERROR_CASE + "_cc2"));
    }

}