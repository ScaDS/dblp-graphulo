package de.alkern.connected_components.weak;

import de.alkern.connected_components.TestUtils;
import de.alkern.connected_components.data.VisitedNodesList;
import org.junit.AfterClass;
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
    public static void init() {
        TestUtils.createWeakExampleMatrix(TABLE);
        TestUtils.createDirectedErrorCase(ERROR_CASE);
        wcc = new WeaklyConnectedComponents(TestUtils.graphulo, new VisitedNodesList());
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(TABLE);
        TestUtils.deleteTable(ERROR_CASE);
        TestUtils.deleteTable("WEAK_wcc1");
        TestUtils.deleteTable("WEAK_wcc2");
        TestUtils.deleteTable("ERROR_wcc1");
        TestUtils.deleteTable("ERROR_wcc2");
    }

    @Test
    public void calculateConnectedComponents() throws Exception {
        wcc.calculateConnectedComponents(TABLE);
        assertEquals(14, TestUtils.graphulo.countEntries(TABLE + "_wcc1"));
        assertEquals(1, TestUtils.graphulo.countEntries(TABLE + "_wcc2"));
        assertEquals(TestUtils.graphulo.countEntries(TABLE),
                TestUtils.graphulo.countEntries(TABLE + "_wcc1") + TestUtils.graphulo.countEntries(TABLE + "_wcc2"));
    }

    @Test
    public void testErrorCaseWithDirectedGraph() throws Exception {
        wcc.calculateConnectedComponents(ERROR_CASE);
        assertTrue(TestUtils.tops.exists(ERROR_CASE + "_wcc1"));
        assertFalse(TestUtils.tops.exists(ERROR_CASE + "_wcc2"));
    }

}