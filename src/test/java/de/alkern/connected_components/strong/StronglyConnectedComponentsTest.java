package de.alkern.connected_components.strong;

import de.alkern.connected_components.TestUtils;
import de.alkern.connected_components.data.VisitedNodesList;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StronglyConnectedComponentsTest {

    private static final String STRONG_EXAMPLE = "strong_example";

    @BeforeClass
    public static void init() {
        TestUtils.createExampleMatrix(STRONG_EXAMPLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(STRONG_EXAMPLE);
        TestUtils.deleteTable(STRONG_EXAMPLE + "_scc1");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_scc2");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_scc3");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_scc4");
    }

    @Test
    public void testStronglyConnectedComponents() throws TableNotFoundException {
        StronglyConnectedComponents scc = new StronglyConnectedComponents(TestUtils.graphulo, new VisitedNodesList());
        scc.calculateConnectedComponents(STRONG_EXAMPLE);
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_scc1"));
        assertEquals(3, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_scc1"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_scc2"));
        assertEquals(2, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_scc2"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_scc3"));
        assertEquals(2, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_scc3"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_scc4"));
        assertEquals(1, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_scc4"));
        assertFalse(TestUtils.tops.exists(STRONG_EXAMPLE + "_scc5"));
    }
}