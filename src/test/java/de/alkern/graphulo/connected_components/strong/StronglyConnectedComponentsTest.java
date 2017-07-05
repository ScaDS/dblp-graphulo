package de.alkern.graphulo.connected_components.strong;

import de.alkern.graphulo.connected_components.TestUtils;
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
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc1");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc2");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc3");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc4");
    }

    @Test
    public void testStronglyConnectedComponents() throws TableNotFoundException {
        StronglyConnectedComponents scc = new StronglyConnectedComponents(TestUtils.graphulo);
        scc.calculateConnectedComponents(STRONG_EXAMPLE);
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc1"));
        assertEquals(3, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_cc1"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc2"));
        assertEquals(2, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_cc2"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc3"));
        assertEquals(2, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_cc3"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc4"));
        assertEquals(1, TestUtils.graphulo.countEntries(STRONG_EXAMPLE + "_cc4"));
        assertFalse(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc5"));
    }
}