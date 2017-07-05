package de.alkern.connected_components;

import de.alkern.connected_components.data.VisitedNodesList;
import de.alkern.connected_components.strong.StronglyConnectedComponents;
import de.alkern.connected_components.weak.WeaklyConnectedComponents;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests that cc-Algorithms work for adjacency matrices where every node has a connection to itself
 * the author processor creates a table in this format
 */
public class SelfConnectionTest {

    private final static String TABLE = "SELF";

    @BeforeClass
    public static void init() {
        TestUtils.createExampleMatrixWithSelfConnections(TABLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(TABLE);
        TestUtils.deleteTable(TABLE + "_wcc1");
        TestUtils.deleteTable(TABLE + "_scc1");
        TestUtils.deleteTable(TABLE + "_scc2");
        TestUtils.deleteTable(TABLE + "_scc3");
        TestUtils.deleteTable(TABLE + "_scc4");
    }

    @Test
    public void test() {
        new WeaklyConnectedComponents(TestUtils.graphulo, new VisitedNodesList()).calculateConnectedComponents(TABLE);
        new StronglyConnectedComponents(TestUtils.graphulo).calculateConnectedComponents(TABLE);

        assertTrue(TestUtils.tops.exists(TABLE + "_wcc1"));
        assertFalse(TestUtils.tops.exists(TABLE + "_wcc2"));
        assertTrue(TestUtils.tops.exists(TABLE + "_scc1"));
        assertTrue(TestUtils.tops.exists(TABLE + "_scc2"));
        assertTrue(TestUtils.tops.exists(TABLE + "_scc3"));
        assertTrue(TestUtils.tops.exists(TABLE + "_scc4"));
        assertFalse(TestUtils.tops.exists(TABLE + "_scc5"));
    }
}
