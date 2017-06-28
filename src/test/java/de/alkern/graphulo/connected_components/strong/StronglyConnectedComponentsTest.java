package de.alkern.graphulo.connected_components.strong;

import de.alkern.graphulo.connected_components.TestUtils;
import edu.mit.ll.graphulo.util.DebugUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

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
        TestUtils.deleteTable(STRONG_EXAMPLE + "_c");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_ct");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc1");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc2");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc3");
        TestUtils.deleteTable(STRONG_EXAMPLE + "_cc4");
    }

    @Test
    public void testStronglyConnectedComponents() throws TableNotFoundException {
        StronglyConnectedComponents scc = new StronglyConnectedComponents(TestUtils.graphulo);
        scc.calculateStronglyConnectedComponents(STRONG_EXAMPLE);
        DebugUtil.printTable("C-Table", TestUtils.conn, STRONG_EXAMPLE + "_c");
        DebugUtil.printTable("CT-Table", TestUtils.conn, STRONG_EXAMPLE + "_ct");
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc1"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc2"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc3"));
        assertTrue(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc4"));
        assertFalse(TestUtils.tops.exists(STRONG_EXAMPLE + "_cc5"));
    }
}