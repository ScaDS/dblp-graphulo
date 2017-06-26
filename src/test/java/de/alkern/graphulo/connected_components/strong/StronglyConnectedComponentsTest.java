package de.alkern.graphulo.connected_components.strong;

import de.alkern.graphulo.connected_components.TestUtils;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StronglyConnectedComponentsTest {

    private static final String STRONG_EXAMPLE = "strong_example";

    @BeforeClass
    public static void init() {
        TestUtils.createExampleMatrix(STRONG_EXAMPLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(STRONG_EXAMPLE);
        TestUtils.deleteTable(STRONG_EXAMPLE + "_deg");
    }

    @Test
    public void test() throws TableNotFoundException {
        StronglyConnectedComponents scc = new StronglyConnectedComponents(TestUtils.graphulo);
        scc.calculateStronglyConnectedComponents(STRONG_EXAMPLE, "result");
    }
}