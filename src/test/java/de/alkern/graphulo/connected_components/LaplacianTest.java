package de.alkern.graphulo.connected_components;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.DebugUtil;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LaplacianTest {

    private static Graphulo graphulo;
    private static TableOperations operations;

    @BeforeClass
    public static void init() throws Exception {
        graphulo = TestUtils.init();
        operations = graphulo.getConnector().tableOperations();
    }

    @Test
    public void calculateLaplacian() throws Exception {
        Laplacian lap = new Laplacian(graphulo);
        lap.calculateLaplacian("test", "test_deg", "test_lap");
        assertEquals(13, graphulo.countEntries("test_lap"));
        DebugUtil.printTable("test_lap", graphulo.getConnector(), "test_lap");
    }

}