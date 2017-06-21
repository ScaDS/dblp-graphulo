package de.alkern.graphulo.connected_components;

import edu.mit.ll.graphulo.util.DebugUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LaplacianTest {

    @Test
    public void calculateLaplacian() throws Exception {
        Laplacian lap = new Laplacian(TestUtils.graphulo);
        lap.calculateLaplacian("test", "test_deg", "test_lap");
        assertEquals(13, TestUtils.graphulo.countEntries("test_lap"));
        DebugUtil.printTable("test_lap", TestUtils.conn, "test_lap");
    }

}