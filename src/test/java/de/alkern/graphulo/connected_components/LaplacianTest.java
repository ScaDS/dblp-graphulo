package de.alkern.graphulo.connected_components;

import edu.mit.ll.graphulo.util.DebugUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LaplacianTest {

    @BeforeClass
    public static void init() {
        TestUtils.fillDatabase();
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.clearDatabase();
    }

    @Test
    public void calculateLaplacian() throws Exception {
        Laplacian lap = new Laplacian(TestUtils.graphulo);
        lap.calculateLaplacian("test", "test_deg", "test_lap");
        assertEquals(13, TestUtils.graphulo.countEntries("test_lap"));
    }

}