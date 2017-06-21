package de.alkern.graphulo.connected_components;

import de.alkern.graphulo.connected_components.weak.WeaklyConnectedComponents;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class WeaklyConnectedComponentsTest {

    private static final String TABLE = "WEAK";

    private static WeaklyConnectedComponents wcc;

    @BeforeClass
    public static void init() throws AccumuloException, AccumuloSecurityException, TableNotFoundException, TableExistsException {
        TestUtils.createWeakExampleMatrix(TABLE);
        wcc = new WeaklyConnectedComponents(TestUtils.graphulo);
    }

    @Test
    public void calculateConnectedComponents() throws Exception {
        wcc.calculateConnectedComponents(TABLE);
        assertEquals(14, TestUtils.graphulo.countEntries(TABLE + "_cc1"));
        assertEquals(1, TestUtils.graphulo.countEntries(TABLE + "_cc2"));
        assertEquals(TestUtils.graphulo.countEntries(TABLE),
                TestUtils.graphulo.countEntries(TABLE + "_cc1") + TestUtils.graphulo.countEntries(TABLE + "_cc2"));
    }

}