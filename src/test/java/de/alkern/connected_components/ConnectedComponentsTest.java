package de.alkern.connected_components;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

@Deprecated
public class ConnectedComponentsTest {

    @BeforeClass
    public static void init() {
        TestUtils.fillDatabase();
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.clearDatabase();
        TestUtils.deleteTable("test_cc1");
        TestUtils.deleteTable("test_cc2");
        TestUtils.deleteTable("l_cc1");
        TestUtils.deleteTable("l_cc2");
        TestUtils.deleteTable("l_cc3");
    }

    @Test
    public void shortExample() throws Exception {
        new ConnectedComponents(TestUtils.graphulo).splitConnectedComponents( "test", "test_deg");
        assertTrue(TestUtils.tops.exists("test_cc1"));
        assertTrue(TestUtils.tops.exists("test_cc2"));
        assertFalse(TestUtils.tops.exists("test_cc3"));

        assertEquals(9, TestUtils.graphulo.countEntries("test_cc1"));
        assertEquals(4, TestUtils.graphulo.countEntries("test_cc2"));
        assertEquals(TestUtils.graphulo.countEntries("test"), TestUtils.graphulo.countEntries("test_cc1") + TestUtils.graphulo.countEntries("test_cc2"));
    }

    @Test
    public void longerExample() throws Exception {
        //splitConnectedComponents connected components
        new ConnectedComponents(TestUtils.graphulo).splitConnectedComponents( "l", "l_deg");
        assertTrue(TestUtils.tops.exists("l_cc1"));
        assertTrue(TestUtils.tops.exists("l_cc2"));
        assertTrue(TestUtils.tops.exists("l_cc3"));
        assertFalse(TestUtils.tops.exists("l_cc4"));

        assertEquals(15, TestUtils.graphulo.countEntries("l_cc1"));
        assertEquals(9, TestUtils.graphulo.countEntries("l_cc2"));
        assertEquals(1, TestUtils.graphulo.countEntries("l_cc3"));
        assertEquals(TestUtils.graphulo.countEntries("l"),
                TestUtils.graphulo.countEntries("l_cc1") + TestUtils.graphulo.countEntries("l_cc2")
                        + TestUtils.graphulo.countEntries("l_cc3"));
    }

    @Test
    public void testGetNeighbours() throws AccumuloSecurityException, AccumuloException {
        Collection<String> neighbours = new ConnectedComponents(TestUtils.graphulo).getNeighbours( "test",
                "test_deg", "Artikel1 Autor1,");
        assertEquals(3, neighbours.size());
    }

}