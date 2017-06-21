package de.alkern.graphulo.connected_components;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class ConnectedComponentsTest {

    @Test
    public void shortExample() throws Exception {
        //prepare if previous run failed
        if (TestUtils.tops.exists("test_cc1")) TestUtils.tops.delete("test_cc1");
        if (TestUtils.tops.exists("test_cc2")) TestUtils.tops.delete("test_cc2");

        //splitConnectedComponents connected components
        new ConnectedComponents(TestUtils.graphulo).splitConnectedComponents( "test", "test_deg");
        assertTrue(TestUtils.tops.exists("test_cc1"));
        assertTrue(TestUtils.tops.exists("test_cc2"));
        assertFalse(TestUtils.tops.exists("test_cc3"));

        assertEquals(6, TestUtils.graphulo.countEntries("test_cc1"));
        assertEquals(2, TestUtils.graphulo.countEntries("test_cc2"));
        assertEquals(TestUtils.graphulo.countEntries("test"), TestUtils.graphulo.countEntries("test_cc1") + TestUtils.graphulo.countEntries("test_cc2"));

        //clean up
//        TestUtils.tops.delete("test");
//        TestUtils.tops.delete("test_deg");
        TestUtils.tops.delete("test_cc1");
        TestUtils.tops.delete("test_cc2");
    }

    @Test
    public void longerExample() throws Exception {
        //prepare if previous run failed
        if (TestUtils.tops.exists("l_cc1")) TestUtils.tops.delete("l_cc1");
        if (TestUtils.tops.exists("l_cc2")) TestUtils.tops.delete("l_cc2");

        //splitConnectedComponents connected components
        new ConnectedComponents(TestUtils.graphulo).splitConnectedComponents( "l", "l_deg");
        assertTrue(TestUtils.tops.exists("l_cc1"));
        assertTrue(TestUtils.tops.exists("l_cc2"));
        assertFalse(TestUtils.tops.exists("l_cc3"));

        assertEquals(12, TestUtils.graphulo.countEntries("l_cc1"));
        assertEquals(6, TestUtils.graphulo.countEntries("l_cc2"));
        assertEquals(TestUtils.graphulo.countEntries("l"),
                TestUtils.graphulo.countEntries("l_cc1") + TestUtils.graphulo.countEntries("l_cc2"));

        //clean up
//        TestUtils.tops.delete("test");
//        TestUtils.tops.delete("test_deg");
        TestUtils.tops.delete("l_cc1");
        TestUtils.tops.delete("l_cc2");
    }

    @Test
    public void testGetNeighbours() throws AccumuloSecurityException, AccumuloException {
        Collection<String> neighbours = new ConnectedComponents(TestUtils.graphulo).getNeighbours( "test",
                "test_deg", "Artikel1 Autor1,");
        assertEquals(2, neighbours.size());
    }

}