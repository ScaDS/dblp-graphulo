package de.alkern.graphulo.connected_components;

import de.alkern.author.AuthorProcessor;
import de.alkern.graphulo.GraphuloConnector;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.GraphuloProcessor;
import de.alkern.infrastructure.connector.AccumuloConnector;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.repository.RepositoryImpl;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.CountAllIterator;
import edu.mit.ll.graphulo.skvi.DynamicIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.iterators.system.CountingIterator;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class ConnectedComponentsTest {

    private static Graphulo graphulo;
    private static TableOperations operations;

    @BeforeClass
    public static void init() throws Exception {
        graphulo = TestUtils.init();
        operations = graphulo.getConnector().tableOperations();
    }

    @Test
    public void shortExample() throws Exception {
        //prepare if previous run failed
        if (operations.exists("test_cc1")) operations.delete("test_cc1");
        if (operations.exists("test_cc2")) operations.delete("test_cc2");

        //splitConnectedComponents connected components
        new ConnectedComponents(graphulo).splitConnectedComponents( "test", "test_deg");
        assertTrue(operations.exists("test_cc1"));
        assertTrue(operations.exists("test_cc2"));
        assertFalse(operations.exists("test_cc3"));

        assertEquals(6, TestUtils.countEntries("test_cc1"));
        assertEquals(2, TestUtils.countEntries("test_cc2"));
        assertEquals(TestUtils.countEntries("test"), TestUtils.countEntries("test_cc1") + TestUtils.countEntries("test_cc2"));

        //clean up
//        operations.delete("test");
//        operations.delete("test_deg");
        operations.delete("test_cc1");
        operations.delete("test_cc2");
    }

    @Test
    public void longerExample() throws Exception {
        //prepare if previous run failed
        if (operations.exists("l_cc1")) operations.delete("l_cc1");
        if (operations.exists("l_cc2")) operations.delete("l_cc2");

        //splitConnectedComponents connected components
        new ConnectedComponents(graphulo).splitConnectedComponents( "l", "l_deg");
        assertTrue(operations.exists("l_cc1"));
        assertTrue(operations.exists("l_cc2"));
        assertFalse(operations.exists("l_cc3"));

        assertEquals(10, TestUtils.countEntries("l_cc1"));
        assertEquals(6, TestUtils.countEntries("l_cc2"));
        assertEquals(TestUtils.countEntries("l"), TestUtils.countEntries("l_cc1") + TestUtils.countEntries("l_cc2"));

        //clean up
//        operations.delete("test");
//        operations.delete("test_deg");
        operations.delete("l_cc1");
        operations.delete("l_cc2");
    }

    @Test
    public void testGetNeighbours() throws AccumuloSecurityException, AccumuloException {
        Collection<String> neighbours = new ConnectedComponents(graphulo).getNeighbours( "test",
                "test_deg", "Artikel1 Autor1,");
        assertEquals(2, neighbours.size());
    }

}