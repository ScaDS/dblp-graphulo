package de.alkern.graphulo.connected_components;

import de.alkern.author.AuthorProcessor;
import de.alkern.graphulo.GraphuloConnector;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.GraphuloProcessor;
import de.alkern.infrastructure.connector.AccumuloConnector;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.repository.RepositoryImpl;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.junit.Test;

import static org.junit.Assert.*;

public class ConnectedComponentsTest {

    @Test
    public void find() throws Exception {
        //load test data into acccumulo
        Connector conn = AccumuloConnector.local();
//        Repository repo = new RepositoryImpl("test", conn, new AdjacencyEntry.AdjacencyBuilder());
//        GraphuloProcessor processor = new AuthorProcessor(repo);
//        processor.parse(ExampleData.TWO_COMPONENTS_EXAMPLE);
        Graphulo graphulo = GraphuloConnector.local(conn);
//        graphulo.generateDegreeTable("test", "test_deg", false);

        //find connected components
        ConnectedComponents.find(graphulo, "test", "test_deg", "Artikel1 Autor1,");
//        TableOperations operations = conn.tableOperations();
//        assertTrue(operations.exists("test_cc1"));
//        assertTrue(operations.exists("test_cc2"));
//        assertFalse(operations.exists("test_cc3"));

        //clean up
//        operations.delete("test");
//        operations.delete("test_deg");
//        operations.delete("test_cc1");
//        operations.delete("test_cc2");
    }

}