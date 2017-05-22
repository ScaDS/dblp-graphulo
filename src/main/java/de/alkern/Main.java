package de.alkern;

import de.alkern.author.AuthorProcessor;
import de.alkern.graphulo.GraphuloConnector;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.GraphuloProcessor;
import de.alkern.infrastructure.connector.AccumuloConnector;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import de.alkern.infrastructure.repository.RepositoryImpl;
import de.alkern.infrastructure.repository.Repository;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.*;

public class Main {

    public static void main(String[] args)
            throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Connector conn = AccumuloConnector.local();
        Repository repo = new RepositoryImpl("authors", conn, new AdjacencyEntry.AdjacencyBuilder());
        GraphuloProcessor processor = new AuthorProcessor(repo);
        processor.parse(ExampleData.EXAMPLE_DATA);
        processor.scan().forEach(System.out::println);
        Graphulo graphulo = GraphuloConnector.local(conn);
        graphulo.generateDegreeTable("authors", "authors_deg", true);
        //processor.clear();
    }
}
