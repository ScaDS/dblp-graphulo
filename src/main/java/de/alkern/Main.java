package de.alkern;

import de.alkern.infrastructure.connector.GraphuloConnector;
import de.alkern.infrastructure.connector.AccumuloConnector;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.*;

public class Main {

    public static void main(String[] args)
            throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Connector conn = AccumuloConnector.local();
//        Repository repo = new RepositoryImpl("authors", conn, new AdjacencyEntry.AdjacencyBuilder());
//        GraphuloProcessor processor = new AuthorProcessor(repo);
//        processor.parse(ExampleData.DBLP);
//        processor.scan().forEach(System.out::println);
        Graphulo graphulo = GraphuloConnector.local(conn);
//        graphulo.generateDegreeTable("authors", "authors_deg", true);
        //processor.clear();
//        new HistogramBuilder(graphulo).getChartsAsPNG("authors", ComponentType.WEAK, "test");
    }
}
