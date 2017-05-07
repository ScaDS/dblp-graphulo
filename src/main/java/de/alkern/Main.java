package de.alkern;

import de.alkern.author.AuthorProcessor;
import de.alkern.infrastructure.connector.AccumuloConnector;
import de.alkern.infrastructure.AdjacencyRepository;
import de.alkern.infrastructure.Repository;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.connector.LocalConnector;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

public class Main {

    public static void main(String[] args)
            throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException {
        Repository repo = new AdjacencyRepository("authors", new LocalConnector().get());
        AuthorProcessor processor = new AuthorProcessor(repo, 10);
        processor.parse(ExampleData.EXAMPLE_DATA);
        processor.scan();
        //processor.clear();
    }
}
