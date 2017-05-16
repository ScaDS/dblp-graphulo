package de.alkern;

import de.alkern.author.AuthorProcessor;
import de.alkern.graphulo.examples.MultiplyWithValue;
import de.alkern.infrastructure.repository.AdjacencyRepository;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.connector.LocalConnector;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.io.IOException;

public class Main {

    public static void main(String[] args)
            throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException,
            IOException {
        Repository repo = new AdjacencyRepository("authors", new LocalConnector().get());
        AuthorProcessor processor = new AuthorProcessor(repo);
        processor.parse(ExampleData.EXAMPLE_DATA);
        processor.scan().forEach(System.out::println);
        MultiplyWithValue mwv = new MultiplyWithValue();
        processor.getIterator().forEachRemaining(it -> {
                    try {
                        mwv.apply(it.getKey(), it.getValue()).forEachRemaining(System.out::println);
                    } catch (IOException e) {}
                });
        processor.clear();
    }
}
