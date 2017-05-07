package de.alkern.infrastructure.repository;

import de.alkern.infrastructure.AdjacencyEntry;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.List;
import java.util.Map;

/**
 * Repository to save entries in an adjacence-matrix
 */
public class AdjacencyRepository implements Repository {

    private final String tableName;
    private final TableOperations operations;
    private final BatchWriter writer;
    private final Scanner scanner;

    public AdjacencyRepository(String tableName, Connector connector)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super();
        this.tableName = tableName;
        operations = connector.tableOperations();
        createTable();
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000L);
        writer = connector.createBatchWriter(tableName, config);
        scanner = connector.createScanner(tableName, new Authorizations());
    }

    private void createTable() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        if (!operations.exists(tableName)) {
            operations.create(tableName);
        }
    }

    @Override
    public void save(String row, String qualifier, String value) {
        try {
            Mutation mutation = new AdjacencyEntry(row, qualifier, value).toMutation();
            writer.addMutation(mutation);
        } catch (AccumuloException e) {
            System.err.println("Could not save");
            throw new RuntimeException(e);
        }
    }

    @Override
    public List scan() {
        for (Map.Entry<Key, Value> entry: scanner) {
            System.out.println(AdjacencyEntry.fromEntry(entry));
        }
        return null;
    }

    @Override
    public void clear() {
        try {
            if (operations.exists(tableName)) {
                operations.delete(tableName);
            }
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            System.err.println("Could not delete table " + tableName);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (MutationsRejectedException e) {
            System.err.println("Could not close writer");
            throw new RuntimeException(e);
        }
    }
}
