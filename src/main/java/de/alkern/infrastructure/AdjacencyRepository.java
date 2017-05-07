package de.alkern.infrastructure;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
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
            Mutation mutation = getMutation(row, qualifier, value);
            writer.addMutation(mutation);
        } catch (AccumuloException e) {
            System.err.println("Could not save");
            throw new RuntimeException(e);
        }
    }

    private Mutation getMutation(String row, String qualifier, String value) {
        Text rowId = new Text(row);
        Text colQual = new Text(qualifier);
        Value v = new Value(value.getBytes());

        Mutation mutation = new Mutation(rowId);
        mutation.put(new Text(""), colQual, v);
        return mutation;
    }

    @Override
    public List scan() {
        for (Map.Entry<Key, Value> entry: scanner) {
            Text row = entry.getKey().getRow();
            Text colQual = entry.getKey().getColumnQualifier();
            Value value = entry.getValue();
            System.out.println(row + " :" + colQual + " []   -> " + value);
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
