package de.alkern.infrastructure;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * Repository to save entries in an adjacence-matrix
 */
public class AdjacencyRepository implements Repository {

    private final String tableName;
    private final TableOperations operations;
    private final BatchWriter writer;

    public AdjacencyRepository(String tableName, Connector connector)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super();
        this.tableName = tableName;
        operations = connector.tableOperations();
        createTable();
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000L);
        writer = connector.createBatchWriter(tableName, config);
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
            writer.close();
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
}
