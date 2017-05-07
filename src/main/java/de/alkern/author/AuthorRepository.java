package de.alkern.author;

import de.alkern.infrastructure.AccumuloRepository;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * Repository to save author-author-relations in an adjacence-matrix
 */
public class AuthorRepository implements AccumuloRepository {

    final static String TABLE_NAME = "authors";

    private Connector conn;
    private TableOperations operations;

    public AuthorRepository() {
        super();
        conn = connect();
        operations = conn.tableOperations();
        System.out.println("Successfully connected");
    }

    @Override
    public void save(String row, String qualifier, String value) {
        try {
            Text rowId = new Text(row);
            Text colQual = new Text(qualifier);
            Value v = new Value(value.getBytes());

            Mutation mutation = new Mutation(rowId);
            mutation.put(new Text(""), colQual, v);

            if (!operations.exists(TABLE_NAME)) {
                operations.create(TABLE_NAME);
            }

            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(10000L);

            BatchWriter writer = conn.createBatchWriter(TABLE_NAME, config);
            writer.addMutation(mutation);
            writer.close();
        } catch (TableNotFoundException | TableExistsException | AccumuloException | AccumuloSecurityException e) {
            System.err.println("Could not save");
            throw new RuntimeException(e);
        }
    }

    private Connector connect() {
        try {
            String instanceName = "bdp";
            String zooServer = "192.168.2.123:2181";
            Instance inst = new ZooKeeperInstance(instanceName, zooServer);
            System.out.println("Starting connecting");
            return inst.getConnector("root", new PasswordToken("acc"));
        } catch (AccumuloSecurityException | AccumuloException e) {
            e.printStackTrace();
            System.err.println("Cannot connect to Accumulo");
            throw new RuntimeException(e);
        }
    }

    @Override
    public List scan() {
        return null;
    }

    @Override
    public void clear() {
        try {
            if (operations.exists(TABLE_NAME)) {
                operations.delete(TABLE_NAME);
            }
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            e.printStackTrace();
            System.err.println("Could not delete table " + TABLE_NAME);
        }
    }
}
