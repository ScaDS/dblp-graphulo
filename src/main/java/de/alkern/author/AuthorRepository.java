package de.alkern.author;

import de.alkern.infrastructure.AccumuloRepository;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.List;

/**
 * Repository to save author-author-relations in an adjacence-matrix
 */
public class AuthorRepository implements AccumuloRepository {

    private Connector conn;

    public AuthorRepository() {
        super();
        conn = connect();
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

            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(10000L);

            BatchWriter writer = conn.createBatchWriter("authors", config);
            writer.addMutation(mutation);
            writer.close();
        } catch (TableNotFoundException | MutationsRejectedException e) {
            throw new RuntimeException("Could not save");
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
            throw new RuntimeException("Cannot connect to Accumulo");
        }
    }

    @Override
    public List scan() {
        return null;
    }

    @Override
    public void clear() {

    }
}
