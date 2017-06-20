package de.alkern.graphulo.connected_components.strong;

import de.alkern.graphulo.GraphuloConnector;
import de.alkern.infrastructure.connector.AccumuloConnector;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.DebugUtil;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class StronglyConnectedComponentsTest {

    private static final String STRONG_EXAMPLE = "strong_example";
    private static Connector conn;
    private static TableOperations tops;

    @BeforeClass
    public static void init() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        conn = AccumuloConnector.local();
        tops = conn.tableOperations();
        if (tops.exists(STRONG_EXAMPLE)) {
            return;
        }
        tops.create(STRONG_EXAMPLE);

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000L);
        BatchWriter bw = conn.createBatchWriter(STRONG_EXAMPLE, config);

        List<String> entries = new LinkedList<>();
        entries.add("ROW1:ROW2");
        entries.add("ROW2:ROW3");
        entries.add("ROW2:ROW5");
        entries.add("ROW2:ROW6");
        entries.add("ROW3:ROW4");
        entries.add("ROW3:ROW7");
        entries.add("ROW4:ROW3");
        entries.add("ROW4:ROW8");
        entries.add("ROW5:ROW1");
        entries.add("ROW5:ROW6");
        entries.add("ROW6:ROW7");
        entries.add("ROW7:ROW6");
        entries.add("ROW7:ROW8");
        entries.add("ROW8:ROW8");

        for (String entry : entries) {
            byte[] row = entry.split(":")[0].getBytes();
            byte[] column = entry.split(":")[1].getBytes();
            Mutation m = new Mutation(row);
            m.put("".getBytes(), column, "1".getBytes());
            bw.addMutation(m);
        }
        bw.flush();
        bw.close();
        DebugUtil.printTable(STRONG_EXAMPLE, conn, STRONG_EXAMPLE, 5);
    }

    @Test
    public void test() throws TableNotFoundException {
        deleteIntermediateTables();
        StronglyConnectedComponents scc = new StronglyConnectedComponents(GraphuloConnector.local(conn));
        scc.calculateStronglyConnectedComponents(STRONG_EXAMPLE, "result");
    }

    private void deleteIntermediateTables() {
        int counter = 1;
        try {
//            tops.delete(STRONG_EXAMPLE);
            tops.delete(STRONG_EXAMPLE + "_deg");
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {

        }
        try {
            while (true) {
                tops.delete(STRONG_EXAMPLE + counter++);
            }
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {

        }
        counter = 1;
        try {
            while (true) {
                tops.delete(STRONG_EXAMPLE + counter++ + "_deg");
            }
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {

        }
    }
}