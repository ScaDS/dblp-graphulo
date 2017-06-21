package de.alkern.graphulo.connected_components;

import de.alkern.author.AuthorProcessor;
import de.alkern.graphulo.GraphuloConnector;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.GraphuloProcessor;
import de.alkern.infrastructure.connector.AccumuloConnector;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.repository.RepositoryImpl;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.CountAllIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TestUtils {
    public static Connector conn;
    public static Graphulo graphulo;
    public static TableOperations tops;

    static {
        try {
            conn = AccumuloConnector.local();
            graphulo = GraphuloConnector.local(conn);
            tops = conn.tableOperations();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void fillDatabase() throws Exception {
        if (!tops.exists("test")) {
            Repository repo = new RepositoryImpl("test", conn, new AdjacencyEntry.AdjacencyBuilder());
            GraphuloProcessor processor = new AuthorProcessor(repo);
            processor.parse(ExampleData.TWO_COMPONENTS_EXAMPLE);
        }
        if (!tops.exists("l")) {
            Repository repo = new RepositoryImpl("l", conn, new AdjacencyEntry.AdjacencyBuilder());
            GraphuloProcessor processor = new AuthorProcessor(repo);
            processor.parse(ExampleData.CC_EXAMPLE);
        }
        if (!tops.exists("test_deg")) {
            graphulo.generateDegreeTable("test", "test_deg", false);
        }
        if (!tops.exists("l_deg")) {
            graphulo.generateDegreeTable("l", "l_deg", false);
        }
    }

    public static void createExampleMatrix(String table) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (tops.exists(table)) {
            return;
        }
        tops.create(table);

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000L);
        BatchWriter bw = conn.createBatchWriter(table, config);

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
    }

    public static void createWeakExampleMatrix(String table) throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        if (tops.exists(table)) {
            return;
        }
        tops.create(table);

        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(10000L);
        BatchWriter bw = conn.createBatchWriter(table, config);

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
        entries.add("ROW9:ROW10");

        for (String entry : entries) {
            byte[] row = entry.split(":")[0].getBytes();
            byte[] column = entry.split(":")[1].getBytes();
            Mutation m = new Mutation(row);
            m.put("".getBytes(), column, "1".getBytes());
            bw.addMutation(m);
        }
        bw.flush();
        bw.close();
    }

}
