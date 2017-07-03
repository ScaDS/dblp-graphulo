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
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;

import java.util.LinkedList;
import java.util.List;

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

    public static void fillDatabase() {
        try {
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
        } catch (Exception e) {
            throw new RuntimeException("Could not init database", e);
        }
    }

    public static void clearDatabase() {
        deleteTable("test");
        deleteTable("l");
        deleteTable("test_deg");
        deleteTable("l_deg");
        deleteTable("test_lap");
    }

    /**
     * Delete table if it exists
     * @param table
     */
    public static void deleteTable(String table) {
        try {
            if (tops.exists(table)) {
                tops.delete(table);
            }
        } catch (Exception e) {
            throw new RuntimeException("SHOULD NOT HAPPEN: Could not delete unexisting table " + table, e);
        }
    }

    /**
     *    R1 R2 R3 R4 R5 R6 R7 R8
     * R1     1
     * R2        1     1  1
     * R3           1        1
     * R4        1              1
     * R5  1              1
     * R6                    1
     * R7                 1     1
     * R8                       1
     * @param table table name
     */
    public static void createExampleMatrix(String table) {
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
        writeTable(table, entries);
    }

    /**
     *    R1 R2 R3 R4 R5 R6 R7 R8 R10
     * R1     1
     * R2        1     1  1
     * R3           1        1
     * R4        1              1
     * R5  1              1
     * R6                    1
     * R7                 1     1
     * R8                       1
     * R9                           1
     * @param table table name
     */
    public static void createWeakExampleMatrix(String table) {
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
        writeTable(table, entries);
    }

    /**
     *   A B
     * B 1
     * C   1
     *
     * C -> B -> A
     *
     * @param table
     */
    public static void createDirectedErrorCase(String table) {
        List<String> entries = new LinkedList<>();
        entries.add("C:B");
        entries.add("B:A");
        writeTable(table, entries);
    }

    private static void writeTable(String table, List<String> entries) {
        try {
            if (tops.exists(table)) {
                return;
            }
            tops.create(table);

            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(10000L);
            BatchWriter bw = conn.createBatchWriter(table, config);

            for (String entry : entries) {
                byte[] row = entry.split(":")[0].getBytes();
                byte[] column = entry.split(":")[1].getBytes();
                Mutation m = new Mutation(row);
                m.put("".getBytes(), column, "1".getBytes());
                bw.addMutation(m);
            }
            bw.flush();
            bw.close();
        } catch (Exception e) {
            throw new RuntimeException("Could not create table " + table, e);
        }
    }
}
