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
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collections;
import java.util.Map;

public class TestUtils {

    private static Graphulo graphulo;

    public static Graphulo init() throws Exception {
        //load test data into acccumulo
        Connector conn = AccumuloConnector.local();
        graphulo = GraphuloConnector.local(conn);
        fillDatabase(conn, graphulo);
        return graphulo;
    }

    private static void fillDatabase(Connector conn, Graphulo graphulo) throws Exception {
        TableOperations operations = conn.tableOperations();
        if (!operations.exists("test")) {
            Repository repo = new RepositoryImpl("test", conn, new AdjacencyEntry.AdjacencyBuilder());
            GraphuloProcessor processor = new AuthorProcessor(repo);
            processor.parse(ExampleData.TWO_COMPONENTS_EXAMPLE);
        }
        if (!operations.exists("l")) {
            Repository repo = new RepositoryImpl("l", conn, new AdjacencyEntry.AdjacencyBuilder());
            GraphuloProcessor processor = new AuthorProcessor(repo);
            processor.parse(ExampleData.CC_EXAMPLE);
        }
        if (!operations.exists("test_deg")) {
            graphulo.generateDegreeTable("test", "test_deg", false);
        }
        if (!operations.exists("l_deg")) {
            graphulo.generateDegreeTable("l", "l_deg", false);
        }
    }

    public static long countEntries(String table) throws TableNotFoundException {
        BatchScanner scanner = graphulo.getConnector().createBatchScanner(table, Authorizations.EMPTY, 5);
        scanner.setRanges(Collections.singleton(new Range()));
        scanner.addScanIterator(new IteratorSetting(1, CountAllIterator.class));
        long cnt = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            cnt += LongCombiner.STRING_ENCODER.decode(entry.getValue().get());
        }
        scanner.close();
        return cnt;
    }
}
