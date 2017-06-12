package de.alkern.graphulo.connected_components;

import de.alkern.infrastructure.entry.AccumuloEntry;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Accumulo table implementation of {@see VisitedNodes}
 * Saves visited nodes in a temporary table
 */
public class VisitedNodesTable implements VisitedNodes {

    private static final String TABLE_NAME = "visited";

    private final Connector conn;
    private final TableOperations tops;
    private BatchWriter writer;

    public VisitedNodesTable(Connector conn) {
        this.conn = conn;
        this.tops = conn.tableOperations();

        try {
            if (tops.exists(TABLE_NAME)) {
                tops.delete(TABLE_NAME);
            }
            tops.create(TABLE_NAME);
            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(10000L);
            this.writer = conn.createBatchWriter(TABLE_NAME, config);
        } catch (TableExistsException | AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void visitNode(String node) {
        try {
            AccumuloEntry entry = new AdjacencyEntry(node, "", "1");
            writer.addMutation(entry.toMutation());
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasVisited(String node) {
        try (BatchScanner scanner = conn.createBatchScanner(TABLE_NAME, Authorizations.EMPTY, 15)) {
            scanner.setRanges(Collections.singleton(new Range(node)));
            return scanner.iterator().hasNext();
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void clear() {
        try {
            writer.close();
            tops.delete(TABLE_NAME);
            tops.create(TABLE_NAME);
            BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxMemory(10000L);
            this.writer = conn.createBatchWriter(TABLE_NAME, config);
        } catch (TableExistsException | AccumuloSecurityException | TableNotFoundException | AccumuloException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Collection<String> getUnvisitedNodes(Collection<String> neighbours) {
        try (BatchScanner scanner = conn.createBatchScanner(TABLE_NAME, Authorizations.EMPTY, 15)) {
            Collection<Range> ranges = neighbours.stream().map(Range::new).collect(Collectors.toList());
            scanner.setRanges(ranges);
            for (Map.Entry<Key, Value> entry : scanner) {
                String visitedNode = entry.getKey().toString();
                neighbours.remove(visitedNode);
            }
            return neighbours;
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
