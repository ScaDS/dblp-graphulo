package de.alkern.connected_components;

import de.alkern.connected_components.data.VisitQueue;
import de.alkern.connected_components.data.VisitQueueImpl;
import de.alkern.connected_components.data.VisitedNodes;
import de.alkern.connected_components.data.VisitedNodesList;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.*;

/**
 * First implementation of cc
 */
@Deprecated
public class ConnectedComponents {

    private static final String COMPONENT_SUFFIX = "_cc";
    private Graphulo graphulo;
    private VisitedNodes visited;
    private VisitQueue toVisit;
    private String table;
    private String degTable;
    private Long ccNumber;

    public ConnectedComponents(Graphulo graphulo) {
        this.graphulo = graphulo;
//        visited = new VisitedNodesTable(graphulo.getConnector());
        visited = new VisitedNodesList();
        toVisit = new VisitQueueImpl();
    }

    /**
     * Finds all connected components and saves them in new tables
     * Tables for the components have the _cc suffix with a number
     *
     * @param table for which to find components
     * @param degTable degree table for the table
     */
    public void splitConnectedComponents(String table, String degTable) {
        // prepare the class to run the algorithm
        visited.clear();
        toVisit.clear();
        this.table = table;
        this.degTable = degTable;
        this.ccNumber = 0L;

        // Iterate over the whole table once, to ensure every node is visited
        BatchScanner bs;
        try {
            bs = graphulo.getConnector().createBatchScanner(table, Authorizations.EMPTY, 25);
            bs.setRanges(Collections.singleton(new Range()));
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        for (Map.Entry<Key, Value> entry : bs) {
            visitEntry(entry);
        }
        bs.close();
    }

    private void visitEntry(Map.Entry<Key, Value> entry) {
        String node = entry.getKey().getRow().toString();
        if (visited.hasVisited(node)) return;
        System.out.println("Found connected component number " + ++ccNumber);
        while (node != null) {
            //@todo vielleicht beide VisitedNodes klassen kombiniert einsetzen, um nicht jeden Knoten einzeln in accumulo zu schreiben?
            if (!visited.hasVisited(node)) {
                visited.visitNode(node);
                toVisit.addAll(this.getUnvisitedNeighbours(node));
                this.copyAllEntriesForNode(node);
            }
            node = toVisit.poll();
        }
    }

    /**
     * Copy all entries from the table which belong to this node to the current cc-table
     * @param node
     */
    private void copyAllEntriesForNode(String node) {
        BatchScanner bs;
        try {
            bs = graphulo.getConnector().createBatchScanner(table, Authorizations.EMPTY, 50);
            bs.setRanges(Collections.singleton(new Range(node))); //just check entries for the given node
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }

        //create table for the current cc
        String currentComponentTable = table + COMPONENT_SUFFIX + ccNumber;
        TableOperations operations = graphulo.getConnector().tableOperations();
        try {
            if (!operations.exists(currentComponentTable)) {
                operations.create(currentComponentTable);
            }
        } catch (AccumuloException | AccumuloSecurityException | TableExistsException e) {
            throw new RuntimeException(e);
        }

        //add a RemoteWriteIterator to the scanner to copy all entries
        DynamicIteratorSetting dis = new DynamicIteratorSetting(15, "copyAllEntriesForNode");
        dis.append(new IteratorSetting(1, RemoteWriteIterator.class,
                graphulo.basicRemoteOpts("", currentComponentTable, null, null)));
        dis.addToScanner(bs);

        AdjacencyEntry.AdjacencyBuilder builder = new AdjacencyEntry.AdjacencyBuilder();
        try {
            for (Map.Entry<Key, Value> entry : bs) {
                System.out.println(builder.fromMapEntry(entry) + " added to " + currentComponentTable);
            }
        } finally {
            bs.close();
        }
    }

    private Collection<String> getUnvisitedNeighbours(String v0) {
        Collection<String> neighbours = this.getNeighbours(v0);
        return visited.getUnvisitedNodes(neighbours);
    }

    /**
     * Find all neighbours for a given Node
     * @param table table to test
     * @param degTable degree table for the table
     * @param v0 node for which to get the neighbours
     * @return list with all neighbours
     */
    public Collection<String> getNeighbours(String table, String degTable, String v0) {
        if (!v0.endsWith(",")) {
            v0 = v0 + ",";
        }
        String neighbours = graphulo.AdjBFS(table, v0, 1, null, null,
                degTable, null, false, 0, Integer.MAX_VALUE);
        Collection<String> neighbourList = new ArrayList<>();
        for (String neighbour : neighbours.split(",")) {
            neighbourList.add(neighbour);
        }
        return neighbourList;
    }

    private Collection<String> getNeighbours(String v0) {
        return this.getNeighbours(this.table, this.degTable, v0);
    }
}
