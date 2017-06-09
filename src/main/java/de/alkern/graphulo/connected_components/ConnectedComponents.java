package de.alkern.graphulo.connected_components;

import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.*;

public class ConnectedComponents {

    private static final String COMPONENT_SUFFIX = "_cc";
    private Graphulo graphulo;
    private List<String> visited;
    private Queue<String> toVisit;
    private String table;
    private String degTable;
    private Long ccNumber;

    public ConnectedComponents(Graphulo graphulo) {
        this.graphulo = graphulo;
        visited = new ArrayList<>();
        toVisit = new PriorityQueue<>();
    }

    /**
     * Finds all connected components and saves them in new tables
     *
     * @param table    for which to find components
     */
    public void splitConnectedComponents(String table, String degTable, String v0) {
        visited.clear();
        toVisit.clear();
        this.table = table;
        this.degTable = degTable;
        this.ccNumber = 1L;
        Iterator<Map.Entry<Key, Value>> it = scanTable(graphulo.getConnector());
        it.forEachRemaining(this::visitEntry);
    }

    private void visitEntry(Map.Entry<Key, Value> entry) {
        String node = entry.getKey().getRow().toString();
        if (visited.contains(node)) return;
        System.out.println("Found Connected component number " + ccNumber++);
        while (node != null && (!visited.contains(node) || !toVisit.isEmpty())) {
            visited.add(node);
            toVisit.addAll(this.getUnvisitedNeighbours(node));
            this.copyAllEntriesForNode(node);
            node = toVisit.poll();
        }
    }

    /**
     * Copy all entries from the table which belong to this node to the current cc-table
     * @param node
     */
    private void copyAllEntriesForNode(String node) {

    }

    private Collection<String> getUnvisitedNeighbours(String v0) {
        Collection<String> neighbours = this.getNeighbours(v0);
        neighbours.removeAll(visited);
        return neighbours;
    }

    /**
     * Scan the table for all entries
     * @param conn
     * @return
     */
    private Iterator<Map.Entry<Key, Value>> scanTable(Connector conn) {
        BatchScanner bs;
        try {
            bs = conn.createBatchScanner(table, new Authorizations(), 50);
            List<Range> ranges = new ArrayList<Range>();
            ranges.add(new Range());
            bs.setRanges(ranges);
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        return bs.iterator();
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
