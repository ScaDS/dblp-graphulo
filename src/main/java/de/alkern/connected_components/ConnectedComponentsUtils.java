package de.alkern.connected_components;

import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ConnectedComponentsUtils {

    /**
     * Use Graphulos AdjBFS to find all reachable nodes
     * @param g Graphulo instance
     * @param table Name of the graph
     * @param node starting node
     * @return D4M String describing all reachable nodes
     */
    public static String getAllNeighbours(Graphulo g, String table, String node) {
        int counter = 1;
        //increment number of steps until all neighbours are found and the neighbourhood string does not change anymore
        String oldNeighbours;
        String newNeighbours = "";
        do {
            oldNeighbours = newNeighbours;
            newNeighbours = g.AdjBFS(table, node + ";", counter++, null, null, null,
                    4, null, null, false, 0,
                    Integer.MAX_VALUE, null, null, null, true,
                    null);
        } while (!oldNeighbours.equals(newNeighbours));
        return newNeighbours;
    }

    /**
     * Get the names of all existing component tables
     * @param g Graphulo instance
     * @param table name of the original table
     * @param type ComponentType of the calculated cc-Tables
     * @return List with all names of cc-Tables
     */
    public static List<String> getExistingComponentTables(Graphulo g, String table, ComponentType type) {
        List<String> result = new LinkedList<>();
        TableOperations tops = g.getConnector().tableOperations();
        long counter = 1;
        String t = table + type + counter++;
        while (tops.exists(t)) {
            result.add(t);
            t = table + type + counter++;
        }
        return result;
    }

    /**
     * Build the name of the table containing a certain connected component
     * @param table
     * @param type
     * @param componentNumber
     * @return
     */
    public static String getComponentTableName(String table, ComponentType type, int componentNumber) {
        return table + type + componentNumber;
    }

    /**
     * Create a BatchScanner for all rows in the given table
     */
    public static BatchScanner createBatchScanner(Graphulo g, String table) {
        return createBatchScanner(g, table, "");
    }

    /**
     * Create a BatchScanner for one row in the given table
     */
    public static BatchScanner createBatchScanner(Graphulo g, String table, String node) {
        return createBatchScanner(g, table, Collections.singleton(new Range(node)));
    }

    /**
     * Create a BatchScanner for all given rows in the given table
     */
    public static BatchScanner createBatchScanner(Graphulo g, String table, Collection<Range> ranges) {
        BatchScanner bs;
        try {
            bs = g.getConnector().createBatchScanner(table, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not create scanner for table " + table);
        }
        bs.setRanges(ranges);
        return bs;
    }
}
