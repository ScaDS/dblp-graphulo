package de.alkern.graphulo.connected_components.strong;

import de.alkern.graphulo.connected_components.ConnectedComponentsUtils;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.DebugUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.HashMap;
import java.util.Map;

/**
 * Calculator for strongly connected components
 * Algorithm found in "Graph ALgorithms in the Language of Linear Algebra" Chapter 3 by C. M. Rader
 * Strongly connected components are such as from every node every other in the component is reachable.
 * Differs from weakly connected components in directed graphs.
 */
public class StronglyConnectedComponents {

    private final Graphulo g;
    private final TableOperations tops;
    private String table;
    private Map<String, String> reachableNodes;

    public StronglyConnectedComponents(Graphulo graphulo) {
        g = graphulo;
        tops = g.getConnector().tableOperations();
        reachableNodes = new HashMap<>();
    }

    public void calculateStronglyConnectedComponents(String table) throws TableNotFoundException {
        this.reachableNodes.clear();
        this.table = table;

        Scanner scanner = g.getConnector().createScanner(table, Authorizations.EMPTY);
        scanner.setRange(new Range());
        scanner.iterator().forEachRemaining(this::visit);
        scanner.close();

        buildCTables();
    }

    private void visit(Map.Entry<Key, Value> entry) {
        String row = entry.getKey().getRow().toString();
        String reachable = ConnectedComponentsUtils.getAllNeighbours(g, table, row);
        this.reachableNodes.put(row, reachable);
    }

    private void buildCTables() {
        try {
            tops.create(table + "_c");
            tops.create(table + "_ct");
            BatchWriterConfig cfg = new BatchWriterConfig();
            BatchWriter cWriter = g.getConnector().createBatchWriter(table + "_c", cfg);
            BatchWriter ctWriter = g.getConnector().createBatchWriter(table + "_ct", cfg);
            for (Map.Entry<String, String> entry : reachableNodes.entrySet()) {
                String row = entry.getKey();
                String neighbours = entry.getValue();
                addCMutations(row, row, cWriter, ctWriter);
                for (String neighbour : neighbours.split(";")) {
                    addCMutations(row, neighbour, cWriter, ctWriter);
                }
            }
            cWriter.flush();
            ctWriter.flush();
        } catch (Exception e) {
            throw new RuntimeException("Error while building cTables", e);
        }
    }

    private void addCMutations(String row, String column, BatchWriter cWriter, BatchWriter ctWriter) {
        try {
            Mutation m = new Mutation(row);
            m.put("", column, "1");
            Mutation mt = new Mutation(column);
            mt.put("", row, "1");
            cWriter.addMutation(m);
            ctWriter.addMutation(mt);
        } catch (MutationsRejectedException e) {
            throw new RuntimeException(e);
        }
    }
}
