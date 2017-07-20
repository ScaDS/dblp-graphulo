package de.alkern.connected_components.strong;

import de.alkern.connected_components.ComponentType;
import de.alkern.connected_components.data.VisitedNodesList;
import de.alkern.connected_components.ConnectedComponentsUtils;
import de.alkern.connected_components.data.VisitedNodes;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.D4mRangeFilter;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedSet;

/**
 * Calculator for strongly connected components
 * Algorithm found in "Graph ALgorithms in the Language of Linear Algebra" Chapter 3 by C. M. Rader
 * Strongly connected components are such as from every node every other in the component is reachable.
 * Differs from weakly connected components in directed graphs.
 * Every connected component is saved in a table named original table + _scc + number
 */
public class StronglyConnectedComponents {

    private final Graphulo g;
    private final TableOperations tops;
    private String table;
    private final Map<String, String> reachableNodes;
    private final VisitedNodes visited;
    private int counter;

    public StronglyConnectedComponents(Graphulo graphulo, VisitedNodes visited) {
        g = graphulo;
        tops = g.getConnector().tableOperations();
        reachableNodes = new HashMap<>();
        this.visited = visited;
    }

    public void calculateConnectedComponents(String table) {
        //TODO add flag to simplify for directed graphs -> no transpose and ewise needed
        this.reachableNodes.clear();
        this.table = table;
        this.visited.clear();
        this.counter = 1;

        Scanner scanner;
        try {
            scanner = g.getConnector().createScanner(table, Authorizations.EMPTY);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not calculate strongly ccs", e);
        }
        scanner.setRange(new Range());
        scanner.iterator().forEachRemaining(this::visit);
        scanner.close();

        buildCTables();
        andOnCTables();
        extractComponents();
        deleteTempTables();
    }

    private void visit(Map.Entry<Key, Value> entry) {
        String row = entry.getKey().getRow().toString();
        String reachable = ConnectedComponentsUtils.getAllNeighbours(g, table, row);
        this.reachableNodes.put(row, reachable);
    }

    /**
     * Build the c-Table and its transposed version
     * The c table shows every node that is reachable from the row-node
     */
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
            cWriter.close();
            ctWriter.close();
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

    /**
     * Logical and on the c- and cT-Table
     * Result shows strongly connected components
     * Result is saved in table with _res-postfix
     */
    private void andOnCTables() {
        String cTable = table + "_c";
        String ctTable = table + "_ct";
        String resultTable = table + "_res";
        g.SpEWiseX(cTable, ctTable, resultTable, null, 5, LogicalAndOp.class,
                null, null, null, null, null, null,
                null, null, null, null, -1,
                Authorizations.EMPTY, Authorizations.EMPTY);
    }

    private void extractComponents() {
        try {
            Scanner scanner = g.getConnector().createScanner(table + "_res", Authorizations.EMPTY);
            scanner.setRange(new Range());
            RowIterator rowIterator = new RowIterator(scanner);
            rowIterator.forEachRemaining(this::extractRow);
        } catch (Exception e) {
            throw new RuntimeException("Error while extracting components", e);
        }
    }

    /**
     * Extract and copy a single strongly connected component
     * This method also checks if a component was already copied and returns in this case
     * @param entryIterator
     */
    private void extractRow(Iterator<Map.Entry<Key, Value>> entryIterator) {
        try {
            //concatenate all nodes belonging to a component as dm4-string
            String row = "";
            StringBuilder range = new StringBuilder();
            while (entryIterator.hasNext()) {
                Map.Entry<Key, Value> entry = entryIterator.next();
                row = entry.getKey().getRow().toString();
                String column = entry.getKey().getColumnQualifier().toString();
                if (visited.hasVisited(column)) {
                    return;
                }
                range.append(column);
                range.append(";");
            }
            visited.visitNode(row);

            //create the component table and copy all relevant entries
            String componentTable = ConnectedComponentsUtils.getComponentTableName(table, ComponentType.STRONG,
                    counter++);
            tops.create(componentTable);
            SortedSet<Range> rangeSet = GraphuloUtil.d4mRowToRanges(range.toString());
            BatchScanner bs = ConnectedComponentsUtils.createBatchScanner(g, table, rangeSet);

            //filter entries that lead to rows which are not in the component
            DynamicIteratorSetting dis = new DynamicIteratorSetting(5, "");
            dis.append(D4mRangeFilter.iteratorSetting(6, D4mRangeFilter.KeyPart.COLQ, range.toString()));
            dis.append(new IteratorSetting(10, "RMW " + componentTable, RemoteWriteIterator.class,
                    g.basicRemoteOpts("", componentTable, "", Authorizations.EMPTY)));
            dis.addToScanner(bs);
            for (Map.Entry<Key, Value> entry : bs) {

            }
            bs.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteTempTables() {
        try {
            tops.delete(table + "_c");
            tops.delete(table + "_ct");
            tops.delete(table + "_res");
        } catch (Exception e) {
            throw new RuntimeException("Could not delete temp tables of strongly connected components", e);
        }
    }
}
