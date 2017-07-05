package de.alkern.graphulo.connected_components.analysis;

import de.alkern.graphulo.connected_components.ComponentType;
import de.alkern.graphulo.connected_components.ConnectedComponentsUtils;
import de.alkern.graphulo.connected_components.SizeType;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Analyzer for connected components
 * Saves the number of components, and the sizes of the single components into a table with _meta-suffix
 */
public class Statistics {

    public final static String META_SUFFIX = "_meta";

    private final Graphulo g;

    public static String metatable(String table) {
        return table + META_SUFFIX;
    }

    public Statistics(Graphulo graphulo) {
        this.g = graphulo;
    }

    /**
     * Create the metadata-table for the given table
     * It uses the existing component-tables
     * @param table
     */
    public void buildMetadataTable(String table) {
        BatchWriter bw;
        try {
            g.getConnector().tableOperations().create(metatable(table));
            bw = g.getConnector().createBatchWriter(metatable(table), new BatchWriterConfig());
        } catch (Exception e) {
            throw new RuntimeException("Could not create meta table for " + table, e);
        }

        try {
            List<String> weakComponents = ConnectedComponentsUtils.getExistingComponentTables(g, table, ComponentType.WEAK);
            analyzeTables(bw, weakComponents, table, ComponentType.WEAK);
            List<String> strongComponents = ConnectedComponentsUtils.getExistingComponentTables(g, table, ComponentType.STRONG);
            analyzeTables(bw, strongComponents, table, ComponentType.STRONG);
            bw.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException("Could not save metadata entries", e);
        }
    }

    private void analyzeTables(BatchWriter bw, List<String> components, String table, ComponentType type) throws MutationsRejectedException {
        for (String component : components) {
            Mutation numberEdges = new Mutation(component);
            numberEdges.put(empty(), SizeType.EDGES.toString(), Long.toString(g.countEntries(component)));
            Mutation numberNodes = new Mutation(component);
            numberNodes.put(empty(), SizeType.NODES.toString(), Long.toString(g.countRows(component)));
            bw.addMutation(numberEdges);
            bw.addMutation(numberNodes);
        }
        Mutation numberComponents = new Mutation(table);
        numberComponents.put(empty(), type.repr(), Integer.toString(components.size()));
        bw.addMutation(numberComponents);
    }

    private String empty() {
        return "";
    }

    /**
     *
     * @param table Original table
     * @param type component type
     * @return number of components of the given type
     */
    public int getNumberOfComponents(String table, ComponentType type) {
        return get(table, table, type.repr());
    }

    /**
     *
     * @param table Original table
     * @param type component type
     * @param componentNumber number of the exact component
     * @return how many edges are in the component
     */
    public int getNumberOfEdges(String table, ComponentType type, int componentNumber) {
        return get(table, table + type + componentNumber, SizeType.EDGES.toString());
    }

    /**
     *
     * @param table Original table
     * @param type component type
     * @param componentNumber number of the exact component
     * @return how many nodes are in the component
     */
    public int getNumberOfNodes(String table, ComponentType type, int componentNumber) {
        return get(table, table + type + componentNumber, SizeType.NODES.toString());
    }

    private int getNumberOf(String table, ComponentType type, int componentNumber, SizeType size) {
        switch (size) {
            case EDGES: return getNumberOfEdges(table, type, componentNumber);
            case NODES: return getNumberOfNodes(table, type, componentNumber);
            default: throw new RuntimeException("Invalid SizeType " + size + ". Should not happen.");
        }
    }

    private int get(String table, String row, String column) {
        try {
            Scanner s = g.getConnector().createScanner(metatable(table), Authorizations.EMPTY);
            s.setRange(new Range(row));
            s.fetchColumn(new Text(empty()), new Text(column));

            int result = 0;
            for (Map.Entry<Key, Value> entry : s) {
                result = Integer.valueOf(entry.getValue().toString());
            }
            s.close();
            return result;
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Metatable does not exist", e);
        }
    }

    /**
     * Get the number of nodes in all components
     * @param table original table name
     * @param type component type
     * @param sizeType size to check
     * @return
     */
    double[] getComponentSizes(String table, ComponentType type, SizeType sizeType) {
        List<Integer> sizes = new LinkedList<>();
        int size = getNumberOfComponents(table, type);
        for (int i = 1; i <= size; i++) {
            sizes.add(getNumberOf(table, type, i, sizeType));
        }
        double[] result = new double[sizes.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = sizes.get(i).doubleValue();
        }
        return result;
    }
}
