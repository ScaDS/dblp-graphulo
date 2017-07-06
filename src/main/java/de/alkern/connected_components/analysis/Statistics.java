package de.alkern.connected_components.analysis;

import de.alkern.connected_components.ComponentType;
import de.alkern.connected_components.ConnectedComponentsUtils;
import de.alkern.connected_components.SizeType;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Analyzer for connected components
 * Calculates and saves additional information into a table with _meta-suffix
 *
 * - number of components
 * - sizes of the single components
 * - highest in degree of a node in the component
 * - highest out degree of a node in the component
 */
public class Statistics {

    public final static String META_SUFFIX = "_meta";
    public final static String MAX_DEGREE_OUT = "MaxOutDegree";
    public final static String MAX_DEGREE_IN = "MaxInDegree";

    private final Graphulo g;

    public static String METATABLE(String table) {
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
            g.getConnector().tableOperations().create(METATABLE(table));
            bw = g.getConnector().createBatchWriter(METATABLE(table), new BatchWriterConfig());
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
            //calculate component sizes
            Mutation numberEdges = new Mutation(component);
            numberEdges.put(emptyCFam(), SizeType.EDGES.toString(), Long.toString(g.countEntries(component)));
            Mutation numberNodes = new Mutation(component);
            numberNodes.put(emptyCFam(), SizeType.NODES.toString(), Long.toString(g.countRows(component)));
            bw.addMutation(numberEdges);
            bw.addMutation(numberNodes);

            bw.addMutation(getHighestOutDegree(component));
            bw.addMutation(getHighestInDegree(component));
        }
        Mutation numberComponents = new Mutation(table);
        numberComponents.put(emptyCFam(), type.repr(), Integer.toString(components.size()));
        bw.addMutation(numberComponents);
    }

    /**
     * Calculate the node with the heighest degree in the table
     * @param component name of the table
     * @return mutation with the highest degree
     */
    private Mutation getHighestOutDegree(String component) {
        BatchScanner bs;
        try {
            bs = g.getConnector().createBatchScanner(component, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not scan table " + component, e);
        }
        bs.setRanges(Collections.singleton(new Range()));
        //set Iterators to sum neighbours
        DynamicIteratorSetting dis = new DynamicIteratorSetting(22, "getMaxDegree");
        dis
                .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))
                .append(Graphulo.PLUS_ITERATOR_BIGDECIMAL);
        dis.addToScanner(bs);
        //find the highest degree in the table
        BigDecimal max = BigDecimal.ZERO;
        for (Map.Entry<Key, Value> entry : bs) {
            BigDecimal current = new BigDecimal(entry.getValue().toString());
            max = max.max(current);
        }
        bs.close();
        Mutation mutation = new Mutation(component);
        mutation.put(emptyCFam(), MAX_DEGREE_OUT, max.toString());
        return mutation;
    }

    private Mutation getHighestInDegree(String component) {
        BatchScanner bs;
        try {
            bs = g.getConnector().createBatchScanner(component, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not scan table " + component, e);
        }
        bs.setRanges(Collections.singleton(new Range()));
        bs.addScanIterator(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW_COLFAM_COLQUAL));

        //count incoming edges for each node
        Map<String, AtomicInteger> counter = new HashMap<>();
        for (Map.Entry<Key, Value> entry : bs) {
            String node = entry.getKey().getColumnQualifier().toString();
            AtomicInteger value = counter.get(node);
            if (value != null) value.incrementAndGet();
            else counter.put(node, new AtomicInteger(1));
        }
        bs.close();

        int max = counter.values().stream().map(AtomicInteger::intValue).max(Integer::compare).orElse(0);
        Mutation mutation = new Mutation(component);
        mutation.put(emptyCFam(), MAX_DEGREE_IN, String.valueOf(max));
        return mutation;
    }

    private String emptyCFam() {
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
        return get(table, ConnectedComponentsUtils.getComponentTableName(table, type, componentNumber),
                SizeType.EDGES.toString());
    }

    /**
     *
     * @param table Original table
     * @param type component type
     * @param componentNumber number of the exact component
     * @return how many nodes are in the component
     */
    public int getNumberOfNodes(String table, ComponentType type, int componentNumber) {
        return get(table, ConnectedComponentsUtils.getComponentTableName(table, type, componentNumber),
                SizeType.NODES.toString());
    }

    private int getNumberOf(String table, ComponentType type, int componentNumber, SizeType size) {
        switch (size) {
            case EDGES: return getNumberOfEdges(table, type, componentNumber);
            case NODES: return getNumberOfNodes(table, type, componentNumber);
            default: throw new RuntimeException("Invalid SizeType " + size + ". Should not happen.");
        }
    }

    public int getHighestOutDegree(String table, ComponentType type, int componentNumber) {
        return get(table, ConnectedComponentsUtils.getComponentTableName(table, type, componentNumber), MAX_DEGREE_OUT);
    }

    public int getHighestInDegree(String table, ComponentType type, int componentNumber) {
        return get(table, ConnectedComponentsUtils.getComponentTableName(table, type, componentNumber), MAX_DEGREE_IN);
    }

    private int get(String table, String row, String column) {
        try {
            Scanner s = g.getConnector().createScanner(METATABLE(table), Authorizations.EMPTY);
            s.setRange(new Range(row));
            s.fetchColumn(new Text(emptyCFam()), new Text(column));

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
