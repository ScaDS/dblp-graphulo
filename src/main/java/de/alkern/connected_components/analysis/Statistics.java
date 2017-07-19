package de.alkern.connected_components.analysis;

import de.alkern.connected_components.ComponentType;
import de.alkern.connected_components.ConnectedComponentsUtils;
import de.alkern.connected_components.SizeType;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply;
import edu.mit.ll.graphulo.skvi.D4mRangeFilter;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    private static final String META_SUFFIX = "_meta";
    static final String MAX_DEGREE_OUT = "MaxOutDegree";
    static final String MAX_DEGREE_IN = "MaxInDegree";
    static final String EMPTY_CFAM = "";

    private final Graphulo g;
    private final Analyzer analyzer;

    public static String METATABLE(String table) {
        return table + META_SUFFIX;
    }

    public Statistics(Graphulo graphulo) {
        this.g = graphulo;
        this.analyzer = new Analyzer(g);
    }

    /**
     * Create the metadata-table for the given table
     * It uses the existing component-tables
     * @param table
     */
    public void buildMetadataTable(String table) {
        analyzer.buildMetadataTable(table);
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

    /**
     * @param table original table name
     * @param type component type
     * @param componentNumber number of the component
     * @return highest out degree in the given component table
     */
    public int getHighestOutDegree(String table, ComponentType type, int componentNumber) {
        return get(table, ConnectedComponentsUtils.getComponentTableName(table, type, componentNumber), MAX_DEGREE_OUT);
    }

    /**
     * @param table original table name
     * @param type component type
     * @param componentNumber number of the component
     * @return highest in degree in the given component table
     */
    public int getHighestInDegree(String table, ComponentType type, int componentNumber) {
        return get(table, ConnectedComponentsUtils.getComponentTableName(table, type, componentNumber), MAX_DEGREE_IN);
    }

    /**
     * Ad hoc calculated out degree value
     * @param table table to check
     * @param node name of the node
     * @return out degree value of the given node
     */
    public int getOutDegree(String table, String node) {
        BatchScanner bs;
        try {
            bs = g.getConnector().createBatchScanner(table, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not scan table " + table, e);
        }
        bs.setRanges(Collections.singleton(new Range(node)));

        DynamicIteratorSetting dis = new DynamicIteratorSetting(22, "getOutDegree");
        dis
                .append(KeyRetainOnlyApply.iteratorSetting(1, PartialKey.ROW))
                .append(Graphulo.PLUS_ITERATOR_BIGDECIMAL);
        dis.addToScanner(bs);

        return Integer.valueOf(bs.iterator().next().getValue().toString());
    }

    /**
     * Ad hoc calculated in degree value
     * @param table table to check
     * @param node name of the node
     * @return in degree value of the given node
     */
    public int getInDegree(String table, String node) {
        BatchScanner bs;
        try {
            bs = g.getConnector().createBatchScanner(table, Authorizations.EMPTY, 15);
        } catch (TableNotFoundException e) {
            throw new RuntimeException("Could not scan table " + table, e);
        }
        bs.setRanges(Collections.singleton(new Range()));
        bs.addScanIterator(D4mRangeFilter.iteratorSetting(1, D4mRangeFilter.KeyPart.COLQ, node + ";"));

        int counter = 0;
        for (Map.Entry<Key, Value> entry : bs) {
            counter++;
        }
        return counter;
    }

    /**
     * Helper Method to get a certain value in the metatable
     */
    private int get(String table, String row, String column) {
        try {
            Scanner s = g.getConnector().createScanner(METATABLE(table), Authorizations.EMPTY);
            s.setRange(new Range(row));
            s.fetchColumn(new Text(EMPTY_CFAM), new Text(column));

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
     * Ad hoc calculated closeness centrality
     * Closeness centrality is 1 / sum of shortest paths to all reachable nodes
     * @param table table to check
     * @param node to check
     * @return closeness centrality, Double.POSITIVE_INFINITY if node has no neighbours
     */
    public double getClosenessCentrality(String table, String node) {
        Map<String, Integer> shortestPaths = new HashMap<>();
        shortestPaths.put(node, 0);
        boolean addedSomething = true;
        int distance = 1;

        while (addedSomething) {
            String neighbours = g.AdjBFS(table, node + ";", distance, null, null,
                    null,null, false, 0, Integer.MAX_VALUE);
            Collection<String> neighbourList = GraphuloUtil.d4mRowToTexts(neighbours).stream().map(Text::toString)
                    .collect(Collectors.toCollection(TreeSet::new));
            addedSomething = false;
            for (String it : neighbourList) {
                if (!shortestPaths.keySet().contains(it)) {
                    shortestPaths.put(it, distance);
                    addedSomething = true;
                }
            }
            distance++;
        }
        return 1d / shortestPaths.values().stream().reduce(0, Integer::sum);
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
