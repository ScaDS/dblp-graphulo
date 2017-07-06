package de.alkern.connected_components.analysis;

import de.alkern.connected_components.ComponentType;
import de.alkern.connected_components.ConnectedComponentsUtils;
import de.alkern.connected_components.SizeType;
import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.apply.KeyRetainOnlyApply;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.data.*;
import org.apache.accumulo.core.security.Authorizations;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class Analyzer {

    private final Graphulo g;

    Analyzer(Graphulo graphulo) {
        g = graphulo;
    }

    /**
     * Create the metadata-table for the given table
     * It uses the existing component-tables
     * @param table
     */
    public void buildMetadataTable(String table) {
        BatchWriter bw;
        try {
            g.getConnector().tableOperations().create(Statistics.METATABLE(table));
            bw = g.getConnector().createBatchWriter(Statistics.METATABLE(table), new BatchWriterConfig());
        } catch (Exception e) {
            throw new RuntimeException("Could not create meta table for " + table, e);
        }

        try {
            List<String> weakComponents = ConnectedComponentsUtils.getExistingComponentTables(g, table,
                    ComponentType.WEAK);
            analyzeTables(bw, weakComponents, table, ComponentType.WEAK);
            List<String> strongComponents = ConnectedComponentsUtils.getExistingComponentTables(g, table,
                    ComponentType.STRONG);
            analyzeTables(bw, strongComponents, table, ComponentType.STRONG);
            bw.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException("Could not save metadata entries", e);
        }
    }

    private void analyzeTables(BatchWriter bw, List<String> components, String table, ComponentType type)
            throws MutationsRejectedException {
        for (String component : components) {
            //calculate component sizes
            Mutation numberEdges = new Mutation(component);
            numberEdges.put(Statistics.EMPTY_CFAM, SizeType.EDGES.toString(), Long.toString(g.countEntries(component)));
            Mutation numberNodes = new Mutation(component);
            numberNodes.put(Statistics.EMPTY_CFAM, SizeType.NODES.toString(), Long.toString(g.countRows(component)));
            bw.addMutation(numberEdges);
            bw.addMutation(numberNodes);

            bw.addMutation(getHighestOutDegree(component));
            bw.addMutation(getHighestInDegree(component));
        }
        Mutation numberComponents = new Mutation(table);
        numberComponents.put(Statistics.EMPTY_CFAM, type.repr(), Integer.toString(components.size()));
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
        mutation.put(Statistics.EMPTY_CFAM, Statistics.MAX_DEGREE_OUT, max.toString());
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
        mutation.put(Statistics.EMPTY_CFAM, Statistics.MAX_DEGREE_IN, String.valueOf(max));
        return mutation;
    }

}
