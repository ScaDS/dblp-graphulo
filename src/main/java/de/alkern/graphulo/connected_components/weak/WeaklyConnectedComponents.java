package de.alkern.graphulo.connected_components.weak;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;

public class WeaklyConnectedComponents {

    private final static String SUFFIX = "_cc";

    private Graphulo g;
    private Connector conn;
    private TableOperations tops;
    private String table;
    private Map<String, String> components;
    private String alreadyVisited;

    public WeaklyConnectedComponents(Graphulo g) {
        this.g = g;
        this.conn = g.getConnector();
        this.tops = conn.tableOperations();
        this.components = new HashMap<>();
    }

    public void calculateConnectedComponents(String table) throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        this.table = table;
        this.components.clear();
        this.alreadyVisited = "";
        Scanner scanner = conn.createScanner(table, Authorizations.EMPTY);
        scanner.setRange(new Range());
        scanner.iterator().forEachRemaining(this::visit);
        scanner.close();
        copyComponents();
    }

    private void visit(Map.Entry<Key, Value> entry) {
        String row = entry.getKey().getRow().toString() + ";";
        if (alreadyVisited.contains(row)) {
            return;
        }
        int counter = 1;
        String oldNeighbours;
        String newNeighbours = "";
        do {
            oldNeighbours = newNeighbours;
            newNeighbours = g.AdjBFS(table, row, counter++, null, null, null,
                    4, null, null, false, 0,
                    Integer.MAX_VALUE, null, null, null, true,
                    null);
        } while (!oldNeighbours.equals(newNeighbours));
        alreadyVisited = alreadyVisited + row + newNeighbours;
        components.put(row.replace(";", ""), newNeighbours);
    }

    private void copyComponents() throws TableNotFoundException, TableExistsException, AccumuloSecurityException, AccumuloException {
        int componentNumber = 1;
        for (Map.Entry<String, String> entry: components.entrySet()) {
            SortedSet<Range> rangeSet = GraphuloUtil.d4mRowToRanges(entry.getValue());
            rangeSet.add(new Range(entry.getKey()));

            String ccName = table + SUFFIX + componentNumber++;
            BatchScanner bs = conn.createBatchScanner(table, Authorizations.EMPTY, 10);
            bs.setRanges(rangeSet);
            bs.addScanIterator(new IteratorSetting(10, "Writer for " + ccName, RemoteWriteIterator.class,
                    g.basicRemoteOpts("", ccName, null, null)));
            tops.create(ccName);
            for (Map.Entry<Key, Value> k : bs) {

            }
            bs.close();
        }
    }
}
