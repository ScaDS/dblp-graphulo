package de.alkern.infrastructure.entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class IncidenceEntry implements AccumuloEntry {

    private static final String SEPARATOR = "|";

    private final Text edgeLabel;
    private final String edgeDirection;
    private final String nodeLabel;
    private final Value edgeWeight;

    public IncidenceEntry(String edgeLabel, String edgeDirection, String nodeLabel, String edgeWeight) {
        this.edgeLabel = new Text(edgeLabel);
        this.edgeDirection = edgeDirection;
        this.nodeLabel = nodeLabel;
        this.edgeWeight = new Value(edgeWeight);
    }

    public IncidenceEntry(Text edgeLabel, String edgeDirection, String nodeLabel, Value edgeWeight) {
        this.edgeLabel = edgeLabel;
        this.edgeDirection = edgeDirection;
        this.nodeLabel = nodeLabel;
        this.edgeWeight = edgeWeight;
    }

    public static class IncidenceBuilder implements AccumuloEntry.Builder<IncidenceEntry> {
        @Override
        public IncidenceEntry fromMapEntry(Map.Entry<Key, Value> mapEntry) {
            Text row = mapEntry.getKey().getRow();
            Text colQual = mapEntry.getKey().getColumnQualifier();
            String edgeDirection = colQual.toString().split(SEPARATOR)[0];
            String nodeLabel = colQual.toString().split(SEPARATOR)[0];
            Value value = mapEntry.getValue();
            return new IncidenceEntry(row, edgeDirection, nodeLabel, value);
        }
    }

    @Override
    public Mutation toMutation() {
        Mutation mutation = new Mutation(edgeLabel);
        String colQual = edgeDirection + SEPARATOR + nodeLabel;
        mutation.put(new Text(""), new Text(colQual), edgeWeight);
        return mutation;
    }

    public String getEdgeLabel() {
        return edgeLabel.toString();
    }

    public String getEdgeDirection() {
        return edgeDirection;
    }

    public String getNodeLabel() {
        return nodeLabel;
    }

    public String getEdgeWeight() {
        return edgeWeight.toString();
    }

    public String toString() {
        return edgeLabel + " :" + edgeDirection + " []   -> " + edgeWeight;
    }
}
