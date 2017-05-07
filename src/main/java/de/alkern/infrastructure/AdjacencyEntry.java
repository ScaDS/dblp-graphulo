package de.alkern.infrastructure;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class AdjacencyEntry {

    private final Text startNode;
    private final Text endNode;
    private final Value edgeWeight;

    public AdjacencyEntry(String startNode, String endNode, String edgeWeight) {
        this.startNode = new Text(startNode);
        this.endNode = new Text(endNode);
        this.edgeWeight = new Value(edgeWeight);
    }

    public AdjacencyEntry(Text startNode, Text endNode, Value edgeWeight) {
        this.startNode = startNode;
        this.endNode = endNode;
        this.edgeWeight = edgeWeight;
    }

    public static AdjacencyEntry fromEntry(Map.Entry<Key, Value> entry) {
        Text row = entry.getKey().getRow();
        Text colQual = entry.getKey().getColumnQualifier();
        Value value = entry.getValue();
        return new AdjacencyEntry(row, colQual, value);
    }

    public Mutation toMutation() {
        Mutation mutation = new Mutation(startNode);
        mutation.put(new Text(""), endNode, edgeWeight);
        return mutation;
    }

    public String getStartNode() {
        return startNode.toString();
    }

    public String getEndNode() {
        return endNode.toString();
    }

    public String getEdgeWeight() {
        return edgeWeight.toString();
    }

    public String toString() {
        return startNode + " :" + endNode + " []   -> " + edgeWeight;
    }

}
