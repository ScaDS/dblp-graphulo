package de.alkern.infrastructure.entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import java.util.Map;

public class DegreeEntry implements AccumuloEntry {

    private final Text node;
    private final DegreeLabel label;
    private final Value degrees;

    public enum DegreeLabel {
        IN,
        OUT
    }

    public DegreeEntry(String node, DegreeLabel label, String degrees) {
        this.node = new Text(node);
        this.label = label;
        this.degrees = new Value(degrees);
    }

    public DegreeEntry(Text node, DegreeLabel label, Value degrees) {
        this.node = node;
        this.label = label;
        this.degrees = degrees;
    }

    public static class DegreeBuilder implements AccumuloEntry.Builder {
        @Override
        public AccumuloEntry fromMapEntry(Map.Entry<Key, Value> mapEntry) {
            Text row = mapEntry.getKey().getRow();
            Text colQual = mapEntry.getKey().getColumnQualifier();
            DegreeLabel label = colQual.toString().equals("IN") ? DegreeLabel.IN : DegreeLabel.OUT;
            Value value = mapEntry.getValue();
            return new DegreeEntry(row, label, value);
        }
    }

    @Override
    public Mutation toMutation() {
        return null;
    }

    public String getNode() {
        return node.toString();
    }

    public DegreeLabel getLabel() {
        return label;
    }

    public String getDegrees() {
        return degrees.toString();
    }

    public String toString() {
        return node + " :" + label + " []   -> " + degrees;
    }
}
