package de.alkern.graphulo.connected_components.strong;

import edu.mit.ll.graphulo.ewise.EWiseOp;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;

public class LogicalAndOp implements EWiseOp, Iterator<Map.Entry<Key, Value>> {

    private Key emitKey;
    private Key tempKey;
    private Value emitValue;

    @Override
    public boolean hasNext() {
        return emitKey != null;
    }

    @Override
    public Map.Entry<Key, Value> next() {
        Key emitK = emitKey;
        emitKey = tempKey;
        tempKey = null;
        return new AbstractMap.SimpleImmutableEntry<>(emitK, emitValue);
    }

    @Override
    public void init(Map<String, String> options, IteratorEnvironment env) throws IOException {

    }

    @Override
    public Iterator<? extends Map.Entry<Key, Value>> multiply(ByteSequence Mrow, ByteSequence McolF, ByteSequence
            McolQ, ByteSequence McolVis, long Atime, long Btime, Value Aval, Value Bval) {
        emitKey = new Key(Mrow.toArray(), McolF.toArray(), McolQ.toArray(), McolVis.toArray(), System.nanoTime());
        tempKey = emitKey;
        int valA = Integer.valueOf(Aval.toString());
        int valB = Integer.valueOf(Bval.toString());
        if (valA > 0 && valB > 0) {
            emitValue = new Value("1");
        }
        else {
            emitValue = new Value("0");
        }
        return this;
    }
}
