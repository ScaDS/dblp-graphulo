package de.alkern.connected_components;

import com.google.common.collect.Iterators;
import edu.mit.ll.graphulo.apply.ApplyIterator;
import edu.mit.ll.graphulo.apply.ApplyOp;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Operator to format degree table entries to adjacence table entries
 * Node1 : [] value1 -> Node1 :Node1 [] value1
 * Used to create a laplacian matrix from degTable - adjTable
 */
@Deprecated
public class LaplacianPrepareOp implements ApplyOp {

    public static IteratorSetting iteratorSetting(int priority) {
        IteratorSetting itset = new IteratorSetting(priority, ApplyIterator.class);
        itset.addOption(ApplyIterator.APPLYOP, LaplacianPrepareOp.class.getName());
        return itset;
    }

    @Override
    public void init(Map<String, String> map, IteratorEnvironment iteratorEnvironment) throws IOException {

    }

    @Override
    public Iterator<? extends Map.Entry<Key, Value>> apply(Key key, Value value) throws IOException {
        Key knew;
        knew = new Key(key.getRowData().toArray(), "".getBytes(), key.getRowData().toArray(),
                key.getColumnVisibilityData().toArray(), key.getTimestamp(), key.isDeleted(), true);
        return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(knew, value));
    }

    @Override
    public void seekApplyOp(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {

    }
}
