package de.alkern.graphulo.examples;

import com.google.common.collect.Iterators;
import de.alkern.infrastructure.entry.AccumuloEntry;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import edu.mit.ll.graphulo.apply.ApplyOp;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.*;

//simple test for an implementation
@Deprecated
public class MultiplyWithValueOp implements ApplyOp {

    @Override
    public void init(Map<String, String> options, IteratorEnvironment iteratorEnvironment) throws IOException {

    }

    @Override
    public Iterator<? extends Map.Entry<Key, Value>> apply(Key key, Value value) throws IOException {
        Value newValue = new Value(new Text("" + (Integer.valueOf(value.toString()) * 4)));
        return Iterators.singletonIterator(new AbstractMap.SimpleImmutableEntry<>(key, newValue));
    }

    @Override
    public void seekApplyOp(Range range, Collection<ByteSequence> collection, boolean b) throws IOException {

    }

    public List<AccumuloEntry> use(Iterator<? extends Map.Entry<Key, Value>> iter) {
        List<AccumuloEntry> entries = new LinkedList<>();
        AccumuloEntry.Builder entryBuilder = new AdjacencyEntry.AdjacencyBuilder();
        iter.forEachRemaining(it -> {
            try {
                this.apply(it.getKey(), it.getValue()).forEachRemaining(transformed ->
                        entries.add(entryBuilder.fromMapEntry(transformed)));
            } catch (IOException e) {
                System.err.println("IOException in MultiplyWithValueOp at " + it);
            }
        });
        return entries;
    }
}
