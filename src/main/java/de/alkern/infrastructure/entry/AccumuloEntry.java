package de.alkern.infrastructure.entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;

import java.util.Map;

public interface AccumuloEntry {
    public Mutation toMutation();

    interface Builder<T extends AccumuloEntry> {
        public T fromMapEntry(Map.Entry<Key, Value> mapEntry);
    }
}
