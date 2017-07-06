package de.alkern.infrastructure.repository;

import de.alkern.infrastructure.entry.AccumuloEntry;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.*;

/**
 * Mockrepository for unit tests
 */
public class MockRepository implements Repository {

    private List<AccumuloEntry> entries;

    public MockRepository() {
        entries = new LinkedList<>();
    }

    @Override
    public void save(AccumuloEntry entry) {
        entries.add(entry);
    }

    @Override
    public List<AccumuloEntry> scan() {
        return entries;
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> getIterator() {
        return null;
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public void close() {}

    @Override
    public void flush() {}
}
