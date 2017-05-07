package de.alkern.infrastructure.repository;

import de.alkern.infrastructure.entry.AccumuloEntry;

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

    private String buildExample(String row, String qualifier, String value) {
        return row + " :" + qualifier + "   -> " + value;
    }

    @Override
    public List<AccumuloEntry> scan() {
        return entries;
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public void close() {

    }
}
