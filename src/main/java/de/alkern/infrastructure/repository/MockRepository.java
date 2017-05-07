package de.alkern.infrastructure.repository;

import java.util.*;

/**
 * Mockrepository for unit tests
 */
public class MockRepository implements Repository {

    private List<String> entries;

    public MockRepository() {
        entries = new LinkedList<>();
    }

    @Override
    public void save(String row, String qualifier, String value) {
        entries.add(buildExample(row, qualifier, value));
    }

    private String buildExample(String row, String qualifier, String value) {
        return row + " :" + qualifier + "   -> " + value;
    }

    @Override
    public List scan() {
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
