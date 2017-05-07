package de.alkern.infrastructure.repository;

import de.alkern.infrastructure.entry.AccumuloEntry;

import java.util.List;

/**
 * Interface for AccumuloRepositories used with Graphulo
 */
public interface Repository {
    /**
     * Save an entry to the Accumulo-Instance
     *
     * @param entry: The entry to save
     */
    public void save(AccumuloEntry entry);

    /**
     * Scan the database
     *
     * @return all entries which belong to this repository
     */
    public List<AccumuloEntry> scan();

    /**
     * Delete all entries which belong to this repository
     */
    public void clear();

    /**
     * Close the used writer
     */
    public void close();
}
