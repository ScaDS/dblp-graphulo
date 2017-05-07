package de.alkern.infrastructure.repository;

import java.util.List;

/**
 * Interface for AccumuloRepositories used with Graphulo
 */
public interface Repository {
    /**
     * Save an entry to the Accumulo-Instance
     *
     * @param row:       start node label
     * @param qualifier: end node label
     * @param value:     edge weight
     */
    public void save(String row, String qualifier, String value);

    /**
     * Scan the database
     *
     * @return all entries which belong to this repository
     */
    public List scan();

    /**
     * Delete all entries which belong to this repository
     */
    public void clear();

    /**
     * Close the used writer
     */
    public void close();
}
