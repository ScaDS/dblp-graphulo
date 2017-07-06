package de.alkern.infrastructure.repository;

import de.alkern.infrastructure.entry.AccumuloEntry;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
     * Scan the database
     *
     * @return Iterator over all entries
     */
    public Iterator<Map.Entry<Key, Value>> getIterator();

    /**
     * Delete all entries which belong to this repository
     */
    public void clear();

    /**
     * Close the used writer
     */
    public void close();

    /**
     * Flush saved mutations
     */
    void flush();
}
