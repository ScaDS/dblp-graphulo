package de.alkern.graphulo.connected_components;

import javax.annotation.Nullable;
import java.util.Collection;

/**
 * Queue to hold all nodes which yet need to be visited by the connected components algorithm
 */
public interface VisitQueue {

    /**
     * Adds the given node to the queue
     * @param node to add
     */
    public void add(String node);

    /**
     * Adds all nodes to the queue
     * @param nodes to add
     */
    public void addAll(Collection<String> nodes);

    /**
     * Gets the next node from the queue
     * @return next node or null
     */
    public @Nullable String poll();

    /**
     * Clears the queue
     */
    public void clear();

    /**
     * @return true if the queue is empty
     */
    public boolean isEmpty();
}
