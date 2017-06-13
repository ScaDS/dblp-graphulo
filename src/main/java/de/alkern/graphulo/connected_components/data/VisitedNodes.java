package de.alkern.graphulo.connected_components.data;

import java.util.Collection;

/**
 * Interface for a collection of nodes visited by the connected components algorithm
 */
public interface VisitedNodes {

    /**
     * Adds the given node to the collection
     * @param node which was visited
     */
    public void visitNode(String node);

    /**
     * Checks if the given node was already visited
     * @param node to check
     * @return true if the node was visited yet
     */
    public boolean hasVisited(String node);

    /**
     * Empties the collection
     */
    public void clear();

    /**
     * Finds all unvisited nodes in the given collection
     * @param neighbours collection to check
     * @return collection of all yet unvisited nodes
     */
    public Collection<String> getUnvisitedNodes(Collection<String> neighbours);
}
