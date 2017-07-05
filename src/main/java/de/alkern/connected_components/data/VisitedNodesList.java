package de.alkern.connected_components.data;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * List implementation of {@see VisitedNodes}
 * Saves all the nodes in the main memory
 */
public class VisitedNodesList implements VisitedNodes {

    private Set<String> visited;

    public VisitedNodesList() {
        visited = new HashSet<>();
    }

    @Override
    public void visitNode(String node) {
        visited.add(node);
    }

    @Override
    public boolean hasVisited(String node) {
        return visited.contains(node);
    }

    @Override
    public void clear() {
        visited.clear();
    }

    @Override
    public Collection<String> getUnvisitedNodes(Collection<String> neighbours) {
        neighbours.removeAll(visited);
        return neighbours;
    }
}
