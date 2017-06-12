package de.alkern.graphulo.connected_components;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * List implementation of {@see VisitedNodes}
 * Saves all the nodes in the main memory
 */
public class VisitedNodesList implements VisitedNodes {

    private List<String> visited;

    public VisitedNodesList() {
        visited = new ArrayList<>();
    }

    @Override
    public void visitNode(String node) {
        if (!visited.contains(node)) {
            visited.add(node);
            return;
        }
        throw new RuntimeException("Node " + node + " could not be added because it was already visited!");
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
