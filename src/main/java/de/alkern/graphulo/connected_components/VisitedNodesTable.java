package de.alkern.graphulo.connected_components;

import java.util.Collection;

/**
 * Accumulo table implementation of {@see VisitedNodes}
 * Saves visited nodes in a temporary table
 */
public class VisitedNodesTable implements VisitedNodes {
    @Override
    public void visitNode(String node) {

    }

    @Override
    public boolean hasVisited(String node) {
        return false;
    }

    @Override
    public void clear() {

    }

    @Override
    public Collection<String> getUnvisitedNodes(Collection<String> neighbours) {
        return null;
    }
}
