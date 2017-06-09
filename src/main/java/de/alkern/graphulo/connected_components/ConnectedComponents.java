package de.alkern.graphulo.connected_components;

import edu.mit.ll.graphulo.Graphulo;

public class ConnectedComponents {

    private static final String COMPONENT_SUFFIX = "_CC";

    /**
     * Finds all connected components and saves them in new tables
     *
     * @param graphulo
     * @param table    for which to find components
     */
    public static void find(Graphulo graphulo, String table, String degTable, String v0) {
        String neighbours = graphulo.AdjBFS(table, v0, 1, null, null,
                degTable, null, false, 0, Integer.MAX_VALUE);
        System.out.println("Nachbarn: " + neighbours);
    }
}
