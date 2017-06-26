package de.alkern.graphulo.connected_components.strong;

import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.DebugUtil;
import org.apache.accumulo.core.client.TableNotFoundException;

/**
 * Calculator for strongly connected components
 * Algorithm found in "Graph ALgorithms in the Language of Linear Algebra" Chapter 3 by C. M. Rader
 * Strongly connected components are such as from every node every other in the component is reachable.
 * Differs from weakly connected components in directed graphs.
 */
public class StronglyConnectedComponents {

    private final Graphulo g;
    private int counter;

    public StronglyConnectedComponents(Graphulo graphulo) {
        g = graphulo;
        counter = 0;
    }

    public void calculateStronglyConnectedComponents(String ATable, String RTable) throws TableNotFoundException {
        counter = 1;
        String oldTable;
        String degTable;
        String newTable = ATable;
        do {
            oldTable = newTable;
            degTable = oldTable + "_deg";
            newTable = ATable + counter;
            g.generateDegreeTable(oldTable, degTable, false);
            g.AdjBFS(oldTable, null, 1, newTable, null, degTable, null,
                    false, 0, Integer.MAX_VALUE);
            DebugUtil.printTable("Iteration " + counter, g.getConnector(), newTable, 5);
            counter++;
        } while (g.countEntries(oldTable) != (g.countEntries(newTable)));
    }
}
