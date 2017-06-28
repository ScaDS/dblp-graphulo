package de.alkern.graphulo.connected_components;

import edu.mit.ll.graphulo.Graphulo;

public class ConnectedComponentsUtils {

    public static String getAllNeighbours(Graphulo g, String table, String row) {
        int counter = 1;
        //increment number of steps until all neighbours are found and the neighbourhood string does not change anymore
        String oldNeighbours;
        String newNeighbours = "";
        do {
            oldNeighbours = newNeighbours;
            newNeighbours = g.AdjBFS(table, row + ";", counter++, null, null, null,
                    4, null, null, false, 0,
                    Integer.MAX_VALUE, null, null, null, true,
                    null);
        } while (!oldNeighbours.equals(newNeighbours));
        return newNeighbours;
    }
}
