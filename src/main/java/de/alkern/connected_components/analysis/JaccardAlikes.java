package de.alkern.connected_components.analysis;

import java.util.Set;
import java.util.TreeSet;

/**
 * Collection for Jaccard alike nodes. Saves the similarity and the nodes
 */
public class JaccardAlikes {

    private final String node1;
    private final String node2;
    private final double similarity;

    private final Set<String> neighbours1;
    private final Set<String> neighbours2;

    public JaccardAlikes(String node1, String node2, double similarity) {
        this.node1 = node1;
        this.node2 = node2;
        this.similarity = similarity;
        neighbours1 = new TreeSet<>();
        neighbours2 = new TreeSet<>();
    }

    public void addNeighbour(String node, String neighbour) {
//        if (neighbour.equals(node1) || neighbour.equals(node2)) {
//            return;
//        }
        if (node.equals(node1)) {
            neighbours1.add(neighbour);
            return;
        }
        if (node.equals(node2)) {
            neighbours2.add(neighbour);
            return;
        }
        System.err.println("Node " + node + " not in jaccard alike nodes");
    }

    @Override
    public String toString() {
        //prepare Sets for printing
        Set<String> intersectionCopy = new TreeSet<>(neighbours1);
        Set<String> difference1 = new TreeSet<>(neighbours1);
        Set<String> difference2 = new TreeSet<>(neighbours2);
        intersectionCopy.retainAll(neighbours2);
        difference1.removeAll(neighbours2);
        difference2.removeAll(neighbours1);

        StringBuilder builder = new StringBuilder();
        builder.append(node1);
        builder.append(" and ");
        builder.append(node2);
        builder.append(" with similarity ");
        builder.append(similarity);

        builder.append("\n-----------Shared Nodes-------------\n");
        builder.append(intersectionCopy);

        builder.append("\n-----------Unigue neighbours of ");
        builder.append(node1);
        builder.append("-------------\n");
        builder.append(difference1);

        builder.append("\n-----------Unigue neighbours of ");
        builder.append(node2);
        builder.append("-------------\n");
        builder.append(difference2);

        builder.append("\n");
        return builder.toString();
    }

    public String getNode1() {
        return node1;
    }

    public String getNode2() {
        return node2;
    }

    public double getSimilarity() {
        return similarity;
    }
}
