package de.alkern.connected_components.analysis;

import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * Collection for Jaccard alike nodes. Saves the similarity and the nodes
 */
public class JaccardAlikes {

    private final static double ALLOWED_DELTA = 0.05d;

    private final String node1;
    private final String node2;
    private final double similarity;

    private final Set<String> neighbours1;
    private final Set<String> neighbours2;

    private Set<String> sharedNeighbours;
    private Set<String> uniqueNeighbours1;
    private Set<String> uniqueNeighbours2;

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

    public void calculate() {
        sharedNeighbours = new TreeSet<>(neighbours1);
        uniqueNeighbours1 = new TreeSet<>(neighbours1);
        uniqueNeighbours2 = new TreeSet<>(neighbours2);
        sharedNeighbours.retainAll(neighbours2);
        uniqueNeighbours1.removeAll(neighbours2);
        uniqueNeighbours2.removeAll(neighbours1);

        Set<String> allNeighbours = new TreeSet<>(neighbours1);
        allNeighbours.addAll(neighbours2);

        double sim = ((double) sharedNeighbours.size())/(allNeighbours.size());
        double delta = Math.abs(sim - similarity) / Math.max(Math.abs(sim), Math.abs(similarity));
        if (delta > ALLOWED_DELTA) {
            throw new RuntimeException("Entries in sets don't fit the given similarity");
        }
    }

    @Override
    public String toString() {
        if (sharedNeighbours == null) {
            return "Similarity of " + node1 + " and " + node2 + " not calculated yet";
        }
        StringBuilder builder = new StringBuilder();
        builder.append(node1);
        builder.append(" and ");
        builder.append(node2);
        builder.append(" with similarity ");
        builder.append(similarity);

        builder.append("\n-----------Shared Neighbours-------------\n");
        builder.append(sharedNeighbours);

        builder.append("\n-----------Unigue neighbours of ");
        builder.append(node1);
        builder.append("-------------\n");
        builder.append(uniqueNeighbours1);

        builder.append("\n-----------Unigue neighbours of ");
        builder.append(node2);
        builder.append("-------------\n");
        builder.append(uniqueNeighbours2);

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

    public Set<String> getSharedNeighbours() {
        return sharedNeighbours;
    }

    public Set<String> getUniqueNeighboursOf(String node) {
        if (node.equals(node1)) {
            return uniqueNeighbours1;
        }
        if (node.equals(node2)) {
            return uniqueNeighbours2;
        }
        System.err.println("Node " + node + " not in jaccard alike nodes");
        return Collections.emptySet();
    }
}
