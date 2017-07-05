package de.alkern.graphulo.connected_components;

public enum SizeType {
    EDGES("edges"), NODES("nodes");

    private String name;

    SizeType(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}