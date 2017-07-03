package de.alkern.graphulo.connected_components;

public enum ComponentType {
    WEAK("_wcc", "Weakly Connected Components"),
    STRONG("_scc", "Strongly Connected Components");

    private final String name;
    private final String repr;

    ComponentType(String name, String repr) {
        this.name = name;
        this.repr = repr;
    }

    @Override
    public String toString() {
        return name;
    }

    public String repr() {
        return repr;
    }
}
