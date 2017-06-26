package de.alkern.graphulo.connected_components.weak;

import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.hadoop.io.Text;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Data structure to hold the single connected components and identify subparts.
 * For the graph c -> b -> a the algorithm would give three components (a, ba and cba). This class only saves
 */
public class Components {

    private Map<String, String> components;

    public Components() {
        this.components = new HashMap<>();
    }

    /**
     * Add a new component to the collection.
     * If a smaller version of the component is already in it, it will be overwritten by the bigger one.
     * @param key
     * @param value
     */
    public void put(String key, String value) {
        List<String> parts = GraphuloUtil.d4mRowToTexts(value).stream().map(Text::toString).collect(Collectors.toList());
        for (String part : parts) {
            if (this.components.keySet().contains(part)) {
                this.components.remove(part);
                break;
            }
        }
        this.components.put(key, value);
    }

    public void clear() {
        this.components.clear();
    }

    public Set<Map.Entry<String, String>> entrySet() {
        return this.components.entrySet();
    }

    public long size() {
        return this.components.size();
    }
}
