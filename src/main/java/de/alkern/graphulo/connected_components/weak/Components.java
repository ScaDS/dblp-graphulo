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
 * For the graph c -> b -> a the algorithm would give three components (a, ba and cba).
 */
public class Components {

    private Map<String, String> components;

    public Components() {
        this.components = new HashMap<>();
    }

    public void put(String key, String value) {
        List<String> parts = GraphuloUtil.d4mRowToTexts(value).stream().map(Text::toString).collect(Collectors.toList());
        for (String part : parts) {
            if (this.components.keySet().contains(part)) {
                return;
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
}
