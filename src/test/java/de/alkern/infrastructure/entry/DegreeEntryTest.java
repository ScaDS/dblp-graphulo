package de.alkern.infrastructure.entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class DegreeEntryTest {

    @Test
    public void fromEntry() throws Exception {
        Map<Key, Value> map = new HashMap<>();
        Key key = new Key(new Text("1"), new Text(""), new Text("IN"), new Text(""), 0L);
        Value value = new Value("1084");
        map.put(key, value);
        DegreeEntry e = null;
        for (Map.Entry<Key, Value> entry: map.entrySet()) {
            e = new DegreeEntry.DegreeBuilder().fromMapEntry(entry);
        }
        assertEquals(e.getNode(), "1");
        assertEquals(e.getLabel(), DegreeEntry.DegreeLabel.IN);
        assertEquals(e.getDegrees(), "1084");
    }

}