package de.alkern.connected_components.weak;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ComponentsTest {

    @Test
    public void putInOrder() throws Exception {
        Components c = new Components();
        c.put("ROW1", "ROW2;RO3;");
        c.put("ROW4", "ROW5");
        assertEquals(2L, c.size());
    }

    @Test
    public void putInLargerComponent() {
        Components c = new Components();
        c.put("ROW1", "ROW2;RO3;");
        c.put("ROW4", "ROW1;ROW2;ROW3;");
        assertEquals(1L, c.size());
    }

    @Test
    public void putInMultipleComponents() {
        Components c = new Components();
        c.put("ROW1", "ROW2;RO3;");
        c.put("ROW4", "ROW1;ROW2;ROW3;");
        c.put("A", "B;");
        c.put("ROW7", "ROW3;ROW2;ROW1;ROW4;");
        assertEquals(2L, c.size());
    }

}