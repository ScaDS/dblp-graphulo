package de.alkern.graphulo.connected_components;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BFSTest {

    private static final String TABLE = "BFSTEST";

    @BeforeClass
    public static void init() {
        TestUtils.createExampleMatrix(TABLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(TABLE);
    }

    @Test
    public void testNeighbourhoodChangesWithK() {
        Map<Integer, String> results = new HashMap<>();
        for (Integer i = 1; i <= 10; i++) {
            String neighbours = TestUtils.graphulo.AdjBFS(TABLE, "ROW1;", i, null,
                    null, null, null, false, 0, Integer.MAX_VALUE);
            results.put(i, neighbours);
        }
        assertEquals("ROW2;", results.get(1));
        assertEquals("ROW6;ROW5;ROW3;", results.get(2));
        assertEquals("ROW1;ROW6;ROW7;ROW4;", results.get(3));
        assertEquals("ROW8;ROW6;ROW7;ROW2;ROW3;", results.get(4));
        assertEquals("ROW8;ROW6;ROW7;ROW4;ROW5;ROW3;", results.get(5));
        assertEquals("ROW1;ROW8;ROW6;ROW7;ROW4;ROW3;", results.get(6));
        assertEquals("ROW8;ROW6;ROW7;ROW4;ROW2;ROW3;", results.get(7));
        assertEquals("ROW8;ROW6;ROW7;ROW4;ROW5;ROW3;", results.get(8));
        assertEquals("ROW1;ROW8;ROW6;ROW7;ROW4;ROW3;", results.get(9));
        assertEquals("ROW8;ROW6;ROW7;ROW4;ROW2;ROW3;", results.get(10));
    }

    @Test
    public void testNeighbourhoodGrowsWithKAsOutputUnion() {
        Map<Integer, String> results = new HashMap<>();
        for (Integer i = 1; i <= 6; i++) {
            String neighbours = TestUtils.graphulo.AdjBFS(TABLE, "ROW1;", i, null,
                    null, null, 1, null, null, false,
                    0, Integer.MAX_VALUE, null, null, null,
                    true, null);
            results.put(i, neighbours);
        }
        assertEquals("ROW2;", results.get(1));
        assertEquals("ROW6;ROW5;ROW2;ROW3;", results.get(2));
        assertEquals("ROW1;ROW6;ROW7;ROW4;ROW5;ROW2;ROW3;", results.get(3));
        assertEquals("ROW1;ROW8;ROW6;ROW7;ROW4;ROW5;ROW2;ROW3;", results.get(4));
        assertEquals("ROW1;ROW8;ROW6;ROW7;ROW4;ROW5;ROW2;ROW3;", results.get(5));
        assertEquals("ROW1;ROW8;ROW6;ROW7;ROW4;ROW5;ROW2;ROW3;", results.get(6));
    }
}
