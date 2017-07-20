package de.alkern.connected_components.analysis;

import org.junit.Test;

import static org.junit.Assert.*;

public class JaccardAlikesTest {

    @Test
    public void testAlikeNodes() {
        JaccardAlikes a = new JaccardAlikes("NODE1", "NODE2", 1);
        a.addNeighbour("NODE1", "NODE3");
        a.addNeighbour("NODE2", "NODE3");
        a.calculate();
        assertTrue(a.getSharedNeighbours().contains("NODE3"));
        assertEquals(1, a.getSharedNeighbours().size());
        assertEquals(0, a.getUniqueNeighboursOf("NODE1").size());
        assertEquals(0, a.getUniqueNeighboursOf("NODE2").size());
    }

    @Test
    public void testErrorAtWrongSimilarities() {
        JaccardAlikes a = new JaccardAlikes("NODE1", "NODE2", 1);
        a.addNeighbour("NODE1", "NODE3");
        a.addNeighbour("NODE1", "NODE4");
        a.addNeighbour("NODE2", "NODE4");
        try {
            a.calculate();
            fail("Should throw exception, because the given similarity does not fit the calculated");
        } catch (RuntimeException e) {
            assertEquals("Entries in sets don't fit the given similarity", e.getMessage());
        }
    }

    @Test
    public void testDeltaAtSimilarityCalculation() {
        JaccardAlikes a = new JaccardAlikes("NODE1", "NODE2", 0.1d);
        a.addNeighbour("NODE1", "NODE3");
        a.addNeighbour("NODE1", "NODE4");
        a.addNeighbour("NODE1", "NODE5");
        a.addNeighbour("NODE1", "NODE6");
        a.addNeighbour("NODE1", "NODE7");
        a.addNeighbour("NODE1", "NODE8");
        a.addNeighbour("NODE1", "NODE9");
        a.addNeighbour("NODE1", "NODE10");
        a.addNeighbour("NODE1", "NODE11");
        a.addNeighbour("NODE1", "NODE12");

        a.addNeighbour("NODE2", "NODE12");
        a.calculate();
    }
}