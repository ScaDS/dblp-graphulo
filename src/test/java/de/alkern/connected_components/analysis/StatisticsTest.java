package de.alkern.connected_components.analysis;

import de.alkern.connected_components.ComponentType;
import de.alkern.connected_components.TestUtils;
import de.alkern.connected_components.data.VisitedNodesList;
import de.alkern.connected_components.strong.StronglyConnectedComponents;
import de.alkern.connected_components.weak.WeaklyConnectedComponents;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class StatisticsTest {

    private static final String TABLE = "STATS";

    @BeforeClass
    public static void init() {
        TestUtils.createExampleMatrix(TABLE);
    }

    @AfterClass
    public static void cleanup() {
        TestUtils.deleteTable(TABLE);
        TestUtils.deleteTable(TABLE + "_wcc1");
        TestUtils.deleteTable(TABLE + "_scc1");
        TestUtils.deleteTable(TABLE + "_scc2");
        TestUtils.deleteTable(TABLE + "_scc3");
        TestUtils.deleteTable(TABLE + "_scc4");
        TestUtils.deleteTable(Statistics.metatable(TABLE));
    }

    @Test
    public void buildMetadataTable() throws Exception {
        new WeaklyConnectedComponents(TestUtils.graphulo, new VisitedNodesList()).calculateConnectedComponents(TABLE);
        new StronglyConnectedComponents(TestUtils.graphulo).calculateConnectedComponents(TABLE);
        Statistics stats = new Statistics(TestUtils.graphulo);
        stats.buildMetadataTable(TABLE);

        assertEquals(1, stats.getNumberOfComponents(TABLE, ComponentType.WEAK));
        assertEquals(4, stats.getNumberOfComponents(TABLE, ComponentType.STRONG));

        assertEquals(3, stats.getNumberOfEdges(TABLE, ComponentType.STRONG, 1));
        assertEquals(3, stats.getNumberOfNodes(TABLE, ComponentType.STRONG, 1));
        assertEquals(2, stats.getNumberOfEdges(TABLE, ComponentType.STRONG, 2));
        assertEquals(2, stats.getNumberOfNodes(TABLE, ComponentType.STRONG, 2));
        assertEquals(2, stats.getNumberOfEdges(TABLE, ComponentType.STRONG, 3));
        assertEquals(2, stats.getNumberOfNodes(TABLE, ComponentType.STRONG, 3));
        assertEquals(1, stats.getNumberOfEdges(TABLE, ComponentType.STRONG, 4));
        assertEquals(1, stats.getNumberOfNodes(TABLE, ComponentType.STRONG, 4));
        assertEquals(14, stats.getNumberOfEdges(TABLE, ComponentType.WEAK, 1));
        assertEquals(8, stats.getNumberOfNodes(TABLE, ComponentType.WEAK, 1));
    }

}