package de.alkern.graphulo.connected_components.strong;

import de.alkern.graphulo.GraphuloConnector;
import de.alkern.graphulo.connected_components.TestUtils;
import de.alkern.infrastructure.connector.AccumuloConnector;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.util.DebugUtil;
import edu.mit.ll.graphulo.util.GraphuloUtil;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.*;

public class StronglyConnectedComponentsTest {

    private static final String STRONG_EXAMPLE = "strong_example";
    private static Connector conn;
    private static TableOperations tops;

    @BeforeClass
    public static void init() throws AccumuloSecurityException, AccumuloException, TableNotFoundException, TableExistsException {
        TestUtils.createExampleMatrix(STRONG_EXAMPLE);
        DebugUtil.printTable(STRONG_EXAMPLE, conn, STRONG_EXAMPLE, 5);
    }

    @Test
    public void test() throws TableNotFoundException {
        deleteIntermediateTables();
        StronglyConnectedComponents scc = new StronglyConnectedComponents(GraphuloConnector.local(conn));
        scc.calculateStronglyConnectedComponents(STRONG_EXAMPLE, "result");
    }

    private void deleteIntermediateTables() {
        int counter = 1;
        try {
//            tops.delete(STRONG_EXAMPLE);
            tops.delete(STRONG_EXAMPLE + "_deg");
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {

        }
        try {
            while (true) {
                tops.delete(STRONG_EXAMPLE + counter++);
            }
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {

        }
        counter = 1;
        try {
            while (true) {
                tops.delete(STRONG_EXAMPLE + counter++ + "_deg");
            }
        } catch (AccumuloSecurityException | TableNotFoundException | AccumuloException e) {

        }
    }
}