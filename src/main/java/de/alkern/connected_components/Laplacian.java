package de.alkern.connected_components;

import edu.mit.ll.graphulo.DynamicIteratorSetting;
import edu.mit.ll.graphulo.Graphulo;
import edu.mit.ll.graphulo.simplemult.MathTwoScalar;
import edu.mit.ll.graphulo.skvi.RemoteWriteIterator;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Collections;
import java.util.Map;

@Deprecated
public class Laplacian {

    private final Graphulo graphulo;

    public Laplacian(Graphulo graphulo) {
        this.graphulo = graphulo;
    }

    /**
     * Calculates the Laplacian Matrix L = degreeTable - adjTable
     * @param adjTable
     * @param degreeTable
     * @param resultTable
     */
    public void calculateLaplacian(String adjTable, String degreeTable, String resultTable) {
        //Create new resultTable
        TableOperations tops = graphulo.getConnector().tableOperations();
        try {
            if (tops.exists(resultTable))
                tops.delete(resultTable);
            tops.create(resultTable);
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException | TableExistsException e) {
            throw new RuntimeException(e);
        }

        //scan degreeTable and fix degreeEntries
        BatchScanner degreeTableScanner;
        try {
            degreeTableScanner = graphulo.getConnector().createBatchScanner(degreeTable, Authorizations.EMPTY, 15);
            degreeTableScanner.setRanges(Collections.singleton(new Range()));
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        DynamicIteratorSetting degreeItSet = new DynamicIteratorSetting(1, "copyTables");
        degreeItSet.append(LaplacianPrepareOp.iteratorSetting(3));
        degreeItSet.append(new IteratorSetting(10, RemoteWriteIterator.class,
                graphulo.basicRemoteOpts("", resultTable, null, null)));
        degreeItSet.addToScanner(degreeTableScanner);
        for (Map.Entry<Key, Value> entry : degreeTableScanner) {}
        degreeTableScanner.close();

        //scan AdjacencyTable and set all values to -1
        BatchScanner adjTableScanner;
        try {
            adjTableScanner = graphulo.getConnector().createBatchScanner(adjTable, Authorizations.EMPTY, 15);
            adjTableScanner.setRanges(Collections.singleton(new Range()));
        } catch (TableNotFoundException e) {
            throw new RuntimeException(e);
        }
        DynamicIteratorSetting adjItSet = new DynamicIteratorSetting(1, "copyTables");
        adjItSet.append(MathTwoScalar.applyOpLong(5, false,
                MathTwoScalar.ScalarOp.TIMES, -1L, false));
        adjItSet.append(new IteratorSetting(10, RemoteWriteIterator.class,
                graphulo.basicRemoteOpts("", resultTable, null, null)));
        adjItSet.addToScanner(adjTableScanner);
        for (Map.Entry<Key, Value> entry : adjTableScanner) {}
        adjTableScanner.close();
    }
}
