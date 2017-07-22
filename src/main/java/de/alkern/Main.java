package de.alkern;

import de.alkern.author.AuthorProcessor;
import de.alkern.connected_components.ComponentType;
import de.alkern.connected_components.SizeType;
import de.alkern.connected_components.analysis.HistogramBuilder;
import de.alkern.connected_components.analysis.Statistics;
import de.alkern.connected_components.data.VisitedNodesList;
import de.alkern.connected_components.strong.StronglyConnectedComponents;
import de.alkern.connected_components.weak.WeaklyConnectedComponents;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.GraphuloProcessor;
import de.alkern.infrastructure.connector.GraphuloConnector;
import de.alkern.infrastructure.connector.AccumuloConnector;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.repository.RepositoryImpl;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final String TABLE = "authors";
    private static Map<String, Long> elapsedTimes;
    private static Graphulo graphulo;

    public static void main(String[] args)
            throws AccumuloSecurityException, AccumuloException, TableExistsException, TableNotFoundException, InterruptedException {
        elapsedTimes = new ConcurrentHashMap<>();
        Connector conn = AccumuloConnector.local();
        graphulo = GraphuloConnector.local(conn);
//        Repository repo = new RepositoryImpl(TABLE, conn, new AdjacencyEntry.AdjacencyBuilder());
//        GraphuloProcessor processor = new AuthorProcessor(repo, 0.0001d);
//
//        long startParsing = System.nanoTime();
//        processor.parse(ExampleData.DBLP);
//        long endParsing = System.nanoTime();
//        long parseTime = endParsing - startParsing;
//        elapsedTimes.put("Parsing", TimeUnit.NANOSECONDS.toSeconds(parseTime));
//        System.out.println("Parsing took " + parseTime + " ns");
//
//        Thread wc = new Thread(Main::weak);
//        Thread sc = new Thread(Main::strong);
//        wc.start();
//        sc.start();
//        wc.join();
//        sc.join();
//
        Statistics s = new Statistics(graphulo);
        s.printJaccardAlikes("authors", 0.75, 0.75);
//
//        long startStat = System.nanoTime();
//        s.buildMetadataTable(TABLE);
//        long endStat = System.nanoTime();
//        long statTime = endStat - startStat;
//        elapsedTimes.put("Statistics", TimeUnit.NANOSECONDS.toSeconds(statTime));
//        System.out.println(TimeUnit.NANOSECONDS.toSeconds(statTime));
//
//        for (Map.Entry<String, Long> entry : elapsedTimes.entrySet()) {
//            System.out.println(entry.getKey() + ": " + entry.getValue() + "s");
//        }
//
//        HistogramBuilder h = new HistogramBuilder(graphulo);
//        h.getChartsAsPNG(TABLE, ComponentType.WEAK, SizeType.EDGES, 1920, 1080);
//        h.getChartsAsPNG(TABLE, ComponentType.WEAK, SizeType.NODES, 1920, 1080);
//        h.getChartsAsPNG(TABLE, ComponentType.STRONG, SizeType.EDGES, 1920, 1080);
//        h.getChartsAsPNG(TABLE, ComponentType.STRONG, SizeType.NODES, 1920, 1080);
    }

    private static void weak() {
        WeaklyConnectedComponents wcc = new WeaklyConnectedComponents(graphulo, new VisitedNodesList());
        long startWcc = System.nanoTime();
        wcc.calculateConnectedComponents(TABLE);
        long endWcc = System.nanoTime();
        long wccTime = endWcc - startWcc;
        elapsedTimes.put("Weakly", TimeUnit.NANOSECONDS.toSeconds(wccTime));
        System.out.println(TimeUnit.NANOSECONDS.toSeconds(wccTime));
    }

    private static void strong() {
        StronglyConnectedComponents scc = new StronglyConnectedComponents(graphulo, new VisitedNodesList());
        long startScc = System.nanoTime();
        scc.calculateConnectedComponents(TABLE);
        long endScc = System.nanoTime();
        long sccTime = endScc - startScc;
        elapsedTimes.put("Strongly", TimeUnit.NANOSECONDS.toSeconds(sccTime));
        System.out.println(TimeUnit.NANOSECONDS.toSeconds(sccTime));
    }
}
