package de.alkern.graphulo.connected_components.analysis;

import de.alkern.graphulo.connected_components.ComponentType;
import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.commons.lang.ArrayUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.data.statistics.HistogramDataset;
import org.jfree.data.statistics.HistogramType;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Build Histograms from the cc-Tables in Accumulo
 */
public class HistogramBuilder {

    private final Graphulo g;

    public HistogramBuilder(Graphulo graphulo) {
        this.g = graphulo;
    }

    public void getChartsAsPNG(String table, ComponentType type, String filename) {
        getChartsAsPNG(table, type, filename, 800, 600);
    }

    public void getChartsAsPNG(String table, ComponentType type, String filename, int width, int height) {
        double[] sizes = getComponentSizes(table, type);
        int max = Collections.max(Arrays.asList(ArrayUtils.toObject(sizes))).intValue();
        HistogramDataset ds = new HistogramDataset();
        ds.setType(HistogramType.FREQUENCY);
        ds.addSeries("h", sizes, max);
        JFreeChart chart = ChartFactory.createHistogram(type.repr(), "Size", "#Components", ds,
                PlotOrientation.VERTICAL, false, false, false);
        try {
            ChartUtilities.saveChartAsPNG(new File(filename + ".png"), chart, width, height);
        } catch (IOException e) {
            throw new RuntimeException("Could not save chart", e);
        }
    }

    private double[] getComponentSizes(String table, ComponentType type) {
        List<Long> sizes = new LinkedList<>();
        TableOperations tops = g.getConnector().tableOperations();
        long counter = 1;
        String t = table + type + counter++;
        while (tops.exists(t)) {
            sizes.add(g.countRows(t));
            t = table + type + counter++;
        }
        double[] result = new double[sizes.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = sizes.get(i).doubleValue();
        }
        return result;
    }
}
