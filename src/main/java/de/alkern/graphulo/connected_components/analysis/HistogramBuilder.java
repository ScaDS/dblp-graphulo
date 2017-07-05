package de.alkern.graphulo.connected_components.analysis;

import de.alkern.graphulo.connected_components.ComponentType;
import de.alkern.graphulo.connected_components.SizeType;
import edu.mit.ll.graphulo.Graphulo;
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

/**
 * Build Histograms from the cc-Tables in Accumulo
 */
public class HistogramBuilder {

    private final Graphulo g;
    private final Statistics s;

    public HistogramBuilder(Graphulo graphulo) {
        this.g = graphulo;
        this.s = new Statistics(g);
    }

    public void createAll(String table) {
        getChartsAsPNG(table, ComponentType.WEAK, SizeType.EDGES);
        getChartsAsPNG(table, ComponentType.WEAK, SizeType.NODES);
        getChartsAsPNG(table, ComponentType.STRONG, SizeType.EDGES);
        getChartsAsPNG(table, ComponentType.STRONG, SizeType.NODES);
    }

    public void getChartsAsPNG(String table, ComponentType type, SizeType sizeType) {
        getChartsAsPNG(table, type, sizeType, 800, 600);
    }

    public void getChartsAsPNG(String table, ComponentType type, SizeType sizeType, int width, int height) {
        double[] sizes = s.getComponentSizes(table, type, sizeType);
        int max = Collections.max(Arrays.asList(ArrayUtils.toObject(sizes))).intValue();
        HistogramDataset ds = new HistogramDataset();
        ds.setType(HistogramType.FREQUENCY);
        ds.addSeries("h", sizes, max); //@TODO if all sizes are equal, the histogram is just a thin line
        JFreeChart chart = ChartFactory.createHistogram(type.repr(), "Size", "#Components", ds,
                PlotOrientation.VERTICAL, false, false, false);
        try {
            String filename = table + "_" + type + "_" + sizeType;
            ChartUtilities.saveChartAsPNG(new File(filename + ".png"), chart, width, height);
        } catch (IOException e) {
            throw new RuntimeException("Could not save chart", e);
        }
    }
}
