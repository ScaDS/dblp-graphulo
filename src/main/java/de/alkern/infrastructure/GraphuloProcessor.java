package de.alkern.infrastructure;

import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.DblpParser;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.List;

public abstract class GraphuloProcessor implements DblpElementProcessor {

    protected AccumuloRepository repo;
    private int size;
    private int count;

    public GraphuloProcessor(AccumuloRepository repo, int size) {
        this.repo = repo;
        this.size = size;
        this.count = 0;
    }

    @Override
    public void process(DblpElement element) throws SAXException {
        size++;
        processLogic(element);
        if (count >= size) {
            throw new ParsingTerminationException();
        }
    }

    protected abstract void processLogic(DblpElement element);

    public void clear() {
        repo.clear();
    }

    public List scan() {
        return repo.scan();
    }

    public List parse(String file) {
        DblpParser.load(this, ExampleData.EXAMPLE_DATA);
        return scan();
    }
}
