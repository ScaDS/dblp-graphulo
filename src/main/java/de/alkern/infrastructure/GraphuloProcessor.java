package de.alkern.infrastructure;

import de.alkern.infrastructure.entry.AccumuloEntry;
import de.alkern.infrastructure.repository.Repository;
import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.DblpParser;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.List;

public abstract class GraphuloProcessor implements DblpElementProcessor {

    protected Repository repo;
    private int size;
    private int count;

    public GraphuloProcessor(Repository repo, int size) {
        this.repo = repo;
        this.size = size;
        this.count = 0;
    }

    @Override
    public void process(DblpElement element) throws SAXException {
        count++;
        processLogic(element);
        if (count >= size) {
            repo.close();
            throw new ParsingTerminationException();
        }
    }

    protected abstract void processLogic(DblpElement element);

    public void clear() {
        repo.clear();
    }

    public List<AccumuloEntry> scan() {
        return repo.scan();
    }

    public List<AccumuloEntry> parse(String file) {
        DblpParser.load(this, file);
        return scan();
    }
}
