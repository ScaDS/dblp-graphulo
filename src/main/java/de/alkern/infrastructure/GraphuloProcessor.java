package de.alkern.infrastructure;

import de.alkern.infrastructure.entry.AccumuloEntry;
import de.alkern.infrastructure.repository.Repository;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.DblpParser;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class GraphuloProcessor implements DblpElementProcessor {

    protected Repository repo;
    private int size;
    private int counter;
    private boolean doesCount;

    /**
     * Processor which processes all given entries
     * @param repo DB-Repository
     */
    public GraphuloProcessor(Repository repo) {
        this.repo = repo;
        this.size = 0;
        this.counter = 0;
        this.doesCount = false;
    }

    /**
     * Processor which processes a certain number of entries
     * @param repo DB-Repository
     * @param size number of entries to process
     */
    public GraphuloProcessor(Repository repo, int size) {
        this.repo = repo;
        this.size = size;
        this.counter = 0;
        this.doesCount = true;
    }

    @Override
    public void process(DblpElement element) throws SAXException {
        processLogic(element);
        incrementCounter();
    }

    protected abstract void processLogic(DblpElement element);

    private void incrementCounter() throws ParsingTerminationException {
        if (!doesCount) return;
        counter++;
        if (counter >= size) {
            repo.close();
            throw new ParsingTerminationException();
        }
    }

    public void clear() {
        repo.clear();
    }

    public List<AccumuloEntry> scan() {
        return repo.scan();
    }

    public Iterator<Map.Entry<Key, Value>> getIterator() {
        return repo.getIterator();
    }

    public List<AccumuloEntry> parse(String file) {
        DblpParser.load(this, file);
        return scan();
    }
}
