package de.alkern.author;

import de.alkern.infrastructure.AccumuloRepository;
import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Process the DBLP-data to extract authors and relations between them.
 * Authors are related, if they worked together on at least one DblpElement
 */
public class AuthorProcessor implements DblpElementProcessor {

    private AccumuloRepository repo;
    private int size;
    private int count;

    public AuthorProcessor(AccumuloRepository repo, int size) {
        this.repo = repo;
        this.size = size;
        this.count = 0;
    }

    @Override
    public void process(DblpElement element) throws SAXException {
        size++;
        Collection<String> authors = element.attributes.get("author");
        for (String author1: authors) {
            for (String author2: authors) {
                if (!author1.equals(author2)) {
                    repo.save(author1, author2, 1L);
                }
            }
        }

        if(count >= size) {
            throw new ParsingTerminationException();
        }
    }

    public void clear() {
        repo.clear();
    }

    public List<String> scan() {
        return repo.scan();
    }
}
