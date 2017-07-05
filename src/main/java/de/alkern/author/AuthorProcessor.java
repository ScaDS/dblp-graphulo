package de.alkern.author;

import de.alkern.infrastructure.entry.AdjacencyEntry;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.GraphuloProcessor;
import org.dblp.datastructures.DblpElement;

import java.util.Collection;

/**
 * Process the DBLP-data to extract authors and relations between them.
 * Authors are related, if they worked together on at least one DblpElement
 */
public class AuthorProcessor extends GraphuloProcessor {

    public AuthorProcessor(Repository repo) {
        super(repo);
    }
    public AuthorProcessor(Repository repo, int size) {
        super(repo, size);
    }

    @Override
    protected void processLogic(DblpElement element) {
        Collection<String> authors = element.attributes.get("author");
        for (String author1 : authors) {
            for (String author2 : authors) {
                repo.save(new AdjacencyEntry(author1, author2, "1"));
            }
        }
    }

}
