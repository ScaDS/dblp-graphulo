package de.alkern.author;

import de.alkern.infrastructure.AccumuloRepository;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.List;

/**
 * Repository to save author-author-relations as adjacence-matrix
 */
public class AuthorRepository implements AccumuloRepository {

    @Override
    public void save(String row, String qualifier, long value) {
        throw new NotImplementedException();
    }

    @Override
    public List scan() {
        return null;
    }

    @Override
    public void clear() {

    }
}
