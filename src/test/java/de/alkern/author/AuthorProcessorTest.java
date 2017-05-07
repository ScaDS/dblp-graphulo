package de.alkern.author;

import de.alkern.infrastructure.Repository;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.MockRepository;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AuthorProcessorTest {

    private static AuthorProcessor authorProcessor;

    @BeforeClass
    public static void init() {
        Repository repo = new MockRepository();
        authorProcessor = new AuthorProcessor(repo, 10);
    }

    @After
    public void setup() {
        authorProcessor.clear();
    }

    @Test
    public void testProcess() {
        List relations = authorProcessor.parse(ExampleData.EXAMPLE_DATA);
        assertEquals("E. F. Codd :C. J. Date   -> 1", relations.get(0));
        assertEquals("C. J. Date :E. F. Codd   -> 1", relations.get(1));
    }

}