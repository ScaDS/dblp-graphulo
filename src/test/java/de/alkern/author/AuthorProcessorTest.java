package de.alkern.author;

import de.alkern.infrastructure.AccumuloRepository;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.MockRepository;
import org.dblp.parser.DblpParser;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class AuthorProcessorTest {

    private static AuthorProcessor authorProcessor;

    @BeforeClass
    public static void init() {
        AccumuloRepository repo = new MockRepository();
        authorProcessor = new AuthorProcessor(repo, 10);
    }

    @After
    public void setup() {
        authorProcessor.clear();
    }

    @Test
    public void testProcess() {
        DblpParser.load(authorProcessor, ExampleData.getExampleData());
        List<String> relations = authorProcessor.scan();
        assertEquals("E. F. Codd :C. J. Date   -> 1", relations.get(0));
        assertEquals("C. J. Date :E. F. Codd   -> 1", relations.get(1));
    }

}