package de.alkern.author;

import de.alkern.infrastructure.entry.AccumuloEntry;
import de.alkern.infrastructure.repository.Repository;
import de.alkern.infrastructure.ExampleData;
import de.alkern.infrastructure.repository.MockRepository;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        List<String> relations = authorProcessor.parse(ExampleData.EXAMPLE_DATA)
                .stream().map(AccumuloEntry::toString).collect(Collectors.toList());
        assertTrue(relations.contains("Michael Ley :Michael Ley []   -> 1"));
        assertTrue(relations.contains("E. F. Codd :C. J. Date []   -> 1"));
        assertTrue(relations.contains("C. J. Date :E. F. Codd []   -> 1"));
        assertEquals(13, relations.size());
    }

    @Test
    public void testErrorWithShinnosuke() {
        List<AccumuloEntry> relations = authorProcessor.parse(ExampleData.SHORT_EXAMPLE);
        assertEquals(9, relations.size());
    }

}