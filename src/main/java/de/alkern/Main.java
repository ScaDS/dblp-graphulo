package de.alkern;

import de.alkern.author.AuthorProcessor;
import de.alkern.author.AuthorRepository;
import de.alkern.infrastructure.AccumuloRepository;

public class Main {

    public static void main(String[] args) {
        AccumuloRepository repo = new AuthorRepository();
        AuthorProcessor processor = new AuthorProcessor(repo, 10);
    }
}
