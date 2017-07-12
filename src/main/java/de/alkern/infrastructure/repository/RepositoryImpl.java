package de.alkern.infrastructure.repository;

import de.alkern.infrastructure.entry.AccumuloEntry;
import de.alkern.infrastructure.entry.AdjacencyEntry;
import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RepositoryImpl implements Repository {

    private final String tableName;
    private final TableOperations operations;
    private final BatchWriter writer;
    private final Scanner scanner;
    private final AccumuloEntry.Builder entryBuilder;

    public RepositoryImpl(String tableName, Connector connector, AccumuloEntry.Builder entryBuilder)
            throws TableExistsException, AccumuloSecurityException, AccumuloException, TableNotFoundException {
        super();
        this.tableName = tableName;
        operations = connector.tableOperations();
        this.entryBuilder = entryBuilder;
        createTable();
        BatchWriterConfig config = new BatchWriterConfig();
        config.setMaxMemory(5000L);
        writer = connector.createBatchWriter(tableName, config);
        scanner = connector.createScanner(tableName, new Authorizations());
    }

    private void createTable() throws AccumuloException, AccumuloSecurityException, TableExistsException {
        if (!operations.exists(tableName)) {
            operations.create(tableName);
        }
    }

    @Override
    public void save(AccumuloEntry entry) {
        try {
            Mutation mutation = entry.toMutation();
            writer.addMutation(mutation);
        } catch (AccumuloException e) {
            throw new RuntimeException("Could not save", e);
        }
    }

    @Override
    public List<AccumuloEntry> scan() {
        List<AccumuloEntry> entries = new LinkedList<>();
        for (Map.Entry<Key, Value> entry: scanner) {
            entries.add(entryBuilder.fromMapEntry(entry));
        }
        return entries;
    }

    @Override
    public Iterator<Map.Entry<Key, Value>> getIterator() {
        return scanner.iterator();
    }

    @Override
    public void clear() {
        try {
            if (operations.exists(tableName)) {
                operations.delete(tableName);
            }
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            throw new RuntimeException("Could not delete table " + tableName, e);
        }
    }

    @Override
    public void close() {
        try {
            writer.close();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException("Could not close writer", e);
        }
    }

    @Override
    public void flush() {
        try {
            writer.flush();
        } catch (MutationsRejectedException e) {
            throw new RuntimeException("Could not flush", e);
        }
    }
}
