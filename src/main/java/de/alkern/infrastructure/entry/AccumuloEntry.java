package de.alkern.infrastructure.entry;

import org.apache.accumulo.core.data.Mutation;

public interface AccumuloEntry {
    public Mutation toMutation();
}
