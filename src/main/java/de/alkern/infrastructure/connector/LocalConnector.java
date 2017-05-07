package de.alkern.infrastructure.connector;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;

public class LocalConnector extends AccumuloConnector {

    @Override
    public Connector get() throws AccumuloSecurityException, AccumuloException {
        return getConnector("bdp", "localhost:2181", "root", "acc");
    }

}
