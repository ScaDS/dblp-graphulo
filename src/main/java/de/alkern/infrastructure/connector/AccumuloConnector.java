package de.alkern.infrastructure.connector;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public abstract class AccumuloConnector {
    public abstract Connector get() throws AccumuloSecurityException, AccumuloException;

    static Connector getConnector(String instanceName, String zookeepers, String user, String password)
            throws AccumuloSecurityException, AccumuloException {
        Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        return instance.getConnector(user, new PasswordToken(password));
    }
}
