package de.alkern.infrastructure.connector;

import org.apache.accumulo.core.client.*;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public abstract class AccumuloConnector {

    static Connector getConnector(String instanceName, String zookeepers, String user, String password)
            throws AccumuloSecurityException, AccumuloException {
        Instance instance = new ZooKeeperInstance(instanceName, zookeepers);
        return instance.getConnector(user, new PasswordToken(password));
    }

    public static Connector local() throws AccumuloSecurityException, AccumuloException {
        return getConnector("bdp", "localhost:2181", "root", "acc");
    }
}
