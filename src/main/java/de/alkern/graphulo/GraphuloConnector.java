package de.alkern.graphulo;

import edu.mit.ll.graphulo.Graphulo;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;

public abstract class GraphuloConnector {

    public static Graphulo local(Connector conn) {
        return new Graphulo(conn, new PasswordToken("acc"));
    }

}
