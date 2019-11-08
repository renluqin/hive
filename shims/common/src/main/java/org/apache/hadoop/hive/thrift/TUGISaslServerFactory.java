package org.apache.hadoop.hive.thrift;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.sasl.ServerSaslPeer;
import org.apache.thrift.transport.sasl.TSaslNegotiationException;
import org.apache.thrift.transport.sasl.TSaslServerFactory;

import java.security.PrivilegedExceptionAction;

import static org.apache.thrift.transport.sasl.TSaslNegotiationException.ErrorType.UNKNOWN;

public class TUGISaslServerFactory extends TSaslServerFactory {

    private final UserGroupInformation serverUGI;

    public TUGISaslServerFactory(UserGroupInformation serverUGI) {
        this.serverUGI = serverUGI;
    }

    @Override
    public ServerSaslPeer getSaslPeer(final String mechanism) throws TSaslNegotiationException {
        try {
            return serverUGI.doAs(new PrivilegedExceptionAction<ServerSaslPeer>() {
                @Override
                public ServerSaslPeer run() throws TSaslNegotiationException {
                    return TUGISaslServerFactory.super.getSaslPeer(mechanism);
                }
            });
        } catch (Throwable e) {
            Throwable cause = e.getCause();
            if (cause instanceof TSaslNegotiationException) {
                throw (TSaslNegotiationException) cause;
            }
            throw new TSaslNegotiationException(UNKNOWN, "Failed to create sasl server", e);
        }
    }
}
