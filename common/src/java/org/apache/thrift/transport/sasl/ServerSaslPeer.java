/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.thrift.transport.sasl;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.Sasl;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import static org.apache.thrift.transport.sasl.TSaslNegotiationException.ErrorType.AUTHENTICATION_FAILURE;
import static org.apache.thrift.transport.sasl.TSaslNegotiationException.ErrorType.MECHANISME_MISMATCH;
import static org.apache.thrift.transport.sasl.TSaslNegotiationException.ErrorType.PROTOCOL_ERROR;

/**
 * Server side sasl state machine.
 */
public class ServerSaslPeer implements SaslPeer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSaslPeer.class);

  private static final String QOP_AUTH_INT = "auth-int";
  private static final String QOP_AUTH_CONF = "auth-conf";

  private final Map<String, TSaslServerDefinition> saslMechanisms;
  private SaslServer saslServer;
  private SaslReader<SaslNegotiationHeader> startMessage;

  public ServerSaslPeer(TTransport transport, Map<String, TSaslServerDefinition> saslMechanisms) {
    this.saslMechanisms = saslMechanisms;
    startMessage = new NegotiationSaslReader(transport);
  }

  @Override
  public void initialize() throws TTransportException {
    if (!startMessage.isComplete()) {
      startMessage.read();
    }
    if (startMessage.isComplete()) {
      SaslNegotiationHeader startHeader = startMessage.getHeader();
      if (startHeader.getStatus() != NegotiationStatus.START) {
        throw new TInvalidSaslFrameException("Expecting START status but got " + startHeader.getStatus());
      }
      byte[] payload = startMessage.getPayload();
      String mechanism = new String(payload, StandardCharsets.UTF_8);
      if (!saslMechanisms.containsKey(mechanism)) {
        throw new TSaslNegotiationException(MECHANISME_MISMATCH, "Unsupported mechanism " + mechanism);
      }
      TSaslServerDefinition saslDef = saslMechanisms.get(mechanism);
      try {
        saslServer = Sasl.createSaslServer(saslDef.mechanism, saslDef.protocol, saslDef.serverName,
            saslDef.props, saslDef.cbh);
      } catch (SaslException e) {
        throw new TSaslNegotiationException(PROTOCOL_ERROR, "Failed to create sasl server " + mechanism, e);
      }
    }
  }

  @Override
  public boolean isInitialized() {
    return saslServer != null;
  }

  @Override
  public byte[] evaluate(byte[] negotiationMessage) throws TSaslNegotiationException {
    try {
      return saslServer.evaluateResponse(negotiationMessage);
    } catch (SaslException e) {
      throw new TSaslNegotiationException(AUTHENTICATION_FAILURE,
          "Authentication failed with " + saslServer.getMechanismName(), e);
    }
  }

  @Override
  public boolean isAuthenticated() {
    return saslServer.isComplete();
  }

  @Override
  public boolean isDataProtected() {
    Object qop = saslServer.getNegotiatedProperty(Sasl.QOP);
    if (qop == null) {
      return false;
    }
    String[] words = qop.toString().split("\\s*,\\s*");
    for (String word : words) {
      if (QOP_AUTH_INT.equalsIgnoreCase(word) || QOP_AUTH_CONF.equalsIgnoreCase(word)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public byte[] wrap(byte[] data, int offset, int length) throws TTransportException {
    try {
      return saslServer.wrap(data, offset, length);
    } catch (SaslException e) {
      throw new TTransportException("Failed to wrap data", e);
    }
  }

  @Override
  public byte[] wrap(byte[] data) throws TTransportException {
    return wrap(data, 0, data.length);
  }

  @Override
  public byte[] unwrap(byte[] data, int offset, int length) throws TTransportException {
    try {
      return saslServer.unwrap(data, offset, length);
    } catch (SaslException e) {
      throw new TTransportException("Failed to unwrap data", e);
    }
  }

  @Override
  public byte[] unwrap(byte[] data) throws TTransportException {
    return unwrap(data, 0, data.length);
  }

  @Override
  public void dispose() {
    if (saslServer != null) {
      try {
        saslServer.dispose();
      } catch (Exception e) {
        LOGGER.warn("Failed to close sasl server " + saslServer.getMechanismName(), e);
      }
    }
  }

  SaslServer getSaslServer() {
    return saslServer;
  }
}
