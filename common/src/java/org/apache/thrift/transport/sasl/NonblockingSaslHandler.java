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

import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.apache.thrift.transport.sasl.TSaslNegotiationException.ErrorType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.apache.thrift.transport.sasl.NegotiationStatus.COMPLETE;
import static org.apache.thrift.transport.sasl.NegotiationStatus.OK;

/**
 * State machine managing one sasl connection in a nonblocking way.
 */
public class NonblockingSaslHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(NonblockingSaslHandler.class);

  private static final int INTEREST_NONE = 0;
  private static final int INTEREST_READ = SelectionKey.OP_READ;
  private static final int INTEREST_WRITE = SelectionKey.OP_WRITE;

  // Tracking the current running phase
  private Phase currentPhase = Phase.INITIIALIIZING;
  // Tracking the next phase on the next invocation of the state machine. It is the same as current phase if current phase is not yet finished.
  // Otherwise, if it is different from current phase, it means current phase is done, and next phase is not yet started.
  private Phase nextPhase = currentPhase;

  private SelectionKey selectionKey;
  private TNonblockingTransport transport;
  private TTransportFactory inputTransportFactory;
  private TTransportFactory outputTransportFactory;
  private TProcessorFactory processorFactory;
  private TProtocolFactory requestProtocolFactory;
  private TProtocolFactory responseProtocolFactory;
  private TServerEventHandler eventHandler;
  private ServerContext serverContext;
  // It turns out the event handler implementation in hive just creates a null ServerContext.
  private boolean serverContextCreated = false;

  // State machine managing sasl server
  private ServerSaslPeer saslPeer;

  // Sasl negotiation io
  private NegotiationSaslReader saslResponse;
  private NegotiationSaslWriter saslChallenge;
  // IO for request from and response to the socket
  private DataFrameSaslReader nonblockingRequestReader;
  private DataFrameSaslWriter nonblockingResponseWriter;
  // Intermediate io used by TProcessor.
  private TMemorySaslServerTransport memoryTransport;
  // Wrapper around memoryTransport.
  private TTransport processingInputTransport;
  private TTransport processingOutputTransport;
  // Thrift protocols
  private TProtocol requestProtocol;
  private TProtocol responseProtocol;

  public NonblockingSaslHandler(SelectionKey selectionKey, TNonblockingTransport transport, TProcessorFactory processorFactory,
                                TTransportFactory inputTransportFactory, TTransportFactory outputTransportFactory,
                                TProtocolFactory requestProtocolFactory, TProtocolFactory responseProtocolFactory,
                                Map<String, TSaslServerDefinition> saslMechanisms, TServerEventHandler eventHandler) {
    this.selectionKey = selectionKey;
    this.transport = transport;
    this.processorFactory = processorFactory;
    this.inputTransportFactory = inputTransportFactory;
    this.outputTransportFactory = outputTransportFactory;
    this.requestProtocolFactory = requestProtocolFactory;
    this.responseProtocolFactory = responseProtocolFactory;
    this.eventHandler = eventHandler;

    saslPeer = new ServerSaslPeer(transport, saslMechanisms);
    saslResponse = new NegotiationSaslReader(transport);
    saslChallenge = new NegotiationSaslWriter(transport);
    nonblockingRequestReader = new DataFrameSaslReader(transport);
    nonblockingResponseWriter = new DataFrameSaslWriter(transport);
  }

  /**
   * Get current phase of the state machine.
   *
   * @return current phase.
   */
  public Phase getCurrentPhase() {
    return currentPhase;
  }

  /**
   * Get next phase of the state machine.
   * It is different from current phase iff current phase is done (and next phase not yet started).
   *
   * @return next phase.
   */
  public Phase getNextPhase() {
    return nextPhase;
  }

  /**
   *
   * @return true if current phase is done.
   */
  public boolean isCurrentPhaseDone() {
    return currentPhase != nextPhase;
  }

  /**
   * Run state machine.
   *
   * @throws IllegalStateException if current state is already done.
   */
  public void runCurrentPhase() {
    currentPhase.runStateMachine(this);
  }

  /**
   * When current phase is intrested in read selection, calling this will run the current phase and its following phases,
   * until there is nothing available in the underlying transport or the following phase has no interest in read (a pure
   * computation phase or a write phase).
   *
   * @throws IllegalStateException if is called in an irrelevant phase.
   */
  public void handleRead() {
    handleOps(INTEREST_READ);
  }

  /**
   * Similiar to handleRead. But it is for write ops.
   *
   * @throws IllegalStateException if it is called in an irrelevant phase.
   */
  public void handleWrite() {
    handleOps(INTEREST_WRITE);
  }

  private void handleOps(int interestOps) {
    if (currentPhase.selectionInterest != interestOps) {
      throw new IllegalStateException("Current phase " + currentPhase + " but got interest " + interestOps);
    }
    runCurrentPhase();
    if (isCurrentPhaseDone() && nextPhase.selectionInterest == interestOps) {
      stepToNextPhase();
      handleOps(interestOps);
    }
  }

  /**
   * When current phase is finished, it's expected to call this method first before running again state machine.
   * By calling this, "next phase" is marked as started (and not done), thus is ready to run.
   * Also, some state clean up (especially, io buffers clean up) will be done in this method to prepare for the run.
   *
   * @throws IllegalArgumentException if current phase is not yet done.
   */
  public void stepToNextPhase() {
    if (!isCurrentPhaseDone()) {
      throw new IllegalArgumentException("Not yet done with current phase: " + currentPhase);
    }
    LOGGER.debug("Switch phase {} to {}", currentPhase, nextPhase);
    switch (nextPhase) {
      case INITIIALIIZING:
        throw new IllegalStateException("INITIALIZING cannot be the next phase of " + currentPhase);
      case READING_SASL_RESPONSE:
        saslResponse.clear();
        break;
      case READING_REQUEST:
        nonblockingRequestReader.clear();
        break;
      default:
    }
    // If next phase's interest is not the same as current,  nor the same as the selection key,
    // we need to change interest on the selector.
    if (!(nextPhase.selectionInterest == currentPhase.selectionInterest || nextPhase.selectionInterest == selectionKey.interestOps())) {
      changeSelectionInterest(nextPhase.selectionInterest);
    }
    currentPhase = nextPhase;
  }

  private void changeSelectionInterest(int selectionInterest) {
    selectionKey.interestOps(selectionInterest);
  }

  // sasl negotiaion failure handling
  private void failSaslNegotiation(TSaslNegotiationException e) {
    LOGGER.error("Sasl negotiation failed", e);
    String errorMsg = e.getDetails();
    saslChallenge.withHeaderAndPayload(new byte[]{e.getErrorType().code.getValue()}, errorMsg.getBytes(StandardCharsets.UTF_8));
    nextPhase = Phase.WRITING_FAILURE_MESSAGE;
  }

  private void fail(Exception e) {
    LOGGER.error("Failed io in " + currentPhase, e);
    nextPhase = Phase.CLOSING;
  }

  private void failIO(TTransportException e) {
    StringBuilder errorMsg = new StringBuilder("IO failure ")
        .append(e.getType())
        .append(" in ")
        .append(currentPhase);
    if (e.getMessage() != null) {
      errorMsg.append(": ").append(e.getMessage());
    }
    LOGGER.error(errorMsg.toString(), e);
    nextPhase = Phase.CLOSING;
  }

  // Read handlings

  private void handleInitializing() {
    try {
      saslPeer.initialize();
      if (saslPeer.isInitialized()) {
        nextPhase = Phase.READING_SASL_RESPONSE;
      }
    } catch (TSaslNegotiationException e) {
      failSaslNegotiation(e);
    } catch (TTransportException e) {
      failIO(e);
    }
  }

  private void handleReadingSaslResponse() {
    if (!saslResponse.isComplete()) {
      try {
        saslResponse.read();
        if (saslResponse.isComplete()) {
          nextPhase = Phase.EVALUATING_SASL_RESPONSE;
        }
      } catch (TSaslNegotiationException e) {
        failSaslNegotiation(e);
      } catch (TTransportException e) {
        failIO(e);
      }
    }
  }

  private void handleReadingRequest() {
    if (!nonblockingRequestReader.isComplete()) {
      try {
        nonblockingRequestReader.read();
        if (nonblockingRequestReader.isComplete()) {
          nextPhase = Phase.PROCESSING;
        }
      } catch (TTransportException e) {
        failIO(e);
      }
    }
  }

  // Computation executions

  private void executeEvaluatingSaslResponse() {
    if (!(saslResponse.getHeader().getStatus() == OK || saslResponse.getHeader().getStatus() == COMPLETE)) {
      String error = "Expect status OK or COMPLETE, but got " + saslResponse.getHeader().getStatus();
      failSaslNegotiation(new TSaslNegotiationException(ErrorType.PROTOCOL_ERROR, error));
      return;
    }
    try {
      byte[] newChallenge = saslPeer.evaluate(saslResponse.getPayload());
      if (saslPeer.isAuthenticated()) {
        saslChallenge.withHeaderAndPayload(new byte[]{COMPLETE.getValue()}, newChallenge);
        nextPhase = Phase.WRITING_SUCCESS_MESSAGE;
      } else {
        saslChallenge.withHeaderAndPayload(new byte[]{OK.getValue()}, newChallenge);
        nextPhase = Phase.WRITING_SASL_CHALLENGE;
      }
    } catch (TSaslNegotiationException e) {
      failSaslNegotiation(e);
    }
  }

  private void executeProcessing() {
    try {
      memoryTransport.reset(nonblockingRequestReader.getFrameBytes());

      if (eventHandler != null) {
        if (!serverContextCreated) {
          serverContext = eventHandler.createContext(requestProtocol, responseProtocol);
          serverContextCreated = true;
        }
        eventHandler.processContext(serverContext, processingInputTransport, processingOutputTransport);
      }

      TProcessor processor = processorFactory.getProcessor(processingInputTransport);
      processor.process(requestProtocol, responseProtocol);
      byte[] response = memoryTransport.getOutput();
      if (response.length == 0) {
        // This is a oneway request, no response to send back. Waiting for next incoming request.
        nextPhase = Phase.READING_REQUEST;
        return;
      }
      nonblockingResponseWriter.withFrameBytes(response);
      nextPhase = Phase.WRITING_RESPONSE;
    } catch (TTransportException e) {
      failIO(e);
    } catch (TException e) {
      fail(e);
    }
  }

  // Write handlings

  private void handleWritingSaslChallenge() {
    if (!saslChallenge.isComplete()) {
      try {
        saslChallenge.write();
        if (saslChallenge.isComplete()) {
          nextPhase = Phase.READING_SASL_RESPONSE;
        }
      } catch (IOException e) {
        fail(e);
      }
    }
  }

  private void handleWritingSuccessMessage() {
    if (!saslChallenge.isComplete()) {
      try {
        saslChallenge.write();
        if (saslChallenge.isComplete()) {
          LOGGER.debug("Authentication is done.");
          saslChallenge = null;
          saslResponse = null;
          memoryTransport = new TMemorySaslServerTransport(saslPeer);
          processingInputTransport = inputTransportFactory.getTransport(memoryTransport);
          processingOutputTransport = outputTransportFactory.getTransport(memoryTransport);
          requestProtocol = requestProtocolFactory.getProtocol(processingInputTransport);
          responseProtocol = responseProtocolFactory.getProtocol(processingOutputTransport);
          nextPhase = Phase.READING_REQUEST;
        }
      } catch (IOException e) {
        fail(e);
      }
    }
  }

  private void handleWritingFailureMessage() {
    if (!saslChallenge.isComplete()) {
      try {
        saslChallenge.write();
        if (saslChallenge.isComplete()) {
          nextPhase = Phase.CLOSING;
        }
      } catch (IOException e) {
        fail(e);
      }
    }
  }

  private void handleWritingResponse() {
    if (!nonblockingResponseWriter.isComplete()) {
      try {
        nonblockingResponseWriter.write();
        if (nonblockingResponseWriter.isComplete()) {
          nextPhase = Phase.READING_REQUEST;
        }
      } catch (IOException e) {
        fail(e);
      }
    }
  }

  /**
   * Release all the resources managed by this state machine (connection, selection and sasl server).
   * To avoid being blocked, this should be invoked in the network thread that manages the selector.
   */
  public void close() {
    transport.close();
    selectionKey.cancel();
    saslPeer.dispose();
    if (serverContextCreated) {
      eventHandler.deleteContext(serverContext, requestProtocol, responseProtocol);
    }
    nextPhase = Phase.CLOSED;
    currentPhase = Phase.CLOSED;
    LOGGER.trace("Connection closed: {}", transport);
  }

  public enum Phase {
    INITIIALIIZING(INTEREST_READ) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleInitializing();
      }
    },
    READING_SASL_RESPONSE(INTEREST_READ) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleReadingSaslResponse();
      }
    },
    EVALUATING_SASL_RESPONSE(INTEREST_NONE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.executeEvaluatingSaslResponse();
      }
    },
    WRITING_SASL_CHALLENGE(INTEREST_WRITE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleWritingSaslChallenge();
      }
    },
    WRITING_SUCCESS_MESSAGE(INTEREST_WRITE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleWritingSuccessMessage();
      }
    },
    WRITING_FAILURE_MESSAGE(INTEREST_WRITE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleWritingFailureMessage();
      }
    },
    READING_REQUEST(INTEREST_READ) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleReadingRequest();
      }
    },
    PROCESSING(INTEREST_NONE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.executeProcessing();
      }
    },
    WRITING_RESPONSE(INTEREST_WRITE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.handleWritingResponse();
      }
    },
    CLOSING(INTEREST_NONE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        statemachine.close();
      }
    },
    CLOSED(INTEREST_NONE) {
      @Override
      void unsafeRun(NonblockingSaslHandler statemachine) {
        // Do nothing.
      }
    }
    ;

    // The interest on the selection key during the phase
    private int selectionInterest;

    Phase(int selectionInterest) {
      this.selectionInterest = selectionInterest;
    }

    /**
     * Provide the execution to run for the state machine in current phase. The execution should
     * return the next phase after running on the state machine.
     *
     * @param statemachine The state machine to run.
     * @throws IllegalArgumentException if the state machine's current phase is different.
     * @throws IllegalStateException if the state machine' current phase is already done.
     */
    void runStateMachine(NonblockingSaslHandler statemachine) {
      if (statemachine.currentPhase != this) {
        throw new IllegalArgumentException("State machine is " + statemachine.currentPhase + " but is expected to be " + this);
      }
      if (statemachine.isCurrentPhaseDone()) {
        throw new IllegalStateException("State machine should step into " + statemachine.nextPhase);
      }
      unsafeRun(statemachine);
    }

    // Run the state machine regardless of its own phase, and it should not be called direcly by users.
    abstract void unsafeRun(NonblockingSaslHandler statemachine);
  }
}
