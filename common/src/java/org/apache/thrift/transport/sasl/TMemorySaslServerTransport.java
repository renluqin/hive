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

import org.apache.thrift.TByteArrayOutputStream;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TMemoryTransport;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.SaslException;

/**
 * In memory sasl server transport, this is an adaptor of TSaslServerTransport over TMemoryTransport.
 * <br>
 * We should call reset to provide the input and clean up the output, each time before it is used by
 * a thrfit processor. The provided input bytes should be a complete sasl data frame (with 4 bytes
 * length prefix).
 * {@link TMemorySaslServerTransport#isOpen()} returns true only if
 * {@link TMemorySaslServerTransport#reset(byte[])} is called at least once.
 * <br>
 * It is not supposed to procede sasl negotiation here, thus we should provide an authenticated
 * sasl server for instantiation.
 */
public class TMemorySaslServerTransport extends TSaslServerTransport {
  private static final Logger LOGGER = LoggerFactory.getLogger(TMemorySaslServerTransport.class);

  private final TMemoryTransport saslBuffer;
  /**
   * Buffer for input.
   */
  private TMemoryInputTransport readBuffer = new TMemoryInputTransport();

  /**
   * Buffer for output.
   */
  private final TByteArrayOutputStream writeBuffer = new TByteArrayOutputStream(1024);
  private boolean dataProtected;

  /**
   *
   * @param saslServer authenticated sasl server
   * @throws IllegalArgumentException if sasl server is not complete/authenticated.
   */
  public TMemorySaslServerTransport(ServerSaslPeer saslServer) {
    super(new TMemoryTransport());
    if (!saslServer.isAuthenticated()) {
      throw new IllegalArgumentException("Sasl negotiation should be complete");
    }
    setSaslServer(saslServer.getSaslServer());
    dataProtected = saslServer.isDataProtected();
    saslBuffer = (TMemoryTransport) underlyingTransport;
  }

  @Override
  public boolean isOpen() {
    return saslBuffer.isOpen();
  }

  /**
   * Reset transport with given input, and clear output buffer.
   *
   * @param input a complete sasl data frame with payload size as 4 bytes prefix (bigendian).
   */
  public void reset(byte[] input) {
    saslBuffer.reset(input);
  }

  public byte[] getOutput() {
    return saslBuffer.getOutput();
  }

  @Override
  public void open() {
    saslBuffer.open();
  }

  /**
   * Read from the underlying transport. Unwraps the contents if a QOP was
   * negotiated during the SASL handshake.
   */
  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen())
      throw new TTransportException("SASL authentication not complete");

    int got = readBuffer.read(buf, off, len);
    if (got > 0) {
      return got;
    }

    // Read another frame of data
    try {
      readFrame();
    } catch (SaslException e) {
      throw new TTransportException(e);
    }

    return readBuffer.read(buf, off, len);
  }

  /**
   * Read a single frame of data from the underlying transport, unwrapping if
   * necessary.
   *
   * @throws TTransportException
   *           Thrown if there's an error reading from the underlying transport.
   * @throws SaslException
   *           Thrown if there's an error unwrapping the data.
   */
  private void readFrame() throws TTransportException, SaslException {
    int dataLength = readLength();

    if (dataLength < 0)
      throw new TTransportException("Read a negative frame size (" + dataLength + ")!");

    byte[] buff = new byte[dataLength];
    LOGGER.debug("{}: reading data length: {}", getRole(), dataLength);
    underlyingTransport.readAll(buff, 0, dataLength);
    if (dataProtected) {
      buff = getSaslServer().unwrap(buff, 0, buff.length);
      LOGGER.debug("data length after unwrap: {}", buff.length);
    }
    readBuffer.reset(buff);
  }

  /**
   * Write to the underlying transport.
   */
  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    if (!isOpen())
      throw new TTransportException("SASL authentication not complete");

    writeBuffer.write(buf, off, len);
  }

  /**
   * Flushes to the underlying transport. Wraps the contents if a QOP was
   * negotiated during the SASL handshake.
   */
  @Override
  public void flush() throws TTransportException {
    byte[] buf = writeBuffer.get();
    int dataLength = writeBuffer.len();
    writeBuffer.reset();

    if (dataProtected) {
      LOGGER.debug("data length before wrap: {}", dataLength);
      try {
        buf = getSaslServer().wrap(buf, 0, dataLength);
      } catch (SaslException e) {
        throw new TTransportException(e);
      }
      dataLength = buf.length;
    }
    LOGGER.debug("writing data length: {}", dataLength);
    writeLength(dataLength);
    underlyingTransport.write(buf, 0, dataLength);
    underlyingTransport.flush();
  }
}
