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

import org.apache.thrift.transport.TEOFException;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

/**
 * Read messages from sasl transport. Implementations should subclass it by providing a header implementation.
 *
 * @param <T> Header type.
 */
public abstract class SaslReader<T extends SaslHeader> {
  private final TTransport transport;
  private final T header;
  private ByteBuffer frameBytes;

  protected SaslReader(T header, TTransport transport) {
    this.transport = transport;
    this.header = header;
  }

  /**
   * (Nonblocking) Read available bytes out of the transport without blocking to wait for incoming
   * data.
   *
   * @return bytes read
   * @throws TSaslNegotiationException if fail to read back a valid sasl negotiation message.
   * @throws TTransportException if io error.
   */
  public int read() throws TSaslNegotiationException, TTransportException {
    int got = 0;
    if (!header.isComplete()) {
      got += readHeader(transport);
      if (header.isComplete()) {
        byte[] headerBytes = header.toBytes();
        frameBytes = ByteBuffer.allocate(headerBytes.length + header.payloadSize());
        frameBytes.put(headerBytes);
      } else {
        return got;
      }
    }
    if (header.payloadSize() > 0) {
      got += readPayload(transport);
    }
    return got;
  }

  /**
   * (Nonblocking) Try to read available header bytes from transport.
   *
   * @return The bytes read out of the transport.
   * @throws TSaslNegotiationException if fail to read back a validd sasl negotiation header.
   * @throws TTransportException if io error.
   */
  private int readHeader(TTransport transport) throws TSaslNegotiationException, TTransportException {
    return header.read(transport);
  }

  /**
   * (Nonblocking) Try to read available
   *
   * @param transport underlying transport.
   * @return number of bytes read out of the socket.
   * @throws TTransportException if io error.
   */
  private int readPayload(TTransport transport) throws TTransportException {
    return readAvailable(transport, frameBytes);
  }

  public T getHeader() {
    return header;
  }

  public int getHeaderSize() {
    return header.toBytes().length;
  }

  public byte[] getPayload() {
    byte[] bytes = new byte[getPayloadSize()];
    System.arraycopy(getFrameBytes(), getHeaderSize(), bytes, 0, getPayloadSize());
    return bytes;
  }

  public int getPayloadSize() {
    return header.payloadSize();
  }

  /**
   *
   * @return All bytes (both header and payload).
   * @throws IllegalStateException if it is called before reader finishes reading a whole frame.
   */
  public byte[] getFrameBytes() {
    if (!isComplete()) {
      throw new IllegalStateException("Reader is not yet complete.");
    }
    return frameBytes.array();
  }

  /**
   *
   * @return true if the reader has fully read a frame.
   */
  public boolean isComplete() {
    return !(frameBytes == null || frameBytes.hasRemaining());
  }

  /**
   * Reset the state of the reader so that it can be reused to read a new frame.
   */
  public void clear() {
    header.clear();
    frameBytes = null;
  }

  /**
   * Read immediately available bytes from the transport into the byte buffer.
   *
   * @param transport TTransport
   * @param recipient ByteBuffer
   * @return number of bytes read out of the transport
   * @throws TTransportException if io error
   */
  static int readAvailable(TTransport transport, ByteBuffer recipient) throws TTransportException {
    return fillRecipient(transport, recipient, false);
  }

  /**
   * Read from the transport into the byte buffer, and block until the byte buffer is full filled.
   *
   * @param transport TTransport
   * @param recipient ByteBuffer
   * @return number of bytes read out of the transport
   * @throws TTransportException if io error
   */
  static int readAll(TTransport transport, ByteBuffer recipient) throws TTransportException {
    return fillRecipient(transport, recipient, true);
  }

  private static int fillRecipient(TTransport transport, ByteBuffer recipient, boolean blocking) throws TTransportException {
    if (!recipient.hasRemaining()) {
      throw new IllegalStateException("Trying to fill a full recipient with " + recipient.limit() + " bytes");
    }
    int currentPosition = recipient.position();
    byte[] bytes = recipient.array();
    int offset = recipient.arrayOffset() + currentPosition;
    int expectedLength = recipient.remaining();
    int got = blocking ? transport.readAll(bytes, offset, expectedLength) : transport.read(bytes, offset, expectedLength);
    if (got < 0) {
      throw new TEOFException("Transport is closed, while trying to read " + expectedLength + " bytes");
    }
    recipient.position(currentPosition + got);
    return got;
  }
}
