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

package org.apache.thrift.transport;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * In memory transport with separate buffers for input and output. It is expected to call reset to
 * provide the input and clean up the output, each time before it is used by a thrfit processor.
 * {@link TMemoryTransport#isOpen()} always returns true only after
 * {@link TMemoryTransport#reset(byte[])} or {@link TMemoryTransport#read(byte[], int, int)}
 * is called at least once.
 */
public class TMemoryTransport extends TTransport {

  private ByteBuffer inputBuffer;
  private List<byte[]> outputBuffer = new ArrayList<>();

  @Override
  public boolean isOpen() {
    return !(inputBuffer == null || outputBuffer == null);
  }

  /**
   * Opening on an in memory transport should have no effect.
   */
  @Override
  public void open() {
    // Do nothing.
  }

  @Override
  public void close() {
    inputBuffer = null;
    outputBuffer = null;
  }

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    int remaining = inputBuffer.remaining();
    if (remaining < len) {
      throw new TTransportException(TTransportException.END_OF_FILE,
          "There's only " + remaining + ", but is asked for " + len + " bytes");
    }
    inputBuffer.get(buf, off, len);
    return len;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    byte[] written = new byte[len];
    System.arraycopy(buf, off, written, 0, len);
    outputBuffer.add(written);
  }

  /**
   * Reset the in memory transport, by providing a new input and clearing the output buffer.
   * This method should be called before using it.
   *
   * @param input The input byte array that will be read by thrift input protocol.
   */
  public void reset(byte[] input) {
    inputBuffer = ByteBuffer.wrap(input);
    outputBuffer.clear();
  }

  /**
   * Get all the bytes written by thrift output protocol.
   *
   * @return byte array.
   */
  public byte[] getOutput() {
    int length = 0;
    for (byte[] bytes : outputBuffer) {
      length += bytes.length;
    }
    byte[] output = new byte[length];
    int position = 0;
    for (byte[] bytes : outputBuffer) {
      System.arraycopy(bytes, 0, output, position, bytes.length);
      position += bytes.length;
    }
    return output;
  }
}
