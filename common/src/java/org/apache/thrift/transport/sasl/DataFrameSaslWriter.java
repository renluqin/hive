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

import org.apache.thrift.transport.TNonblockingTransport;
import org.apache.thrift.utils.StringUtils;

import java.nio.ByteBuffer;

public class DataFrameSaslWriter extends SaslWriter {

  public DataFrameSaslWriter(TNonblockingTransport transport) {
    super(transport);
  }

  @Override
  protected ByteBuffer buildBuffer(byte[] header, byte[] payload) {
    if (header != null && header.length > 0) {
      throw new IllegalArgumentException("Header should be empty, but got " + StringUtils.bytesToHexString(header));
    }
    return ByteBuffer.allocate(DataFrameHeader.PAYLOAD_LENGTH_BYTES + payload.length)
        .putInt(payload.length)
        .put(payload);
  }
}
