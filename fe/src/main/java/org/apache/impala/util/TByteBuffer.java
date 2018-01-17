// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.util;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * ByteBuffer-backed implementation of TTransport. This is copied from thrift 0.10.0.
 * TODO: Upgrade thrift to 0.10.0 or higher and remove this file.
 */
public final class TByteBuffer extends TTransport {
  private final ByteBuffer byteBuffer;

  public TByteBuffer(ByteBuffer byteBuffer) { this.byteBuffer = byteBuffer; }

  @Override
  public boolean isOpen() { return true; }

  @Override
  public void open() {}

  @Override
  public void close() {}

  @Override
  public int read(byte[] buf, int off, int len) throws TTransportException {
    final int n = Math.min(byteBuffer.remaining(), len);
    if (n > 0) {
      try {
        byteBuffer.get(buf, off, n);
      } catch (BufferUnderflowException e) {
        throw new TTransportException("Unexpected end of input buffer", e);
      }
    }
    return n;
  }

  @Override
  public void write(byte[] buf, int off, int len) throws TTransportException {
    throw new TTransportException("Write is not supported by TByteBuffer");
  }
}
