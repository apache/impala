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

package org.apache.impala.hive.executor;

import org.apache.hadoop.io.BytesWritable;

/**
 * Impala writable type that implements the BytesWritable interface. The data
 * marshalling is handled by the underlying {@link ImpalaStringWritable} object.
 */
public class ImpalaBytesWritable extends BytesWritable {
  private final ImpalaStringWritable string_;

  public ImpalaBytesWritable(long ptr) {
    string_ = new ImpalaStringWritable(ptr);
  }

  @Override
  public byte[] copyBytes() {
    byte[] src = getBytes();
    return src.clone();
  }

  @Override
  public byte[] get() { return getBytes(); }
  @Override
  public byte[] getBytes() { return string_.getBytes(); }
  @Override
  public int getCapacity() { return string_.getCapacity(); }
  @Override
  public int getLength() { return string_.getLength(); }

  public ImpalaStringWritable getStringWritable() { return string_; }

  @Override
  public void set(byte[] v, int offset, int len) { string_.set(v, offset, len); }
  @Override
  public void setCapacity(int newCap) { string_.setCapacity(newCap); }
  @Override
  public void setSize(int size) { string_.setSize(size); }
}
