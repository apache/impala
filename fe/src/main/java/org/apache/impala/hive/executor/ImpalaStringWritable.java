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

import java.nio.ByteBuffer;

import org.apache.impala.util.UnsafeUtil;

@SuppressWarnings("restriction")
/**
 * Underlying class for Text and Bytes writable. This class understands marshalling
 * values that map to StringValue in the BE.
 * StringValue is replicated here:
 * struct StringValue {
 *   char* ptr;
 *   int len;
 * };
 */
public class ImpalaStringWritable {
  // The length is 8 bytes into the struct.
  static public final int STRING_VALUE_LEN_OFFSET = 8;

  // Ptr (to native heap) where the value should be read from and written to.
  // This needs to be ABI compatible with the BE StringValue class
  private final long stringValPtr_;

  // Array object to convert between native and java heap (i.e. byte[]).
  private ByteBuffer array_;

  // Set if this object had to allocate from the native heap on the java side. If this
  // is set, it will always be stringValPtr_->ptr
  // We only need to allocate from the java side if we are trying to set the
  // StringValue to a bigger size than what the native side allocated.
  // If this object is used as a read-only input argument, this value will stay
  // 0.
  private long bufferPtr_;

  // Allocation size of stringValPtr_'s ptr.
  private int bufferCapacity_;

  // Creates a string writable backed by a StringValue object. Ptr must be a valid
  // StringValue (in the native heap).
  public ImpalaStringWritable(long ptr) {
    stringValPtr_ = ptr;
    bufferPtr_= 0;
    bufferCapacity_ = getLength();
    array_ = ByteBuffer.allocate(0);
  }

  /*
   * Implement finalize() to clean up any allocations from the native heap.
   */
  @Override
  protected void finalize() throws Throwable {
    UnsafeUtil.UNSAFE.freeMemory(bufferPtr_);
    super.finalize();
  }

  // Returns the underlying bytes as a byte[]
  public byte[] getBytes() {
    int len = getLength();
    // TODO: reuse this array.
    array_ = ByteBuffer.allocate(len);
    byte[] buffer = array_.array();

    long srcPtr = UnsafeUtil.UNSAFE.getLong(stringValPtr_);
    UnsafeUtil.Copy(buffer, 0, srcPtr, len);
    return buffer;
  }

  // Returns the capacity of the underlying array
  public int getCapacity() {
    return bufferCapacity_;
  }

  // Updates the new capacity. No-op if the new capacity is smaller.
  public void setCapacity(int newCap) {
    if (newCap <= bufferCapacity_) return;
    bufferPtr_ = UnsafeUtil.UNSAFE.reallocateMemory(bufferPtr_, newCap);
    UnsafeUtil.UNSAFE.putLong(stringValPtr_, bufferPtr_);
    bufferCapacity_ = newCap;
  }

  // Returns the length of the string
  public int getLength() {
    return UnsafeUtil.UNSAFE.getInt(stringValPtr_ + STRING_VALUE_LEN_OFFSET);
  }

  // Updates the length of the string. If the new length is bigger,
  // the additional bytes are undefined.
  public void setSize(int s) {
    setCapacity(s);
    UnsafeUtil.UNSAFE.putInt(stringValPtr_ + 8, s);
  }

  // Sets (v[offset], len) to the underlying buffer, growing it as necessary.
  public void set(byte[] v, int offset, int len) {
    setSize(len);
    long strPtr = UnsafeUtil.UNSAFE.getLong(stringValPtr_);
    UnsafeUtil.Copy(strPtr, v, offset, len);
  }
}
