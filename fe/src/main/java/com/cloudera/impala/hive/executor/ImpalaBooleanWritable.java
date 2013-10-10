// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.hive.executor;

import org.apache.hadoop.io.BooleanWritable;

import com.cloudera.impala.util.UnsafeUtil;

@SuppressWarnings("restriction")
public class ImpalaBooleanWritable extends BooleanWritable {
  // Ptr (to native heap) where the value should be read from and written to.
  private final long ptr_;

  public ImpalaBooleanWritable(long ptr) {
    ptr_ = ptr;
  }

  @Override
  public boolean get() { return UnsafeUtil.UNSAFE.getByte(ptr_) != 0; }

  @Override
  public void set(boolean v) { UnsafeUtil.UNSAFE.putByte(ptr_, v ? (byte)1 : 0); }
}
