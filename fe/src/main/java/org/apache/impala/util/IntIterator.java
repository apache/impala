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

/** Primitive int iterator */
public abstract class IntIterator {
  public abstract boolean hasNext();
  public abstract int next();
  public abstract int peek();

  static IntIterator fromArray(final int[] array) {
    return new IntIterator() {
      private int pos_ = 0;

      @Override
      public boolean hasNext() { return pos_ < array.length; }

      @Override
      public int next() { return array[pos_++]; }

      @Override
      public int peek() { return array[pos_]; }
    };
  }
}
