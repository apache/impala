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

/**
 * An int arraylist used to reduce GC pressure and to remove unnecessary boxing.
 */
public class IntArrayList {
  private int[] data_;
  private int size_;

  public IntArrayList() { this(0); }

  public IntArrayList(int capacity) {
    data_ = new int[capacity];
    size_ = 0;
  }

  public int[] data() { return data_; }

  /**
   * Grow the internal array as needed to accommodate the specified number of elements.
   */
  private void ensureCapacity(int capacity) {
    if (capacity > data_.length) {
      int[] newData = new int[Math.max(data_.length * 2, capacity)];
      System.arraycopy(data_, 0, newData, 0, data_.length);
      data_ = newData;
    }
  }

  /**
   * Add an int at the end of the list.
   */
  public void add(int value) {
    ensureCapacity(size_ + 1);
    data_[size_++] = value;
  }

  /**
   * Remove elements from the end the of list.
   */
  public void removeLast(int numRemove) {
    if (numRemove < 0 || numRemove > size_) {
      throw new ArrayIndexOutOfBoundsException();
    }
    size_ -= numRemove;
  }

  public void clear() { size_ = 0; }

  public int size() { return size_; }

  public void set(int index, int value) {
    if (index >= size_) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    data_[index] = value;
  }

  public int get(int index) {
    if (index >= size_) {
      throw new ArrayIndexOutOfBoundsException(index);
    }
    return data_[index];
  }

  public IntIterator iterator() {
    return new IntIterator() {
      private int pos_ = 0;

      @Override
      public boolean hasNext() { return pos_ < size(); }

      @Override
      public int next() { return get(pos_++); }

      @Override
      public int peek() { return get(pos_); }
    };
  }
}
