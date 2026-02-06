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

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.impala.thrift.THash128;

/**
 * Represents a 128-bit hash value using two longs (high and low 64 bits).
 *
 * Usage:
 * - Create: Hash128 hash = IcebergUtil.getFilePathHash(filePath);
 * - As map key: map.put(hash, value);
 * - Thrift: THash128 t = hash.toThrift(); Hash128 h = Hash128.fromThrift(t);
 * - Logging: LOG.debug("Hash: {}", hash.toString());
 */
public class Hash128 {
  private final long high_;
  private final long low_;

  public Hash128(long high, long low) {
    high_ = high;
    low_ = low;
  }

  public long getHigh() {
    return high_;
  }

  public long getLow() {
    return low_;
  }

  /**
   * Converts this hash to a Thrift THash128 struct for serialization.
   */
  public THash128 toThrift() {
    return new THash128(high_, low_);
  }

  /**
   * Creates a Hash128 from a Thrift THash128 struct.
   */
  public static Hash128 fromThrift(THash128 thrift) {
    Preconditions.checkNotNull(thrift);
    return new Hash128(thrift.getHigh(), thrift.getLow());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Hash128 hash128 = (Hash128) o;
    return high_ == hash128.high_ && low_ == hash128.low_;
  }

  @Override
  public int hashCode() {
    return Objects.hash(high_, low_);
  }

  @Override
  public String toString() {
    // Return hexadecimal representation for debugging
    return String.format("%016x%016x", high_, low_);
  }
}
