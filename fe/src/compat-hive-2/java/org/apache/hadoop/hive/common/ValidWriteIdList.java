// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
package org.apache.hadoop.hive.common;

/**
 * ValidWriteIdList is not supported in Hive 2
 */
public class ValidWriteIdList {
  public enum RangeResponse {NONE, SOME, ALL};

  public boolean isWriteIdValid(long writeId) {
    throw new UnsupportedOperationException("isWriteIdValid not supported for "
        + getClass().getName());
  }

  public boolean isValidBase(long writeId) {
    throw new UnsupportedOperationException("isValidBase not supported for "
        + getClass().getName());
  }

  public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
    throw new UnsupportedOperationException("isWriteIdRangeValid not supported for "
        + getClass().getName());
  }

  public String writeToString() {
    throw new UnsupportedOperationException("writeToStringd not supported for "
        + getClass().getName());
  }

  public void readFromString(String src) {
    throw new UnsupportedOperationException("readFromString not supported for "
        + getClass().getName());
  }

  public long getHighWatermark() {
    throw new UnsupportedOperationException("getHighWatermark not supported for "
        + getClass().getName());
  }

  public long[] getInvalidWriteIds() {
    throw new UnsupportedOperationException("getInvalidWriteIds not supported for "
        + getClass().getName());
  }

  public boolean isWriteIdAborted(long writeId) {
    throw new UnsupportedOperationException("isWriteIdAborted not supported for "
        + getClass().getName());
  }

  public RangeResponse isWriteIdRangeAborted(long minWriteId, long maxWriteId) {
    throw new UnsupportedOperationException(
        "isWriteIdRangeAborted not supported for " + getClass().getName());
  }

  public Long getMinOpenWriteId() {
    throw new UnsupportedOperationException("getMinOpenWriteId not supported for "
        + getClass().getName());
  }
}
