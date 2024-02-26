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

package org.apache.impala.hive.common;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.ValidWriteIdList;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class is a mutable version of ValidReaderWriteIdList. With this class, we can
 * maintain the writeIdList based on the HMS events coming in. We need to always mark
 * the writeId as open before to mark it as aborted/committed.
 * Also note that this class is not thread safe, we should not update one instance by
 * multiple threads.
 */
public class MutableValidReaderWriteIdList implements MutableValidWriteIdList {
  private static final Logger LOG =
      LoggerFactory.getLogger(MutableValidReaderWriteIdList.class);

  private String tableName; // Full table name of format <db_name>.<table_name>
  private List<Long> exceptions;
  private BitSet abortedBits;
  private long minOpenWriteId = Long.MAX_VALUE;
  private long highWatermark;

  public MutableValidReaderWriteIdList(ValidWriteIdList writeIdList) {
    readFromString(writeIdList.writeToString());
  }

  @Override
  public boolean isWriteIdValid(long writeId) {
    if (writeId > highWatermark) {
      return false;
    }
    return Collections.binarySearch(exceptions, writeId) < 0;
  }

  @Override
  public boolean isValidBase(long writeId) {
    return (writeId < minOpenWriteId) && (writeId <= highWatermark);
  }

  @Override
  public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId) {
    if (minWriteId > highWatermark) {
      return RangeResponse.NONE;
    }
    if (exceptions.size() > 0 && exceptions.get(0) > maxWriteId) {
      return RangeResponse.ALL;
    }

    // since the exceptions and the range in question overlap, count the
    // exceptions in the range
    long count = Math.max(0, maxWriteId - highWatermark);
    for (long txn : exceptions) {
      if (minWriteId <= txn && txn <= maxWriteId) {
        count += 1;
      }
    }

    if (count == 0) {
      return RangeResponse.ALL;
    }
    if (count == (maxWriteId - minWriteId + 1)) {
      return RangeResponse.NONE;
    }
    return RangeResponse.SOME;
  }

  @Override
  public String toString() {
    return writeToString();
  }

  // Format is <table_name>:<hwm>:<minOpenWriteId>:<open_writeids>:<abort_writeids>
  @Override
  public String writeToString() {
    StringBuilder buf = new StringBuilder();
    if (tableName == null) {
      buf.append("null");
    } else {
      buf.append(tableName);
    }
    buf.append(':');
    buf.append(highWatermark);
    buf.append(':');
    buf.append(minOpenWriteId);

    StringBuilder open = new StringBuilder();
    StringBuilder abort = new StringBuilder();
    for (int i = 0; i < exceptions.size(); i++) {
      if (abortedBits.get(i)) {
        if (abort.length() > 0) {
          abort.append(',');
        }
        abort.append(exceptions.get(i));
      } else {
        if (open.length() > 0) {
          open.append(',');
        }
        open.append(exceptions.get(i));
      }
    }
    buf.append(':');
    buf.append(open);
    buf.append(':');
    buf.append(abort);

    return buf.toString();
  }

  @Override
  public void readFromString(String src) {
    if (src == null || src.length() == 0) {
      highWatermark = Long.MAX_VALUE;
      exceptions = new ArrayList<>();
      abortedBits = new BitSet();
    } else {
      String[] values = src.split(":");
      Preconditions.checkState(values.length >= 3, "Not enough values");
      tableName = values[0];
      if (tableName.equalsIgnoreCase("null")) {
        tableName = null;
      }
      highWatermark = Long.parseLong(values[1]);
      minOpenWriteId = Long.parseLong(values[2]);
      String[] openWriteIds = new String[0];
      String[] abortedWriteIds = new String[0];
      if (values.length < 4) {
        openWriteIds = new String[0];
        abortedWriteIds = new String[0];
      } else if (values.length == 4) {
        if (!values[3].isEmpty()) {
          openWriteIds = values[3].split(",");
        }
      } else {
        if (!values[3].isEmpty()) {
          openWriteIds = values[3].split(",");
        }
        if (!values[4].isEmpty()) {
          abortedWriteIds = values[4].split(",");
        }
      }
      exceptions = new ArrayList<>(openWriteIds.length + abortedWriteIds.length);
      for (String open : openWriteIds) {
        exceptions.add(Long.parseLong(open));
      }
      for (String abort : abortedWriteIds) {
        exceptions.add(Long.parseLong(abort));
      }
      Collections.sort(exceptions);
      abortedBits = new BitSet(exceptions.size());
      for (String abort : abortedWriteIds) {
        int index = Collections.binarySearch(exceptions, Long.parseLong(abort));
        abortedBits.set(index);
      }
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public long getHighWatermark() {
    return highWatermark;
  }

  @Override
  public long[] getInvalidWriteIds() {
    return Longs.toArray(exceptions);
  }

  @Override
  public Long getMinOpenWriteId() {
    return minOpenWriteId == Long.MAX_VALUE ? null : minOpenWriteId;
  }

  @Override
  public boolean isWriteIdAborted(long writeId) {
    int index = Collections.binarySearch(exceptions, writeId);
    return index >= 0 && abortedBits.get(index);
  }

  @Override
  public RangeResponse isWriteIdRangeAborted(long minWriteId, long maxWriteId) {
    if (highWatermark < minWriteId) {
      return RangeResponse.NONE;
    }

    int count = 0; // number of aborted txns found in exceptions

    // traverse the aborted txns list, starting at first aborted txn index
    for (int i = abortedBits.nextSetBit(0); i >= 0; i = abortedBits.nextSetBit(i + 1)) {
      long abortedTxnId = exceptions.get(i);
      if (abortedTxnId > maxWriteId) { // we've already gone beyond the specified range
        break;
      }
      if (abortedTxnId >= minWriteId && abortedTxnId <= maxWriteId) {
        count++;
      }
    }

    if (count == 0) {
      return RangeResponse.NONE;
    }
    if (count == (maxWriteId - minWriteId + 1)) {
      return RangeResponse.ALL;
    }
    return RangeResponse.SOME;
  }

  @Override
  public boolean isWriteIdOpen(long writeId) {
    int index = Collections.binarySearch(exceptions, writeId);
    return index >= 0 && !abortedBits.get(index);
  }

  @Override
  public boolean addOpenWriteId(long writeId) {
    if (writeId <= highWatermark) {
      LOG.debug("Not adding open write id: {} since high water mark: {}", writeId,
          highWatermark);
      return false;
    }
    for (long currentId = highWatermark + 1; currentId <= writeId; currentId++) {
      exceptions.add(currentId);
    }
    LOG.debug("Added OPEN write id: {}. Old high water mark: {}.",
        writeId, highWatermark);
    highWatermark = writeId;
    return true;
  }

  @Override
  public boolean addAbortedWriteIds(List<Long> writeIds) {
    Preconditions.checkNotNull(writeIds);
    Preconditions.checkArgument(writeIds.size() > 0);
    // used to track if any of the writeIds is added
    boolean added = false;
    long maxWriteId = Collections.max(writeIds);
    if (maxWriteId > highWatermark) {
      LOG.info("Current high water mark: {} and max aborted write id: {}, so mark them "
          + "as open first", highWatermark, maxWriteId);
      addOpenWriteId(maxWriteId);
      added = true;
    }
    for (long writeId : writeIds) {
      int index = Collections.binarySearch(exceptions, writeId);
      if (index < 0) {
        LOG.info("Not added ABORTED write id {} since it's not opened and might " +
            "already be cleaned up. minOpenWriteId: {}.", writeId, minOpenWriteId);
        continue;
      }
      added = added || !abortedBits.get(index);
      abortedBits.set(index);
    }
    updateMinOpenWriteId();
    if (!added) {
      LOG.info("Not added any ABORTED write ids of the given {}", writeIds.size());
    }
    return added;
  }

  @Override
  public boolean addCommittedWriteIds(List<Long> writeIds) {
    Preconditions.checkNotNull(writeIds);
    Preconditions.checkArgument(writeIds.size() > 0);
    long maxWriteId = Collections.max(writeIds);
    if (maxWriteId > highWatermark) {
      LOG.trace("Current high water mark: {} and max committed write id: {}, so mark "
          + "them as open first", highWatermark, maxWriteId);
      addOpenWriteId(maxWriteId);
    }
    List<Long> updatedExceptions = new ArrayList<>();
    BitSet updatedAbortedBits = new BitSet();

    Set<Integer> idxToRemove = new HashSet<>();
    for (long writeId : writeIds) {
      int idx = Collections.binarySearch(exceptions, writeId);
      if (idx >= 0) {
        // make sure the write id is open rather than aborted
        Preconditions.checkState(!abortedBits.get(idx),
            "write id %d is expected to be open but is aborted", writeId);
        idxToRemove.add(idx);
      }
    }
    for (int idx = 0; idx < exceptions.size(); idx++) {
      if (idxToRemove.contains(idx)) {
        continue;
      }
      updatedAbortedBits.set(updatedExceptions.size(), abortedBits.get(idx));
      updatedExceptions.add(exceptions.get(idx));
    }
    exceptions = updatedExceptions;
    abortedBits = updatedAbortedBits;
    updateMinOpenWriteId();
    return !idxToRemove.isEmpty();
  }

  private void updateMinOpenWriteId() {
    int index = abortedBits.nextClearBit(0);
    if (index >= exceptions.size()) {
      minOpenWriteId = Long.MAX_VALUE;
    } else {
      minOpenWriteId = exceptions.get(index);
    }
  }
}
