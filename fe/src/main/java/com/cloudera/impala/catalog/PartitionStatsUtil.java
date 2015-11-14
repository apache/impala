// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TPartitionStats;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.util.MetaStoreUtil;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Base64;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TException;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Handles serialising and deserialising intermediate statistics from the Hive MetaStore
 * via the parameters map attached to every Hive partition object.
 */
public class PartitionStatsUtil {
  public static final String INCREMENTAL_STATS_NUM_CHUNKS =
      "impala_intermediate_stats_num_chunks";

  public static final String INCREMENTAL_STATS_CHUNK_PREFIX =
      "impala_intermediate_stats_chunk";

  private final static Logger LOG = LoggerFactory.getLogger(PartitionStatsUtil.class);

  /**
   * Reconstructs a TPartitionStats object from its serialised form in the given parameter
   * map. Returns null if no stats are serialised, and throws an exception if there was an
   * error during deserialisation.
   */
  public static TPartitionStats partStatsFromParameters(
      Map<String, String> hmsParameters) throws ImpalaException {
    if (hmsParameters == null) return null;
    String numChunksStr = hmsParameters.get(INCREMENTAL_STATS_NUM_CHUNKS);
    if (numChunksStr == null) return null;
    int numChunks = Integer.parseInt(numChunksStr);
    if (numChunks == 0) return null;

    Preconditions.checkState(numChunks >= 0);
    StringBuilder encodedStats = new StringBuilder();
    for (int i = 0; i < numChunks; ++i) {
      String chunk = hmsParameters.get(INCREMENTAL_STATS_CHUNK_PREFIX + i);
      if (chunk == null) {
        throw new ImpalaRuntimeException("Missing stats chunk: " + i);
      }
      encodedStats.append(chunk);
    }

    byte[] decodedStats = Base64.decodeBase64(encodedStats.toString());
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    TPartitionStats ret = new TPartitionStats();
    JniUtil.deserializeThrift(protocolFactory, ret, decodedStats);
    return ret;
  }

  /**
   * Serialises a TPartitionStats object to a partition.
   */
  public static void partStatsToParameters(TPartitionStats partStats,
      HdfsPartition partition) {
    // null stats means logically delete the stats from this partition
    if (partStats == null) {
      deletePartStats(partition);
      return;
    }

    // The HMS has a 4k (as of CDH5.2) limit on the length of any parameter string. The
    // serialised version of the partition stats is often larger than this. Therefore, we
    // naively 'chunk' the byte string into 4k pieces, and store the number of pieces in a
    // separate parameter field.
    //
    // The object itself is first serialised by Thrift, and then base-64 encoded to be a
    // valid string. This inflates its length somewhat; we may want to consider a
    // different scheme or at least understand why this scheme doesn't seem much more
    // effective than an ASCII representation.
    try {
      TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
      TSerializer serializer = new TSerializer(protocolFactory);
      byte[] serialized = serializer.serialize(partStats);
      String base64 = new String(Base64.encodeBase64(serialized));
      List<String> chunks =
          chunkStringForHms(base64, MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH);
      partition.putToParameters(
          INCREMENTAL_STATS_NUM_CHUNKS, Integer.toString(chunks.size()));
      for (int i = 0; i < chunks.size(); ++i) {
        partition.putToParameters(INCREMENTAL_STATS_CHUNK_PREFIX + i, chunks.get(i));
      }
    } catch (TException e) {
      LOG.info("Error saving partition stats: ", e);
      // TODO: What to throw here?
    }
  }

  public static void deletePartStats(HdfsPartition partition) {
    partition.putToParameters(INCREMENTAL_STATS_NUM_CHUNKS, "0");
    for (Iterator<String> it = partition.getParameters().keySet().iterator();
         it.hasNext(); ) {
      if (it.next().startsWith(INCREMENTAL_STATS_CHUNK_PREFIX)) {
        it.remove();
      }
    }
  }

  static private List<String> chunkStringForHms(String data, int chunkLen) {
    int idx = 0;
    List<String> ret = Lists.newArrayList();
    while (idx < data.length()) {
      int remaining = data.length() - idx;
      int chunkSize = (chunkLen > remaining) ? remaining : chunkLen;
      ret.add(data.substring(idx, idx + chunkSize));
      idx += chunkSize;
    }
    return ret;
  }
}
