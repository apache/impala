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

package org.apache.impala.catalog;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TPartitionStats;
import org.apache.impala.util.CompressionUtil;
import org.apache.impala.util.MetaStoreUtil;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

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
   * Deflate-decompresses 'compressedStats' and deserializes it into TPartitionStats.
   * Returns null if either 'compressedStats' is null or deserialization/decompression
   * returns a null value. The partition, 'part', provides debugging context.
   * Throws an exception if there is an error with deserialization/decompression.
   */
  public static TPartitionStats partStatsFromCompressedBytes(
      byte[] compressedStats, FeFsPartition part) throws ImpalaException {
    if (compressedStats == null) return null;
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    TPartitionStats ret = new TPartitionStats();
    byte[] decompressed = CompressionUtil.deflateDecompress(compressedStats);
    if (decompressed == null) {
      String partitionName = (part == null ? "N/A" : part.getPartitionName());
      LOG.warn("Error decompressing partition stats for partition: " + partitionName);
      return null;
    }
    JniUtil.deserializeThrift(protocolFactory, ret, decompressed);
    return ret;
  }

  /**
   * Get the partition stats from the given partition, or null if no stats
   * are available. If stats are present but cannot be parsed, logs a warning
   * and returns null.
   */
  public static TPartitionStats getPartStatsOrWarn(FeFsPartition part) {
    try {
      byte[] compressedStats = part.getPartitionStatsCompressed();
      return partStatsFromCompressedBytes(compressedStats, part);
    } catch (ImpalaException e) {
      LOG.warn("Bad partition stats for " + part.getPartitionName(), e);
      return null;
    }
  }

  /**
   * Reconstructs the intermediate stats from chunks and returns the corresponding
   * byte array. The output byte array is deflate-compressed. Sets hasIncrStats to
   * 'true' if the partition stats contain intermediate col stats.
   */
  public static byte[] partStatsBytesFromParameters(
      Map<String, String> hmsParameters, Reference<Boolean> hasIncrStats) throws
      ImpalaException {
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
    byte[] decodedBytes = Base64.getDecoder().decode(encodedStats.toString());
    TPartitionStats stats = new TPartitionStats();
    JniUtil.deserializeThrift(new TCompactProtocol.Factory(), stats, decodedBytes);
    hasIncrStats.setRef(stats.isSetIntermediate_col_stats());
    return CompressionUtil.deflateCompress(decodedBytes);
  }

  /**
   * Serializes 'stats' and deflate-compresses it to bytes. Returns null if 'stats' is
   * null. Throws an exception if there is an error with serialization/compression.
   */
  public static byte[] partStatsToCompressedBytes(TPartitionStats stats)
      throws TException {
    if (stats == null) return null;
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    TSerializer serializer = new TSerializer(protocolFactory);
    byte[] serialized = CompressionUtil.deflateCompress(serializer.serialize(stats));
    return serialized;
  }

  /**
   * Serialises a TPartitionStats object to a partition. If 'partStats' is null, the
   * partition's stats are removed.
   */
  public static void partStatsToPartition(TPartitionStats partStats,
      HdfsPartition.Builder partition) throws ImpalaException {
    if (partStats == null) {
      partition.setPartitionStatsBytes(null, false);
      return;
    }

    try {
      partition.setPartitionStatsBytes(
          partStatsToCompressedBytes(partStats), partStats.isSetIntermediate_col_stats());
    } catch (TException e) {
      String debugString =
          String.format("Error saving partition stats: table %s, partition %s",
          partition.getTable().getFullName(), partition.getPartitionName());
      LOG.error(debugString, e);
      throw new ImpalaRuntimeException(debugString, e);
    }
  }

  /**
   * Converts byte[] representation of partition's stats into a chunked string form
   * appropriate to store in the HMS parameters map. Inserts these chunks into the
   * given input 'params' map. If we run into any errors deserializing partition stats,
   * 'params' map is not altered.
   */
  public static void partStatsToParams(
      HdfsPartition partition, Map<String, String> params) {
    byte[] compressedStats = partition.getPartitionStatsCompressed();
    if (compressedStats == null) return;
    // The HMS has a 4k (as of Hive 0.13, Impala 2.0) limit on the length of any parameter
    // string.  The serialised version of the partition stats is often larger than this.
    // Therefore, we naively 'chunk' the byte string into 4k pieces, and store the number
    // of pieces in a separate parameter field.
    //
    // The object itself is first serialised by Thrift, and then base-64 encoded to be a
    // valid string. This inflates its length somewhat; we may want to consider a
    // different scheme or at least understand why this scheme doesn't seem much more
    // effective than an ASCII representation.
    byte[] decompressed = CompressionUtil.deflateDecompress(compressedStats);
    if (decompressed  == null)  {
      LOG.error(
          "Error decompressing partition stats for " + partition.getPartitionName());
      return;
    }
    String base64 = new String(Base64.getEncoder().encode(decompressed));
    List<String> chunks =
      chunkStringForHms(base64, MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH);
    params.put(INCREMENTAL_STATS_NUM_CHUNKS, Integer.toString(chunks.size()));
    for (int i = 0; i < chunks.size(); ++i) {
      params.put(INCREMENTAL_STATS_CHUNK_PREFIX + i, chunks.get(i));
    }
  }

  static private List<String> chunkStringForHms(String data, int chunkLen) {
    int idx = 0;
    List<String> ret = new ArrayList<>();
    while (idx < data.length()) {
      int remaining = data.length() - idx;
      int chunkSize = (chunkLen > remaining) ? remaining : chunkLen;
      ret.add(data.substring(idx, idx + chunkSize));
      idx += chunkSize;
    }
    return ret;
  }
}
