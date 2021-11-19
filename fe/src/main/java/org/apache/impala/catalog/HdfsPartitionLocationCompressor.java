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

import java.util.List;

import org.apache.impala.common.Pair;
import org.apache.impala.thrift.THdfsPartitionLocation;
import org.apache.impala.util.ListMap;
import com.google.common.base.Preconditions;

/**
 * Utility class for storing HdfsPartition locations in a comrpessed format.  Each
 * instance of this class is owned by a single HdfsTable instance.
 *
 * This class is not thread-safe by itself since it is only modified when the lock on an
 * HdfsTable object is held.
 *
 * TODO: Generalize this to compress other sets of Strings that are likely to share common
 * prefixes, like table locations.
 *
 */
public class HdfsPartitionLocationCompressor {
  int numClusteringColumns_;

  // A bi-directional map between partition location prefixes and their compressed
  // representation, an int.
  final private ListMap<String> prefixMap_ = new ListMap<String>();

  public HdfsPartitionLocationCompressor(int numClusteringColumns) {
    numClusteringColumns_ = numClusteringColumns;
  }

  // Construct an HdfsPartitionLocationCompressor with a pre-filled bidirectional map
  // (indexToPrefix_, prefixToIndex_).
  public HdfsPartitionLocationCompressor(
      int numClusteringColumns, List<String> prefixes) {
    numClusteringColumns_ = numClusteringColumns;
    prefixMap_.populate(prefixes);
  }

  public void setClusteringColumns(int numClusteringColumns) {
    numClusteringColumns_ = numClusteringColumns;
  }

  public List<String> getPrefixes() {
    return prefixMap_.getList();
  }

  // One direction of the map: returns the prefix associated with an index, or "" is the
  // index is -1. Indexes less than -1 or greater than indexToPrefix_.size()-1 are invalid
  // and casue and IllegalArgumentException to be thrown.
  private String indexToPrefix(int i) {
    // Uncompressed location are represented by -1:
    if (i == -1) return "";
    Preconditions.checkElementIndex(i, prefixMap_.size());
    return prefixMap_.getEntry(i);
  }

  // Compress a location prefix, adding it to the bidirectional map (indexToPrefix_,
  // prefixToIndex_) if it is not already present.
  private int prefixToIndex(String s) {
    return prefixMap_.getOrAddIndex(s);
  }

  // A surrogate for THdfsPartitionLocation, which represents a partition's location
  // relative to its parent table's list of partition prefixes.
  public class Location {
    // 'prefix_index_' represents the portion of the partition's location that comes before
    // the last N directories, where N is the number of partitioning columns.
    // 'prefix_index_' is an index into
    // HdfsPartitionLocationCompressor.this.indexToPrefix_. 'suffix_' is the rest of the
    // partition location.
    //
    // TODO: Since each partition stores the literal values for the partitioning columns,
    // we could also elide the column names and values from suffix_ when a partition is in
    // the canonical location "/partitioning_column_name_1=value_1/..."
    private final int prefix_index_;
    private final String suffix_;

    public Location(String location) {
      Preconditions.checkNotNull(location);
      Pair<String,String> locationParts = decompose(location);
      prefix_index_ =
          HdfsPartitionLocationCompressor.this.prefixToIndex(locationParts.first);
      suffix_ = locationParts.second;
    }

    public Location(THdfsPartitionLocation thrift) {
      Preconditions.checkNotNull(thrift);
      prefix_index_ = thrift.prefix_index;
      suffix_ = thrift.getSuffix();
    }

    public THdfsPartitionLocation toThrift() {
      return new THdfsPartitionLocation(prefix_index_, suffix_);
    }

    @Override
    public String toString() {
      return HdfsPartitionLocationCompressor.this.indexToPrefix(prefix_index_) + suffix_;
    }

    @Override
    public int hashCode() { return toString().hashCode(); }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof Location) && (toString() == obj.toString());
    }

    // Decompose a location string by removing its last N directories, where N is the
    // number of clustering columns. The result is a Pair<String,String> where the first
    // String is the prefix and the second is the suffix. (In orther words, their
    // concatenation equals the input.) If the input does not have at least N '/'
    // characters, the prefix is empty and the suffix is the entire input.
    private Pair<String,String> decompose(String s) {
      Preconditions.checkNotNull(s);
      int numClusteringColumns =
          HdfsPartitionLocationCompressor.this.numClusteringColumns_;
      if (numClusteringColumns == 0) return new Pair<String,String>(s, "");
      // Iterate backwards over the input until we have passed 'numClusteringColumns'
      // directories. What is left is the prefix.
      int i = s.length() - 1;
      // If the string ends in '/', iterating past it does not pass a clustering column.
      if (i >= 0 && s.charAt(i) == '/') --i;
      for (; numClusteringColumns > 0 && i >= 0; --i) {
        if (s.charAt(i) == '/') --numClusteringColumns;
      }
      // If we successfully removed all the partition directories, s.charAt(i+1) is '/'
      // and we can include it in the prefix.
      if (0 == numClusteringColumns) ++i;
      return new Pair<String,String>(s.substring(0, i + 1), s.substring(i + 1));
    }
  }
}
