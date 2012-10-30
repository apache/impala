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

package com.cloudera.impala.catalog;

import java.util.List;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.thrift.Constants;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.THdfsPartition;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 * Query-relevant information for one table partition. Not thread safe due to a static
 * counter that's incremented for every time the constructor is called.
 */
public class HdfsPartition {
  /**
   * Metadata for a single file in this partition - the full path and the length of the
   * file.
   */
  static public class FileDescriptor {
    private final String filePath;
    private final long fileLength;

    public String getFilePath() { return filePath; }
    public long getFileLength() { return fileLength; }

    public FileDescriptor(String filePath, long fileLength) {
      Preconditions.checkNotNull(filePath);
      Preconditions.checkArgument(fileLength >= 0);
      this.filePath = filePath;
      this.fileLength = fileLength;
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("Path", filePath)
          .add("Length", fileLength).toString();
    }
  }

  private final List<LiteralExpr> partitionKeyValues;

  private static long partitionIdCounter = 0;

  // A unique ID for each partition, used to identify a partition in the thrift
  // representation of a table.
  private final long id;

  /*
   * Note: Although you can write multiple formats to a single partition (by changing
   * the format before each write), Hive won't let you read that data and neither should
   * we. We should therefore treat mixing formats inside one partition as user error.
   * It's easy to add per-file metadata to FileDescriptor if this changes.
   */
  private final HdfsStorageDescriptor fileFormatDescriptor;

  private final List<HdfsPartition.FileDescriptor> fileDescriptors;

  public HdfsStorageDescriptor getInputFormatDescriptor() { return fileFormatDescriptor; }

  public long getId() { return id; }

  /**
   * Returns an immutable list of partition key expressions
   */
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues; }

  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors;
  }

  public List<LiteralExpr> getPartitionKeyValues() {
    return partitionKeyValues;
  }

  private HdfsPartition(List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, long id) {
    this.partitionKeyValues = ImmutableList.copyOf(partitionKeyValues);
    this.fileDescriptors = ImmutableList.copyOf(fileDescriptors);
    this.fileFormatDescriptor = fileFormatDescriptor;
    this.id = id;
  }

  public HdfsPartition(List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors) {
    this(partitionKeyValues, fileFormatDescriptor, fileDescriptors,
        partitionIdCounter++);
  }

  public static HdfsPartition defaultPartition(
      HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = Lists.newArrayList();
    List<FileDescriptor> emptyFileDescriptorList = Lists.newArrayList();
    HdfsPartition partition = new HdfsPartition(emptyExprList,
        storageDescriptor, emptyFileDescriptorList, Constants.DEFAULT_PARTITION_ID);
    return partition;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("fileDescriptors", fileDescriptors)
      .toString();
  }

  public THdfsPartition toThrift() {
    List<TExpr> thriftExprs =
      Expr.treesToThrift(getPartitionValues());

    return new THdfsPartition((byte)fileFormatDescriptor.getLineDelim(),
        (byte)fileFormatDescriptor.getFieldDelim(),
        (byte)fileFormatDescriptor.getCollectionDelim(),
        (byte)fileFormatDescriptor.getMapKeyDelim(),
        (byte)fileFormatDescriptor.getEscapeChar(),
        fileFormatDescriptor.getFileFormat().toThrift(), thriftExprs,
        fileFormatDescriptor.getBlockSize(), fileFormatDescriptor.getCompression());
  }
}