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

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.THdfsPartition;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Query-relevant information for one table partition. Not thread safe due to a static
 * counter that's incremented for every time the constructor is called.
 */
public class HdfsPartition {
  /**
   * Metadata for a single file in this partition
   */
  static public class FileDescriptor {
    // TODO: split filePath into dir and file name and reuse the dir string to save
    // memory.
    private final String filePath;
    private final long fileLength;
    private final HdfsCompression fileCompression;
    private final long modificationTime;
    private final List<FileBlock> fileBlocks;

    public String getFilePath() { return filePath; }
    public long getFileLength() { return fileLength; }
    public long getModificationTime() { return modificationTime; }
    public HdfsCompression getFileCompression() { return fileCompression; }
    public List<FileBlock> getFileBlocks() { return fileBlocks; }

    public FileDescriptor(String filePath, long fileLength, long modificationTime) {
      Preconditions.checkNotNull(filePath);
      Preconditions.checkArgument(fileLength >= 0);
      this.filePath = filePath;
      this.fileLength = fileLength;
      this.modificationTime = modificationTime;
      fileCompression = HdfsCompression.fromFileName(filePath);
      fileBlocks = Lists.newArrayList();
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("Path", filePath)
          .add("Length", fileLength).toString();
    }

    public void addFileBlock(FileBlock blockMd) {
      fileBlocks.add(blockMd);
    }
  }

  /**
   * File Block metadata
   */
  public static class FileBlock {
    private final String fileName;
    private final long fileSize; // total size of the file holding the block, in bytes
    private final long offset;
    private final long length;

    // result of BlockLocation.getNames(): list of (IP:port) hosting this block
    private final String[] hostPorts;

    // hostPorts[i] stores this block on diskId[i]; the BE uses this information to
    // schedule scan ranges
    private int[] diskIds;

    /**
     * Construct a FileBlock from blockLocation and populate hostPorts from
     * BlockLocation.getNames(). Does not fill diskIds.
     */
    public FileBlock(String fileName, long fileSize, BlockLocation blockLocation) {
      Preconditions.checkNotNull(blockLocation);
      this.fileName = fileName;
      this.fileSize = fileSize;
      this.offset = blockLocation.getOffset();
      this.length = blockLocation.getLength();

      String[] blockHostPorts;
      try {
        blockHostPorts = blockLocation.getNames();
      } catch (IOException e) {
        // this shouldn't happen, getNames() doesn't throw anything
        String errorMsg = "BlockLocation.getNames() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

      // use String.intern() to reuse string
      hostPorts = new String[blockHostPorts.length];
      for (int i = 0; i < blockHostPorts.length; ++i) {
        hostPorts[i] = blockHostPorts[i].intern();
      }
    }

    public String getFileName() { return fileName; }
    public long getFileSize() { return fileSize; }
    public long getOffset() { return offset; }
    public long getLength() { return length; }
    public String[] getHostPorts() { return hostPorts; }

    public void setDiskIds(int[] diskIds) {
      Preconditions.checkArgument(diskIds.length == hostPorts.length);
      this.diskIds = diskIds;
    }

    /**
     * Return the disk id of the block in BlockLocation.getNames()[hostIndex]; -1 if
     * disk id is not supported.
     */
    public int getDiskId(int hostIndex) {
      if (diskIds == null) return -1;
      Preconditions.checkArgument(hostIndex >= 0);
      Preconditions.checkArgument(hostIndex < diskIds.length);
      return diskIds[hostIndex];
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("offset", offset)
          .add("length", length)
          .add("#disks", diskIds.length)
          .toString();
    }
  }

  private final HdfsTable table;
  private final List<LiteralExpr> partitionKeyValues;

  // estimated number of rows in partition; -1: unknown
  private long numRows = -1;

  // partition-specific stats for each column
  // TODO: fill this
  private final Map<Column, ColumnStats> columnStats = Maps.newHashMap();

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

  private final org.apache.hadoop.hive.metastore.api.Partition msPartition;

  private final List<FileDescriptor> fileDescriptors;

  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartition.class);

  public HdfsStorageDescriptor getInputFormatDescriptor() { return fileFormatDescriptor; }

  /**
   * Returns the metastore.api.Partition object this HdfsPartition represents. Returns
   * null if this is the default partition.
   */
  public org.apache.hadoop.hive.metastore.api.Partition getMetaStorePartition() {
    return msPartition;
  }

  /*
   * Returns the storage location (HDFS path) of this partition.
   */
  public String getLocation() {
    return msPartition.getSd().getLocation();
  }

  public long getId() { return id; }

  public HdfsTable getTable() { return table; }

  public void setNumRows(long numRows) {
    this.numRows = numRows;
  }

  public long getNumRows() { return numRows; }

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

  private HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, long id) {
    this.table = table;
    this.msPartition = msPartition;
    this.partitionKeyValues = ImmutableList.copyOf(partitionKeyValues);
    this.fileDescriptors = ImmutableList.copyOf(fileDescriptors);
    this.fileFormatDescriptor = fileFormatDescriptor;
    this.id = id;
    // TODO: instead of raising an exception, we should consider marking this partition
    // invalid and moving on, so that table loading won't fail and user can query other
    // partitions.
    for (FileDescriptor fileDescriptor: fileDescriptors) {
      String result = checkFileCompressionTypeSupported(fileDescriptor.getFilePath());
      if (!result.isEmpty()) {
        throw new RuntimeException(result);
      }
    }
  }

  public HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors) {
    this(table, msPartition, partitionKeyValues, fileFormatDescriptor, fileDescriptors,
        partitionIdCounter++);
  }

  public static HdfsPartition defaultPartition(
      HdfsTable table, HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = Lists.newArrayList();
    List<FileDescriptor> emptyFileDescriptorList = Lists.newArrayList();
    HdfsPartition partition = new HdfsPartition(table, null, emptyExprList,
        storageDescriptor, emptyFileDescriptorList,
        ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID);
    return partition;
  }

  /*
   * Checks whether a file is supported in Impala based on the file extension.
   * Returns an empty string if the file format is supported, otherwise a string with
   * details on the incompatibility is returned.
   * Impala only supports .lzo on text files for partitions that have been declared in
   * the metastore as TEXT_LZO. For now, raise an error on any other type.
   */
  public String checkFileCompressionTypeSupported(String fileName) {
    // Check to see if the file has a compression suffix.
    // Impala only supports .lzo on text files that have been declared in the metastore
    // as TEXT_LZO. For now, raise an error on any other type.
    HdfsCompression compressionType = HdfsCompression.fromFileName(fileName);
    if (compressionType == HdfsCompression.LZO_INDEX) {
      // Index files are read by the LZO scanner directly.
      return "";
    }

    HdfsStorageDescriptor sd = getInputFormatDescriptor();
    if (compressionType == HdfsCompression.LZO) {
      if (sd.getFileFormat() != HdfsFileFormat.LZO_TEXT) {
        return "Compressed file not supported without compression input format: " +
            fileName;
      }
    } else if (sd.getFileFormat() == HdfsFileFormat.LZO_TEXT) {
      return "Expected file with .lzo suffix: " + fileName;
    } else if (sd.getFileFormat() == HdfsFileFormat.TEXT
               && compressionType != HdfsCompression.NONE) {
      return "Compressed text files are not supported: " + fileName;
    }
    return "";
  }

  /**
   * Return the size (in bytes) of all the files inside this partition
   */
  public long getSize() {
    long result = 0;
    for (HdfsPartition.FileDescriptor fileDescriptor: fileDescriptors) {
      result += fileDescriptor.getFileLength();
    }
    return result;
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
