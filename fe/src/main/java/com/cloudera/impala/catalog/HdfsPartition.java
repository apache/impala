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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.BlockLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.analysis.Expr;
import com.cloudera.impala.analysis.LiteralExpr;
import com.cloudera.impala.analysis.NullLiteral;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TExpr;
import com.cloudera.impala.thrift.THdfsFileBlock;
import com.cloudera.impala.thrift.THdfsFileDesc;
import com.cloudera.impala.thrift.THdfsPartition;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Query-relevant information for one table partition.
 */
public class HdfsPartition {
  /**
   * Metadata for a single file in this partition.
   * TODO: Do we even need this class? Just get rid of it and use the Thrift version?
   */
  static public class FileDescriptor {
    // TODO: split filePath into dir and file name and reuse the dir string to save
    // memory.
    private final THdfsFileDesc fileDescriptor_;

    public String getFilePath() { return fileDescriptor_.getPath(); }
    public long getFileLength() { return fileDescriptor_.getLength(); }
    public long getModificationTime() {
      return fileDescriptor_.getLast_modification_time();
    }
    public List<THdfsFileBlock> getFileBlocks() {
      return fileDescriptor_.getFile_blocks();
    }

    public THdfsFileDesc toThrift() { return fileDescriptor_; }

    public FileDescriptor(String filePath, long fileLength, long modificationTime) {
      Preconditions.checkNotNull(filePath);
      Preconditions.checkArgument(fileLength >= 0);
      fileDescriptor_ = new THdfsFileDesc();
      fileDescriptor_.setPath(filePath);
      fileDescriptor_.setLength(fileLength);
      fileDescriptor_.setLast_modification_time(modificationTime);
      fileDescriptor_.setCompression(
          HdfsCompression.fromFileName(filePath).toThrift());
      List<THdfsFileBlock> emptyFileBlockList = Lists.newArrayList();
      fileDescriptor_.setFile_blocks(emptyFileBlockList);
    }

    private FileDescriptor(THdfsFileDesc fileDesc) {
      this(fileDesc.path, fileDesc.length, fileDesc.last_modification_time);
      for (THdfsFileBlock block: fileDesc.getFile_blocks()) {
        fileDescriptor_.addToFile_blocks(block);
      }
    }

    public void addFileBlock(FileBlock blockMd) {
      fileDescriptor_.addToFile_blocks(blockMd.toThrift());
    }

    public static FileDescriptor fromThrift(THdfsFileDesc desc) {
      return new FileDescriptor(desc);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this).add("Path", getFilePath())
          .add("Length", getFileLength()).toString();
    }
  }

  /**
   * File Block metadata
   */
  public static class FileBlock {
    private final THdfsFileBlock fileBlock_;

    private FileBlock(THdfsFileBlock fileBlock) {
      this.fileBlock_ = fileBlock;
    }

    /**
     * Construct a FileBlock from blockLocation and populate hostPorts from
     * BlockLocation.getNames(). Does not fill diskIds.
     */
    public FileBlock(String fileName, long fileSize, BlockLocation blockLocation) {
      Preconditions.checkNotNull(blockLocation);
      fileBlock_ = new THdfsFileBlock();
      fileBlock_.setFile_name(fileName);
      fileBlock_.setFile_size(fileSize);
      fileBlock_.setOffset(blockLocation.getOffset());
      fileBlock_.setLength(blockLocation.getLength());

      // result of BlockLocation.getNames(): list of (IP:port) hosting this block
      String[] blockHostPorts;
      try {
        blockHostPorts = blockLocation.getNames();
      } catch (IOException e) {
        // this shouldn't happen, getNames() doesn't throw anything
        String errorMsg = "BlockLocation.getNames() failed:\n" + e.getMessage();
        LOG.error(errorMsg);
        throw new IllegalStateException(errorMsg);
      }

      // hostPorts[i] stores this block on diskId[i]; the BE uses this information to
      // schedule scan ranges

      // use String.intern() to reuse string
      fileBlock_.host_ports = Lists.newArrayList();
      for (int i = 0; i < blockHostPorts.length; ++i) {
        fileBlock_.host_ports.add(blockHostPorts[i].intern());
      }
    }

    public String getFileName() { return fileBlock_.getFile_name(); }
    public long getFileSize() { return fileBlock_.getFile_size(); }
    public long getOffset() { return fileBlock_.getOffset(); }
    public long getLength() { return fileBlock_.getLength(); }
    public List<String> getHostPorts() { return fileBlock_.getHost_ports(); }

    /**
     * Populates the given THdfsFileBlock's list of disk ids with the given disk id
     * values. The number of disk ids must match the number of host ports
     * set in the file block.
     */
    public static void setDiskIds(int[] diskIds, THdfsFileBlock fileBlock) {
      Preconditions.checkArgument(diskIds.length == fileBlock.getHost_ports().size());
      fileBlock.setDisk_ids(Arrays.asList(ArrayUtils.toObject(diskIds)));
    }

    /**
     * Return the disk id of the block in BlockLocation.getNames()[hostIndex]; -1 if
     * disk id is not supported.
     */
    public int getDiskId(int hostIndex) {
      if (fileBlock_.disk_ids == null) return -1;
      Preconditions.checkArgument(hostIndex >= 0);
      Preconditions.checkArgument(hostIndex < fileBlock_.getDisk_idsSize());
      return fileBlock_.getDisk_ids().get(hostIndex);
    }

    public THdfsFileBlock toThrift() { return fileBlock_; }

    public static FileBlock fromThrift(THdfsFileBlock thriftFileBlock) {
      return new FileBlock(thriftFileBlock);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("offset", fileBlock_.offset)
          .add("length", fileBlock_.length)
          .add("#disks", fileBlock_.getDisk_idsSize())
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
  private static AtomicLong partitionIdCounter = new AtomicLong();

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
  private final String location;
  private final static Logger LOG = LoggerFactory.getLogger(HdfsPartition.class);

  public HdfsStorageDescriptor getInputFormatDescriptor() {
    return fileFormatDescriptor;
  }

  /**
   * Returns the metastore.api.Partition object this HdfsPartition represents. Returns
   * null if this is the default partition, or if this belongs to a unpartitioned
   * table.
   */
  public org.apache.hadoop.hive.metastore.api.Partition getMetaStorePartition() {
    return msPartition;
  }

  /**
   * Returns the storage location (HDFS path) of this partition. Should only be called
   * for partitioned tables.
   */
  public String getLocation() { return location; }
  public long getId() { return id; }
  public HdfsTable getTable() { return table; }
  public void setNumRows(long numRows) { this.numRows = numRows; }
  public long getNumRows() { return numRows; }

  /**
   * Returns an immutable list of partition key expressions
   */
  public List<LiteralExpr> getPartitionValues() { return partitionKeyValues; }
  public List<HdfsPartition.FileDescriptor> getFileDescriptors() {
    return fileDescriptors;
  }

  private HdfsPartition(HdfsTable table,
      org.apache.hadoop.hive.metastore.api.Partition msPartition,
      List<LiteralExpr> partitionKeyValues,
      HdfsStorageDescriptor fileFormatDescriptor,
      List<HdfsPartition.FileDescriptor> fileDescriptors, long id,
      String location) {
    this.table = table;
    this.msPartition = msPartition;
    this.location = location;
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
        partitionIdCounter.getAndIncrement(), msPartition != null ?
            msPartition.getSd().getLocation() : null);
  }

  public static HdfsPartition defaultPartition(
      HdfsTable table, HdfsStorageDescriptor storageDescriptor) {
    List<LiteralExpr> emptyExprList = Lists.newArrayList();
    List<FileDescriptor> emptyFileDescriptorList = Lists.newArrayList();
    return new HdfsPartition(table, null, emptyExprList,
        storageDescriptor, emptyFileDescriptorList,
        ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID, null);
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

  public static HdfsPartition fromThrift(HdfsTable table,
      long id, THdfsPartition thriftPartition) {
    HdfsStorageDescriptor storageDesc = new HdfsStorageDescriptor(table.getName(),
        HdfsFileFormat.fromThrift(thriftPartition.getFileFormat()),
        (char) thriftPartition.lineDelim,
        (char) thriftPartition.fieldDelim,
        (char) thriftPartition.collectionDelim,
        (char) thriftPartition.mapKeyDelim,
        (char) thriftPartition.escapeChar,
        '"', // TODO: We should probably add quoteChar to THdfsPartition.
        (int) thriftPartition.blockSize,
        thriftPartition.compression);

    List<LiteralExpr> literalExpr = Lists.newArrayList();
    if (id != ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID) {
      List<Column> clusterCols = Lists.newArrayList();
      for (int i = 0; i < table.getNumClusteringCols(); ++i) {
        clusterCols.add(table.getColumns().get(i));
      }

      List<com.cloudera.impala.thrift.TExprNode> exprNodes = Lists.newArrayList();
      for (com.cloudera.impala.thrift.TExpr expr: thriftPartition.getPartitionKeyExprs()) {
        for (com.cloudera.impala.thrift.TExprNode node: expr.getNodes()) {
          exprNodes.add(node);
        }
      }
      Preconditions.checkState(clusterCols.size() == exprNodes.size(),
          String.format("Number of partition columns (%d) does not match number " +
              "of partition key expressions (%d)",
              clusterCols.size(), exprNodes.size()));

      for (int i = 0; i < exprNodes.size(); ++i) {
        literalExpr.add(TExprNodeToLiteralExpr(
            exprNodes.get(i), clusterCols.get(i).getType()));
      }
    }

    List<HdfsPartition.FileDescriptor> fileDescriptors = Lists.newArrayList();
    if (thriftPartition.isSetFile_desc()) {
      for (THdfsFileDesc desc: thriftPartition.getFile_desc()) {
        fileDescriptors.add(HdfsPartition.FileDescriptor.fromThrift(desc));
      }
    }
    return new HdfsPartition(table, null, literalExpr, storageDesc, fileDescriptors, id,
        thriftPartition.getLocation());
  }

  private static LiteralExpr TExprNodeToLiteralExpr(
      com.cloudera.impala.thrift.TExprNode exprNode, PrimitiveType primitiveType) {
    try {
      switch (exprNode.node_type) {
        case FLOAT_LITERAL:
          return LiteralExpr.create(Double.toString(exprNode.float_literal.value),
              primitiveType);
        case INT_LITERAL:
          return LiteralExpr.create(Long.toString(exprNode.int_literal.value),
              primitiveType);
        case STRING_LITERAL:
          return LiteralExpr.create(exprNode.string_literal.value, primitiveType);
        case NULL_LITERAL:
          return new NullLiteral();
        default:
          throw new IllegalStateException("Unsupported partition key type: " +
              exprNode.node_type);
      }
    } catch (Exception e) {
      throw new IllegalStateException("Error creating LiteralExpr: ", e);
    }
  }

  public THdfsPartition toThrift(boolean includeFileDescriptorMetadata) {
    List<TExpr> thriftExprs = Expr.treesToThrift(getPartitionValues());

    THdfsPartition thriftHdfsPart =
        new THdfsPartition((byte)fileFormatDescriptor.getLineDelim(),
        (byte)fileFormatDescriptor.getFieldDelim(),
        (byte)fileFormatDescriptor.getCollectionDelim(),
        (byte)fileFormatDescriptor.getMapKeyDelim(),
        (byte)fileFormatDescriptor.getEscapeChar(),
        fileFormatDescriptor.getFileFormat().toThrift(), thriftExprs,
        fileFormatDescriptor.getBlockSize(), fileFormatDescriptor.getCompression());
    thriftHdfsPart.setLocation(location);
    if (includeFileDescriptorMetadata) {
      // Add block location information
      for (FileDescriptor fd: fileDescriptors) {
        thriftHdfsPart.addToFile_desc(fd.toThrift());
      }
    }
    return thriftHdfsPart;
  }
}
