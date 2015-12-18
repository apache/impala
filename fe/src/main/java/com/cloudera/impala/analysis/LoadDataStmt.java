// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.analysis;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.ImpalaInternalServiceConstants;
import com.cloudera.impala.thrift.TLoadDataReq;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.TAccessLevelUtil;
import com.cloudera.impala.util.FsPermissionChecker;
import com.google.common.base.Preconditions;

/*
 * Represents a LOAD DATA statement for moving data into an existing table:
 * LOAD DATA INPATH 'filepath' [OVERWRITE] INTO TABLE <table name>
 * [PARTITION (partcol1=val1, partcol2=val2 ...)]
 *
 * The LOAD DATA operation supports loading (moving) a single file or all files in a
 * given source directory to a table or partition location. If OVERWRITE is true, all
 * exiting files in the destination will be removed before moving the new data in.
 * If OVERWRITE is false, existing files will be preserved. If there are any file name
 * conflicts, the new files will be uniquified by inserting a UUID into the file name
 * (preserving the extension).
 * Loading hidden files is not supported and any hidden files in the source or
 * destination are preserved, even if OVERWRITE is true.
 */
public class LoadDataStmt extends StatementBase {
  private final TableName tableName_;
  private final HdfsUri sourceDataPath_;
  private final PartitionSpec partitionSpec_;
  private final boolean overwrite_;

  // Set during analysis
  private String dbName_;

  public LoadDataStmt(TableName tableName, HdfsUri sourceDataPath, boolean overwrite,
      PartitionSpec partitionSpec) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(sourceDataPath);
    this.tableName_ = tableName;
    this.sourceDataPath_ = sourceDataPath;
    this.overwrite_ = overwrite;
    this.partitionSpec_ = partitionSpec;
  }

  public String getTbl() {
    return tableName_.getTbl();
  }

  public String getDb() {
    Preconditions.checkNotNull(dbName_);
    return dbName_;
  }

  /*
   * Print SQL syntax corresponding to this node.
   * @see com.cloudera.impala.parser.ParseNode#toSql()
   */
  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("LOAD DATA INPATH '");
    sb.append(sourceDataPath_ + "' ");
    if (overwrite_) sb.append("OVERWRITE ");
    sb.append("INTO TABLE " + tableName_.toString());
    if (partitionSpec_ != null) sb.append(" " + partitionSpec_.toSql());
    return sb.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    dbName_ = analyzer.getTargetDbName(tableName_);
    Table table = analyzer.getTable(tableName_, Privilege.INSERT);
    if (!(table instanceof HdfsTable)) {
      throw new AnalysisException("LOAD DATA only supported for HDFS tables: " +
          dbName_ + "." + getTbl());
    }

    // Analyze the partition spec, if one was specified.
    if (partitionSpec_ != null) {
      partitionSpec_.setTableName(tableName_);
      partitionSpec_.setPartitionShouldExist();
      partitionSpec_.setPrivilegeRequirement(Privilege.INSERT);
      partitionSpec_.analyze(analyzer);
    } else {
      if (table.getMetaStoreTable().getPartitionKeysSize() > 0) {
        throw new AnalysisException("Table is partitioned but no partition spec was " +
            "specified: " + dbName_ + "." + getTbl());
      }
    }
    analyzePaths(analyzer, (HdfsTable) table);
  }

  /**
   * Check to see if Impala has the necessary permissions to access the source and dest
   * paths for this LOAD statement (which maps onto a sequence of file move operations,
   * with the requisite permission requirements), and check to see if all files to be
   * moved are in format that Impala understands. Errors are raised as AnalysisExceptions.
   */
  private void analyzePaths(Analyzer analyzer, HdfsTable hdfsTable)
      throws AnalysisException {
    // The user must have permission to access the source location. Since the files will
    // be moved from this location, the user needs to have all permission.
    sourceDataPath_.analyze(analyzer, Privilege.ALL);

    // Catch all exceptions thrown by accessing files, and rethrow as AnalysisExceptions.
    try {
      Path source = sourceDataPath_.getPath();
      FileSystem fs = source.getFileSystem(FileSystemUtil.getConfiguration());
      if (!(fs instanceof DistributedFileSystem)) {
        throw new AnalysisException(String.format("INPATH location '%s' " +
            "must point to an HDFS filesystem.", sourceDataPath_));
      }
      if (!fs.exists(source)) {
        throw new AnalysisException(String.format(
            "INPATH location '%s' does not exist.", sourceDataPath_));
      }

      // If the source file is a directory, we must be able to read from and write to
      // it. If the source file is a file, we must be able to read from it, and write to
      // its parent directory (in order to delete the file as part of the move operation).
      FsPermissionChecker checker = FsPermissionChecker.getInstance();

      if (fs.isDirectory(source)) {
        if (FileSystemUtil.getTotalNumVisibleFiles(source) == 0) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' contains no visible files.", sourceDataPath_));
        }
        if (FileSystemUtil.containsVisibleSubdirectory(source)) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' cannot contain non-hidden subdirectories.", sourceDataPath_));
        }
        if (!checker.getPermissions(fs, source).checkPermissions(
            FsAction.READ_WRITE)) {
          throw new AnalysisException(String.format("Unable to LOAD DATA from %s " +
              "because Impala does not have READ and WRITE permissions on this directory",
              source));
        }
      } else {
        // INPATH names a file.
        if (FileSystemUtil.isHiddenFile(source.getName())) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' points to a hidden file.", source));
        }

        if (!checker.getPermissions(fs, source.getParent()).checkPermissions(
            FsAction.WRITE)) {
          throw new AnalysisException(String.format("Unable to LOAD DATA from %s " +
              "because Impala does not have WRITE permissions on its parent " +
              "directory %s", source, source.getParent()));
        }

        if (!checker.getPermissions(fs, source).checkPermissions(
            FsAction.READ)) {
          throw new AnalysisException(String.format("Unable to LOAD DATA from %s " +
              "because Impala does not have READ permissions on this file", source));
        }
      }

      String noWriteAccessErrorMsg = String.format("Unable to LOAD DATA into " +
          "target table (%s) because Impala does not have WRITE access to HDFS " +
          "location: ", hdfsTable.getFullName());

      HdfsPartition partition;
      String location;
      if (partitionSpec_ != null) {
        partition = hdfsTable.getPartition(partitionSpec_.getPartitionSpecKeyValues());
        location = partition.getLocation();
        if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
          throw new AnalysisException(noWriteAccessErrorMsg + partition.getLocation());
        }
      } else {
        // "default" partition
        partition = hdfsTable.getPartitionMap().get(
            ImpalaInternalServiceConstants.DEFAULT_PARTITION_ID);
        location = hdfsTable.getLocation();
        if (!hdfsTable.hasWriteAccess()) {
          throw new AnalysisException(noWriteAccessErrorMsg + hdfsTable.getLocation());
        }
      }
      Preconditions.checkNotNull(partition);

      // Until Frontend.loadTableData() can handle cross-filesystem and filesystems
      // that aren't HDFS, require that source and dest are on the same HDFS.
      if (!FileSystemUtil.isPathOnFileSystem(new Path(location), fs)) {
        throw new AnalysisException(String.format(
            "Unable to LOAD DATA into target table (%s) because source path (%s) and " +
            "destination %s (%s) are on different filesystems.",
            hdfsTable.getFullName(),
            source, partitionSpec_ == null ? "table" : "partition",
            partition.getLocation()));
      }
      // Verify the files being loaded are supported.
      for (FileStatus fStatus: fs.listStatus(source)) {
        if (fs.isDirectory(fStatus.getPath())) continue;
        StringBuilder errorMsg = new StringBuilder();
        HdfsFileFormat fileFormat = partition.getInputFormatDescriptor().getFileFormat();
        if (!fileFormat.isFileCompressionTypeSupported(fStatus.getPath().toString(),
          errorMsg)) {
          throw new AnalysisException(errorMsg.toString());
        }
      }
    } catch (FileNotFoundException e) {
      throw new AnalysisException("File not found: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new AnalysisException("Error accessing filesystem: " + e.getMessage(), e);
    }
  }

  public TLoadDataReq toThrift() {
    TLoadDataReq loadDataReq = new TLoadDataReq();
    loadDataReq.setTable_name(new TTableName(getDb(), getTbl()));
    loadDataReq.setSource_path(sourceDataPath_.toString());
    loadDataReq.setOverwrite(overwrite_);
    if (partitionSpec_ != null) {
      loadDataReq.setPartition_spec(partitionSpec_.toThrift());
    }
    return loadDataReq;
  }
}
