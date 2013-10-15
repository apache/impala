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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.HdfsPartition;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.cloudera.impala.thrift.TLoadDataReq;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.TAccessLevelUtil;
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
  private final TableName tableName;
  private final HdfsURI sourceDataPath;
  private final PartitionSpec partitionSpec;
  private final boolean overwrite;

  // Set during analysis
  private String dbName;

  public LoadDataStmt(TableName tableName, HdfsURI sourceDataPath, boolean overwrite,
      PartitionSpec partitionSpec) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(sourceDataPath);
    this.tableName = tableName;
    this.sourceDataPath = sourceDataPath;
    this.overwrite = overwrite;
    this.partitionSpec = partitionSpec;
  }

  public String getTbl() {
    return tableName.getTbl();
  }

  public String getDb() {
    Preconditions.checkNotNull(dbName);
    return dbName;
  }

  /*
   * Print SQL syntax corresponding to this node.
   * @see com.cloudera.impala.parser.ParseNode#toSql()
   */
  @Override
  public String toSql() {
    StringBuilder sb = new StringBuilder("LOAD DATA INPATH '");
    sb.append(sourceDataPath + "' ");
    if (overwrite) {
      sb.append("OVERWRITE ");
    }
    sb.append("INTO TABLE " + tableName.toString());
    if (partitionSpec != null) {
      sb.append(" " + partitionSpec.toSql());
    }
    return sb.toString();
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    dbName = analyzer.getTargetDbName(tableName);
    Table table = analyzer.getTable(tableName, Privilege.INSERT);
    if (!(table instanceof HdfsTable)) {
      throw new AnalysisException("LOAD DATA only supported for HDFS tables: " +
          dbName + "." + getTbl());
    }

    // Analyze the partition spec, if one was specified.
    if (partitionSpec != null) {
      partitionSpec.setTableName(tableName);
      partitionSpec.setPartitionShouldExist();
      partitionSpec.setPrivilegeRequirement(Privilege.INSERT);
      partitionSpec.analyze(analyzer);
    } else {
      if (table.getMetaStoreTable().getPartitionKeysSize() > 0) {
        throw new AnalysisException("Table is partitioned but no partition spec was " +
            "specified: " + dbName + "." + getTbl());
      }
    }
    analyzePaths(analyzer, (HdfsTable) table);
  }

  private void analyzePaths(Analyzer analyzer, HdfsTable hdfsTable)
      throws AnalysisException, AuthorizationException {
    // The user must have permission to access the source location. Since the files will
    // be moved from this location, the user needs to have all permission.
    sourceDataPath.analyze(analyzer, Privilege.ALL);

    try {
      Path source = sourceDataPath.getPath();
      FileSystem fs = source.getFileSystem(FileSystemUtil.getConfiguration());
      DistributedFileSystem dfs = (DistributedFileSystem) fs;
      if (!dfs.exists(source)) {
        throw new AnalysisException(String.format(
            "INPATH location '%s' does not exist.", sourceDataPath));
      }

      if (dfs.isDirectory(source)) {
        if (FileSystemUtil.getTotalNumVisibleFiles(source) == 0) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' contains no visible files.", sourceDataPath));
        }
        if (FileSystemUtil.containsSubdirectory(source)) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' cannot contain subdirectories.", sourceDataPath));
        }
      } else { // INPATH points to a file.
        if (FileSystemUtil.isHiddenFile(source.getName())) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' points to a hidden file.", source));
        }
      }


      String noWriteAccessErrorMsg = String.format("Unable to LOAD DATA into " +
          "target table (%s) because Impala does not have WRITE access to HDFS " +
          "location: ", hdfsTable.getFullName());

      HdfsPartition partition;
      if (partitionSpec != null) {
        partition = hdfsTable.getPartition(partitionSpec.getPartitionSpecKeyValues());
        if (!TAccessLevelUtil.impliesWriteAccess(partition.getAccessLevel())) {
          throw new AnalysisException(noWriteAccessErrorMsg + partition.getLocation());
        }
      } else {
        // "default" partition
        partition = hdfsTable.getPartitions().get(0);
        if (!hdfsTable.hasWriteAccess()) {
          throw new AnalysisException(noWriteAccessErrorMsg + hdfsTable.getLocation());
        }
      }
      Preconditions.checkNotNull(partition);

      // Verify the files being loaded are supported.
      for (FileStatus fStatus: fs.listStatus(source)) {
        if (fs.isDirectory(fStatus.getPath())) continue;
        String result = partition.checkFileCompressionTypeSupported(
            fStatus.getPath().toString());
        if (!result.isEmpty()) {
          throw new AnalysisException(result);
        }
      }
    } catch (FileNotFoundException e) {
      throw new AnalysisException("File not found: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new AnalysisException("Error accessing file system: " + e.getMessage(), e);
    }
  }

  public TLoadDataReq toThrift() {
    TLoadDataReq loadDataReq = new TLoadDataReq();
    loadDataReq.setTable_name(new TTableName(getDb(), getTbl()));
    loadDataReq.setSource_path(sourceDataPath.toString());
    loadDataReq.setOverwrite(overwrite);
    if (partitionSpec != null) {
      loadDataReq.setPartition_spec(partitionSpec.toThrift());
    }
    return loadDataReq;
  }
}
