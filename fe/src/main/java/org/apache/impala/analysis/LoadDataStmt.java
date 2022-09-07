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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.TLoadDataReq;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.util.FsPermissionChecker;
import org.apache.orc.OrcFile;
import org.apache.parquet.hadoop.ParquetFileWriter;

/**
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
 *
 * Notes on Iceberg data loading:
 * Iceberg files have specific field ids, therefore native parquet tables cannot be added
 * to the data directory with only moving the files. To rewrite the files with the
 * necessary metadata the LOAD DATA operation will be executed with 3 sub-queries:
 *  1. CREATE temporary table
 *  2. INSERT INTO from the temporary table to the target table
 *  3. DROP temporary table
 */
public class LoadDataStmt extends StatementBase {
  private final TableName tableName_;
  private final HdfsUri sourceDataPath_;
  private final PartitionSpec partitionSpec_;
  private final boolean overwrite_;

  // Set during analysis
  private String dbName_;
  private FeTable table_;
  private String createTmpTblQuery_;
  private String insertTblQuery_;
  private String dropTmpTblQuery_;

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
   * @see org.apache.impala.parser.ParseNode#toSql()
   */
  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("LOAD DATA INPATH '");
    sb.append(sourceDataPath_ + "' ");
    if (overwrite_) sb.append("OVERWRITE ");
    sb.append("INTO TABLE " + tableName_.toString());
    if (partitionSpec_ != null) sb.append(" " + partitionSpec_.toSql(options));
    return sb.toString();
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    dbName_ = analyzer.getTargetDbName(tableName_);
    table_ = analyzer.getTable(tableName_, Privilege.INSERT);
    if (!(table_ instanceof FeFsTable)) {
      throw new AnalysisException("LOAD DATA only supported for HDFS tables: " +
          dbName_ + "." + getTbl());
    }
    analyzer.checkTableCapability(table_, Analyzer.OperationType.WRITE);
    analyzer.ensureTableNotTransactional(table_, "LOAD DATA");

    // Analyze the partition spec, if one was specified.
    if (partitionSpec_ != null) {
      if (table_ instanceof FeIcebergTable) {
        throw new AnalysisException("PARTITION clause is not supported for Iceberg " +
            "tables.");
      } else {
        partitionSpec_.setTableName(tableName_);
        partitionSpec_.setPartitionShouldExist();
        partitionSpec_.setPrivilegeRequirement(Privilege.INSERT);
        partitionSpec_.analyze(analyzer);
      }
    } else {
      if (table_.getMetaStoreTable().getPartitionKeysSize() > 0) {
        throw new AnalysisException("Table is partitioned but no partition spec was " +
            "specified: " + dbName_ + "." + getTbl());
      }
    }
    analyzePaths(analyzer, (FeFsTable) table_);
    if (table_ instanceof FeIcebergTable) {
      analyzeLoadIntoIcebergTable();
    }
  }

  /**
   * Check to see if Impala has the necessary permissions to access the source and dest
   * paths for this LOAD statement (which maps onto a sequence of file move operations,
   * with the requisite permission requirements), and check to see if all files to be
   * moved are in format that Impala understands. Errors are raised as AnalysisExceptions.
   *
   * We don't check permissions for the S3AFileSystem and the AdlFileSystem due to
   * limitations with thier getAclStatus() API. (see HADOOP-13892 and HADOOP-14437)
   */
  private void analyzePaths(Analyzer analyzer, FeFsTable table)
      throws AnalysisException {
    // The user must have permission to access the source location. Since the files will
    // be moved from this location, the user needs to have all permission.
    sourceDataPath_.analyze(analyzer, Privilege.ALL);

    // Catch all exceptions thrown by accessing files, and rethrow as AnalysisExceptions.
    try {
      Path source = sourceDataPath_.getPath();
      FileSystem fs = source.getFileSystem(FileSystemUtil.getConfiguration());
      if (!FileSystemUtil.isValidLoadDataInpath(fs)) {
        throw new AnalysisException(String.format("INPATH location '%s' "
                + "must point to one of the supported filesystem URI scheme (%s).",
            sourceDataPath_, FileSystemUtil.getValidLoadDataInpathSchemes()));
      }
      if (!fs.exists(source)) {
        throw new AnalysisException(String.format(
            "INPATH location '%s' does not exist.", sourceDataPath_));
      }

      // If the source file is a directory, we must be able to read from and write to
      // it. If the source file is a file, we must be able to read from it, and write to
      // its parent directory (in order to delete the file as part of the move operation).
      FsPermissionChecker checker = FsPermissionChecker.getInstance();
      // TODO: Disable permission checking for S3A as well (HADOOP-13892)
      boolean shouldCheckPerms =
          FileSystemUtil.FsType.getFsType(fs.getScheme()) != FileSystemUtil.FsType.ADLS;
      boolean authzEnabled = analyzer.isAuthzEnabled();

      if (fs.isDirectory(source)) {
        if (FileSystemUtil.getTotalNumVisibleFiles(source) == 0) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' contains no visible files.", sourceDataPath_));
        }
        if (FileSystemUtil.containsVisibleSubdirectory(source)) {
          throw new AnalysisException(String.format(
              "INPATH location '%s' cannot contain non-hidden subdirectories.",
              sourceDataPath_));
        }
        if (shouldCheckPerms && !checker
          .checkAccess(source, fs, FsAction.READ_WRITE, authzEnabled)) {
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

        if (shouldCheckPerms && !checker
          .checkAccess(source.getParent(), fs, FsAction.WRITE, authzEnabled)) {
          throw new AnalysisException(String.format("Unable to LOAD DATA from %s " +
              "because Impala does not have WRITE permissions on its parent " +
              "directory %s", source, source.getParent()));
        }

        if (shouldCheckPerms && !checker
          .checkAccess(source, fs, FsAction.READ, authzEnabled)) {
          throw new AnalysisException(String.format("Unable to LOAD DATA from %s " +
              "because Impala does not have READ permissions on this file", source));
        }
      }

      FeFsTable.Utils.checkWriteAccess(table,
          partitionSpec_ != null ? partitionSpec_.getPartitionSpecKeyValues() : null,
          "LOAD DATA");
    } catch (FileNotFoundException e) {
      throw new AnalysisException("File not found: " + e.getMessage(), e);
    } catch (IOException e) {
      throw new AnalysisException("Error accessing filesystem: " + e.getMessage(), e);
    }
  }

  /**
   * Creates the child queries that are used to load data into Iceberg tables:
   *   1. a temporary table is created which points to the data directory
   *   2. an insert query copies the data from the source table to the destination table
   *   3. a drop table deletes the temporary table and the data
   */
  private void analyzeLoadIntoIcebergTable() throws AnalysisException {
    Path sourcePath = sourceDataPath_.getPath();
    String tmpTableName = QueryStringBuilder.createTmpTableName(dbName_,
        tableName_.getTbl());
    QueryStringBuilder.Create createTableQueryBuilder =
        QueryStringBuilder.Create.builder().table(tmpTableName, true);
    try {
      FileSystem fs = sourcePath.getFileSystem(FileSystemUtil.getConfiguration());
      Path filePathForLike = sourcePath;
      // LIKE <file format> clause needs a file to infer schema, for directories: list
      // the files under the directory and pick the first one.
      if (fs.isDirectory(sourcePath)) {
        RemoteIterator<? extends FileStatus> fileStatuses = FileSystemUtil.listFiles(fs,
            sourcePath, true, "");
        while (fileStatuses.hasNext()) {
          FileStatus fileStatus = fileStatuses.next();
          String fileName = fileStatus.getPath().getName();
          if (fileStatus.isFile() && !FileSystemUtil.isHiddenFile(fileName)) {
            filePathForLike = fileStatus.getPath();
            break;
          }
        }
      }
      String magicString = FileSystemUtil.readMagicString(filePathForLike);
      if (magicString.substring(0, 4).equals(ParquetFileWriter.MAGIC_STR)) {
        createTableQueryBuilder.like("PARQUET", "%s").storedAs("PARQUET");
      } else if (magicString.substring(0, 3).equals(OrcFile.MAGIC)) {
        createTableQueryBuilder.like("ORC", "%s").storedAs("ORC");
      } else {
        throw new AnalysisException(String.format("INPATH contains unsupported LOAD "
            + "format, file '%s' has '%s' magic string.", filePathForLike, magicString));
      }
      createTableQueryBuilder.tableLocation("%s");
      createTableQueryBuilder.property("TEMPORARY", "true");
    } catch (IOException e) {
      throw new AnalysisException("Failed to generate CREATE TABLE subquery "
          + "statement. ", e);
    }
    createTmpTblQuery_ = createTableQueryBuilder.build();
    QueryStringBuilder.Insert insertTblQueryBuilder = QueryStringBuilder.Insert.builder()
        .overwrite(overwrite_)
        .table(tableName_.toString());
    QueryStringBuilder.Select insertSelectTblQueryBuilder =
        QueryStringBuilder.Select.builder().selectList("*").from(tmpTableName);
    insertTblQueryBuilder.select(insertSelectTblQueryBuilder);
    insertTblQuery_ = insertTblQueryBuilder.build();
    dropTmpTblQuery_ = QueryStringBuilder.Drop.builder().table(tmpTableName).build();
  }

  public TLoadDataReq toThrift() {
    TLoadDataReq loadDataReq = new TLoadDataReq();
    loadDataReq.setTable_name(new TTableName(getDb(), getTbl()));
    loadDataReq.setSource_path(sourceDataPath_.toString());
    loadDataReq.setOverwrite(overwrite_);
    if (partitionSpec_ != null) {
      loadDataReq.setPartition_spec(partitionSpec_.toThrift());
    }
    if (table_ instanceof FeIcebergTable) {
      loadDataReq.setIceberg_tbl(true);
      loadDataReq.setCreate_tmp_tbl_query_template(createTmpTblQuery_);
      loadDataReq.setInsert_into_dst_tbl_query(insertTblQuery_);
      loadDataReq.setDrop_tmp_tbl_query(dropTmpTblQuery_);
    }
    return loadDataReq;
  }
}
