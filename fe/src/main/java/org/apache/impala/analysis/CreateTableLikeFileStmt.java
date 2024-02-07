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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.HdfsCompression;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.Pair;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.THdfsFileFormat;


/**
 * Represents a CREATE TABLE tablename LIKE fileformat '/path/to/file' statement
 * where the schema is inferred from the given file. Does not partition the table by
 * default.
 */
public class CreateTableLikeFileStmt extends CreateTableStmt {
  private final HdfsUri schemaLocation_;
  private final THdfsFileFormat schemaFileFormat_;

  public CreateTableLikeFileStmt(CreateTableStmt createTableStmt,
      THdfsFileFormat schemaFileFormat, HdfsUri schemaLocation) {
    super(createTableStmt);
    schemaLocation_ = schemaLocation;
    schemaFileFormat_ = schemaFileFormat;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    List<String> colsSql = new ArrayList<>();
    List<String> partitionColsSql = new ArrayList<>();
    HdfsCompression compression = HdfsCompression.fromFileName(
        schemaLocation_.toString());
    String s = ToSqlUtils.getCreateTableSql(getDb(),
        getTbl() + " __LIKE_FILEFORMAT__ ",  getComment(), colsSql, partitionColsSql,
        /* isPrimaryKeyUnique */true, /* primaryKeysSql */null, /* foreignKeysSql */null,
        /* kuduPartitionByParams */null, new Pair<>(getSortColumns(), getSortingOrder()),
        getTblProperties(), getSerdeProperties(), isExternal(), getIfNotExists(),
        getRowFormat(), HdfsFileFormat.fromThrift(getFileFormat()), compression, null,
        getLocation(), null, null);
    s = s.replace("__LIKE_FILEFORMAT__", String.format("LIKE %s '%s'",
        schemaFileFormat_, schemaLocation_.toString()));
    return s;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (getFileFormat() == THdfsFileFormat.KUDU) {
      throw new AnalysisException("CREATE TABLE LIKE FILE statement is not supported " +
          "for Kudu tables.");
    } else if (getFileFormat() == THdfsFileFormat.JDBC) {
      throw new AnalysisException("CREATE TABLE LIKE FILE statement is not supported " +
          "for JDBC tables.");
    }
    schemaLocation_.analyze(analyzer, Privilege.ALL, FsAction.READ);
    switch (schemaFileFormat_) {
      case PARQUET:
        getColumnDefs().addAll(ParquetSchemaExtractor.extract(schemaLocation_));
        break;
      case ORC:
        if (MetastoreShim.getMajorVersion() < 3) {
          throw new AnalysisException("Creating table like ORC file is unsupported for " +
              "Hive with version < 3");
        }
        getColumnDefs().addAll(OrcSchemaExtractor.extract(schemaLocation_));
        break;
      default:
        throw new AnalysisException("Unsupported file type for schema inference: " +
            schemaFileFormat_);
    }
    super.analyze(analyzer);
  }
}
