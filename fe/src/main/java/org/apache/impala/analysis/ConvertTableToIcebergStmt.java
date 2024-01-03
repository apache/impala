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
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.impala.analysis.QueryStringBuilder.Create;
import org.apache.impala.analysis.QueryStringBuilder.Drop;
import org.apache.impala.analysis.QueryStringBuilder.Invalidate;
import org.apache.impala.analysis.QueryStringBuilder.Refresh;
import org.apache.impala.analysis.QueryStringBuilder.Rename;
import org.apache.impala.analysis.QueryStringBuilder.SetTblProps;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.Table;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.THdfsFileFormat;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TConvertTableRequest;
import org.apache.impala.util.AcidUtils;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.MigrateTableUtil;

/**
 * Represents an "ALTER TABLE ... CONVERT TO" statement for the migration from HDFS table
 * to an Iceberg table:
 * ALTER TABLE <table name> CONVERT TO ICEBERG
 * [TBLPROPERTIES (prop1=val1, prop2=val2 ...)]
 */
public class ConvertTableToIcebergStmt extends StatementBase {

  private TableName tableName_;
  private TableName tmpHdfsTableName_;
  private final Map<String, String> properties_;
  private String setHdfsTablePropertiesQuery_;
  private String renameHdfsTableToTemporaryQuery_;
  private String refreshTemporaryHdfsTableQuery_;
  private String resetTableNameQuery_;
  private String createIcebergTableQuery_;
  private String invalidateMetadataQuery_;
  private String postCreateAlterTableQuery_;
  private String dropTemporaryHdfsTableQuery_;

  public ConvertTableToIcebergStmt(TableName tableName, Map<String, String> properties) {
    tableName_ = tableName;
    properties_ = properties;
  }

  public ConvertTableToIcebergStmt(TableName tableName) {
    this(tableName, Maps.newHashMap());
  }

  @Override
  public void collectTableRefs(List<TableRef> tblRefs) {
    tblRefs.add(new TableRef(tableName_.toPath(), null));
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    tableName_ = analyzer.getFqTableName(tableName_);
    // TODO: Until IMPALA-12190 is fixed, user needs ALL privileges on the DB to migrate a
    // table. Once it's fixed, ALL privileges on the table are enough.
    analyzer.getDb(tableName_.getDb(), Privilege.ALL);
    FeTable table = analyzer.getTable(tableName_, Privilege.ALL);
    if (!(table instanceof FeFsTable)) {
      throw new AnalysisException("CONVERT TO ICEBERG is not supported for " +
          table.getClass().getSimpleName());
    }

    if (table.getMetaStoreTable().getParameters() != null &&
        AcidUtils.isTransactionalTable(table.getMetaStoreTable().getParameters())) {
      throw new AnalysisException(
          "CONVERT TO ICEBERG is not supported for transactional tables");
    }

    if (!MetaStoreUtils.isExternalTable(table.getMetaStoreTable())) {
      throw new AnalysisException(
              "CONVERT TO ICEBERG is not supported for managed tables");
    }

    StorageDescriptor sd = table.getMetaStoreTable().getSd();
    if (MigrateTableUtil.getFileFormat(sd) == null) {
      throw new AnalysisException("CONVERT TO ICEBERG is not supported for " +
          sd.getInputFormat());
    }

    if (properties_.size() > 1 ||
        properties_.keySet().stream().anyMatch(
            key -> !key.equalsIgnoreCase(IcebergTable.ICEBERG_CATALOG)) ) {
      throw new AnalysisException(String.format(
          "CONVERT TO ICEBERG only accepts '%s' as TBLPROPERTY.",
          IcebergTable.ICEBERG_CATALOG));
    }

    if (TIcebergCatalog.HADOOP_CATALOG == IcebergUtil.getTIcebergCatalog(properties_)) {
      throw new AnalysisException("The Hadoop Catalog is not supported because the " +
          "location may change");
    }

    createSubQueryStrings((FeFsTable) table);
  }

  private void createSubQueryStrings(FeFsTable table)  {
    setHdfsTablePropertiesQuery_ = SetTblProps.builder()
              .table(table.getFullName())
              .property(Table.TBL_PROP_EXTERNAL_TABLE_PURGE, "false")
              .property("TRANSLATED_TO_EXTERNAL", "FALSE").build();

    tmpHdfsTableName_ = createTmpTableName();
    Preconditions.checkState(tmpHdfsTableName_.isFullyQualified());

    renameHdfsTableToTemporaryQuery_ = Rename.builder()
        .source(table.getFullName())
        .target(tmpHdfsTableName_.toString()).build();

    refreshTemporaryHdfsTableQuery_ = Refresh.builder()
        .table(tmpHdfsTableName_.toString())
        .build();

    resetTableNameQuery_ = Rename.builder()
            .source(tmpHdfsTableName_.toString())
            .target(table.getFullName()).build();

    if (!IcebergUtil.isHiveCatalog(properties_)) {
      Preconditions.checkState(tableName_.isFullyQualified());
      Create create = Create.builder()
          .table(tableName_.toString(), true)
          .storedAs(THdfsFileFormat.ICEBERG.toString())
          .tableLocation(table.getLocation());
      for (Map.Entry<String, String> propEntry : properties_.entrySet()) {
        create.property(propEntry.getKey(), propEntry.getValue());
      }
      createIcebergTableQuery_ = create.build();

      postCreateAlterTableQuery_ = SetTblProps.builder()
          .table(tableName_.toString())
          .property(Table.TBL_PROP_EXTERNAL_TABLE_PURGE, "true").build();
    } else {
      // In HiveCatalog we invoke an IM after creating the table to immediately propagate
      // the existance of the new Iceberg table and avoid timing issues.
      invalidateMetadataQuery_ = Invalidate.builder()
          .table(tableName_.toString())
          .build();
    }

    dropTemporaryHdfsTableQuery_ = Drop.builder()
        .table(tmpHdfsTableName_.toString()).build();
  }

  private TableName createTmpTableName() {
    String tmpTableNameStr = QueryStringBuilder.createTmpTableName(
        tableName_.getDb(), tableName_.getTbl());
    return TableName.parse(tmpTableNameStr);
  }

  public TConvertTableRequest toThrift() {
    Preconditions.checkNotNull(tableName_);
    Preconditions.checkNotNull(tmpHdfsTableName_);
    TConvertTableRequest params = new TConvertTableRequest(tableName_.toThrift(),
        tmpHdfsTableName_.toThrift(), THdfsFileFormat.ICEBERG);
    params.setProperties(properties_);
    params.setSet_hdfs_table_properties_query(setHdfsTablePropertiesQuery_);
    params.setRename_hdfs_table_to_temporary_query(renameHdfsTableToTemporaryQuery_);
    params.setRefresh_temporary_hdfs_table_query(refreshTemporaryHdfsTableQuery_);
    params.setReset_table_name_query(resetTableNameQuery_);
    if (!Strings.isNullOrEmpty(createIcebergTableQuery_)) {
      params.setCreate_iceberg_table_query(createIcebergTableQuery_);
    }
    if (!Strings.isNullOrEmpty(invalidateMetadataQuery_)) {
      params.setInvalidate_metadata_query(invalidateMetadataQuery_);
    }
    if (!Strings.isNullOrEmpty(postCreateAlterTableQuery_)) {
      params.setPost_create_alter_table_query(postCreateAlterTableQuery_);
    }
    params.setDrop_temporary_hdfs_table_query(dropTemporaryHdfsTableQuery_);
    return params;
  }
}