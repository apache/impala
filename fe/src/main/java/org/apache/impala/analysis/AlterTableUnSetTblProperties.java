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

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.FeDataSourceTable;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.KuduTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableUnSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TTablePropertyType;
import org.apache.impala.util.MetaStoreUtil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
* Represents an ALTER TABLE UNSET [PARTITION ('k1'='a', 'k2'='b'...)]
* TBLPROPERTIES|SERDEPROPERTIES ('p1','p2' ...) statement.
*/
public class AlterTableUnSetTblProperties extends AlterTableStmt {
  private final PartitionSet partitionSet_;
  private final TTablePropertyType targetProperty_;
  private final List<String> tblPropertyKeys_;
  private final boolean ifExists_;

  public AlterTableUnSetTblProperties(TableName tableName, PartitionSet partitionSet,
      boolean ifExist, TTablePropertyType targetProperty, List<String> tblPropertyKeys) {
    super(tableName);
    Preconditions.checkNotNull(tblPropertyKeys);
    Preconditions.checkNotNull(targetProperty);
    targetProperty_ = targetProperty;
    tblPropertyKeys_ = tblPropertyKeys;
    partitionSet_ = partitionSet;
    ifExists_ = ifExist;
  }

  public List<String> getTblPropertyKeys() { return tblPropertyKeys_; }

  @Override
  public String getOperation() {
    return (targetProperty_ == TTablePropertyType.TBL_PROPERTY)
        ? "UNSET TBLPROPERTIES" : "UNSET SERDEPROPERTIES";
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.UNSET_TBL_PROPERTIES);
    TAlterTableUnSetTblPropertiesParams tblUnsetPropertyParams =
      new TAlterTableUnSetTblPropertiesParams();
    tblUnsetPropertyParams.setTarget(targetProperty_);
    tblUnsetPropertyParams.setProperty_keys(tblPropertyKeys_);
    if (partitionSet_ != null) {
      tblUnsetPropertyParams.setPartition_set(partitionSet_.toThrift());
    }
    tblUnsetPropertyParams.setIf_exists(ifExists_);
    params.setUnset_tbl_properties_params(tblUnsetPropertyParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    for (String property : tblPropertyKeys_) {
      MetaStoreUtil.checkShortProperty("Property key", property,
          MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH);
    }

    if (tblPropertyKeys_.contains(hive_metastoreConstants.META_TABLE_STORAGE)) {
      throw new AnalysisException(String.format("Changing the '%s' table property is " +
          "not supported to protect against metadata corruption.",
          hive_metastoreConstants.META_TABLE_STORAGE));
    }

    if (getTargetTable() instanceof FeKuduTable) {
      analyzeKuduTable(analyzer);
    } else if (getTargetTable() instanceof FeIcebergTable) {
      analyzeIcebergTable(analyzer);
    } else if (getTargetTable() instanceof FeDataSourceTable) {
      analyzeDataSourceTable(analyzer);
    }

    // Unsetting avro.schema.url or avro.schema.literal are not allowed to
    // avoid potential metadata corruption (see IMPALA-2042).
    // One improvement can be that if both avro.schema.literal and avro.schema.url
    // are already set then we can allow unsetting one of them if Avro Schema pointed
    // by other is valid. But for now it is not allowed.
    propertyCheck(AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(),
        "Avro");
    propertyCheck(AvroSerdeUtils.AvroTableProperties.SCHEMA_URL.getPropName(),
        "Avro");
  }

  private void analyzeKuduTable(Analyzer analyzer) throws AnalysisException {
    // Don't allow unsetting those properties that cannot be set back using
    // ALTER TABLE SET TBLPROPERTIES. Also during runtime, config validation
    // for Kudu table will be done.

    // Throw error if kudu.table_name is provided for synchronized Kudu tables
    // TODO IMPALA-6375: Allow setting kudu.table_name for synchronized Kudu tables
    if (KuduTable.isSynchronizedTable(table_.getMetaStoreTable())) {
      propertyCheck(KuduTable.KEY_TABLE_NAME, "synchronized Kudu");
    }
    propertyCheck(KuduTable.KEY_TABLE_ID, "Kudu");
    propertyCheck(KuduTable.KEY_MASTER_HOSTS, "Kudu");
  }

  private void analyzeIcebergTable(Analyzer analyzer) throws AnalysisException {
    //Cannot unset these properties related to metadata
    propertyCheck(IcebergTable.ICEBERG_CATALOG, "Iceberg");
    propertyCheck(IcebergTable.ICEBERG_CATALOG_LOCATION, "Iceberg");
    propertyCheck(IcebergTable.ICEBERG_TABLE_IDENTIFIER, "Iceberg");
    propertyCheck(IcebergTable.METADATA_LOCATION, "Iceberg");
  }

  private void analyzeDataSourceTable(Analyzer analyzer) throws AnalysisException {
    if (partitionSet_ != null) {
      throw new AnalysisException("Partition is not supported for DataSource table.");
    } else if (targetProperty_ == TTablePropertyType.SERDE_PROPERTY) {
      throw new AnalysisException("ALTER TABLE UNSET SERDEPROPERTIES is not supported " +
          "for DataSource table.");
    }
    // Cannot unset internal properties of DataSource.
    propertyCheck(DataSourceTable.TBL_PROP_DATA_SRC_NAME, "DataSource");
    propertyCheck(DataSourceTable.TBL_PROP_INIT_STRING, "DataSource");
    propertyCheck(DataSourceTable.TBL_PROP_LOCATION, "DataSource");
    propertyCheck(DataSourceTable.TBL_PROP_CLASS, "DataSource");
    propertyCheck(DataSourceTable.TBL_PROP_API_VER, "DataSource");
    // Cannot unset properties which are required JDBC parameters.
    for (String property : tblPropertyKeys_) {
      if (DataSourceTable.isRequiredJdbcParameter(property)) {
        throw new AnalysisException(String.format("Unsetting the '%s' table property " +
            "is not supported for JDBC DataSource table.", property));
      }
    }
  }

  private void propertyCheck(String property, String tableType) throws AnalysisException {
    if (tblPropertyKeys_.contains(property)) {
      throw new AnalysisException(String.format("Unsetting the '%s' table property is " +
          "not supported for %s table.", property, tableType));
    }
  }
}
