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
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeView;
import org.apache.impala.catalog.MaterializedViewHdfsTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetTblPropertiesParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTablePropertyType;
import org.apache.impala.util.MetaStoreUtil;

import java.util.Map;

/**
 * Represents an ALTER VIEW SET TBLPROPERTIES ('p1'='v1', ...) statement.
 */
public class AlterViewSetTblProperties extends AlterTableSetStmt {

  private final Map<String, String> tblProperties_;

  public AlterViewSetTblProperties(TableName tableName,
      Map<String, String> tblProperties) {
    super(tableName, null);
    Preconditions.checkNotNull(tblProperties);
    tblProperties_ = tblProperties;
    CreateTableStmt.unescapeProperties(tblProperties_);
  }

  @Override
  public String getOperation() { return "SET TBLPROPERTIES"; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = new TAlterTableParams();
    params.setTable_name(new TTableName(getDb(), getTbl()));
    params.setAlter_type(TAlterTableType.SET_VIEW_PROPERTIES);
    TAlterTableSetTblPropertiesParams tblPropertyParams =
        new TAlterTableSetTblPropertiesParams();
    tblPropertyParams.setTarget(TTablePropertyType.TBL_PROPERTY);
    tblPropertyParams.setProperties(tblProperties_);
    params.setSet_tbl_properties_params(tblPropertyParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    MetaStoreUtil.checkShortPropertyMap("Property", tblProperties_);

    table_ = analyzer.getTable(tableName_, Privilege.ALTER);
    Preconditions.checkNotNull(table_);
    if (table_ instanceof MaterializedViewHdfsTable) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a materialized view: %s", tableName_));
    } else if (!(table_ instanceof FeView)) {
      throw new AnalysisException(String.format(
          "ALTER VIEW not allowed on a table: %s", tableName_));
    }

    if (tblProperties_.containsKey(hive_metastoreConstants.META_TABLE_STORAGE)) {
      throw new AnalysisException(String.format("Changing the '%s' view property is " +
          "not supported to protect against metadata corruption.",
          hive_metastoreConstants.META_TABLE_STORAGE));
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append("ALTER VIEW ");
    if (tableName_.getDb() != null) sb.append(tableName_.getDb() + ".");
    sb.append(tableName_.getTbl());
    sb.append(" SET TBLPROPERTIES " + ToSqlUtils.propertyMapToSql(tblProperties_));
    return sb.toString();
  }
}