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

package com.cloudera.impala.analysis;

import java.util.HashMap;

import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetTblPropertiesParams;
import com.cloudera.impala.thrift.TAlterTableType;
import com.cloudera.impala.thrift.TTablePropertyType;
import com.google.common.base.Preconditions;

/**
* Represents an ALTER TABLE SET TBLPROPERTIES|SERDEPROPERTIES ('p1'='v1', ...) statement.
*/
public class AlterTableSetTblProperties extends AlterTableSetStmt {
  private final TTablePropertyType targetProperty_;
  private final HashMap<String, String> tblProperties_;

  public AlterTableSetTblProperties(TableName tableName,
      TTablePropertyType targetProperty,
      HashMap<String, String> tblProperties) {
   super(tableName, null);
   Preconditions.checkNotNull(tblProperties);
   Preconditions.checkNotNull(targetProperty);
   targetProperty_ = targetProperty;
   tblProperties_ = tblProperties;
   CreateTableStmt.unescapeProperties(tblProperties_);
  }

  public HashMap<String, String> getTblProperties() { return tblProperties_; }

  @Override
  public TAlterTableParams toThrift() {
   TAlterTableParams params = super.toThrift();
   params.setAlter_type(TAlterTableType.SET_TBL_PROPERTIES);
   TAlterTableSetTblPropertiesParams tblPropertyParams =
       new TAlterTableSetTblPropertiesParams();
   tblPropertyParams.setTarget(targetProperty_);
   tblPropertyParams.setProperties(tblProperties_);
   params.setSet_tbl_properties_params(tblPropertyParams);
   return params;
  }
}
