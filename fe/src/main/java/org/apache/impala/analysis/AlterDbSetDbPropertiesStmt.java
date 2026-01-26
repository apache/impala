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

import java.util.Map;

import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterDbParams;
import org.apache.impala.thrift.TAlterDbSetDbPropertiesParams;
import org.apache.impala.thrift.TAlterDbType;

/**
 * Represents an ALTER DATABASE db SET DBPROPERTIES ('key'='value', ...) statement.
 */
public class AlterDbSetDbPropertiesStmt extends AlterDbStmt {
  private final Map<String, String> dbProperties_;

  public AlterDbSetDbPropertiesStmt(String dbName, Map<String, String> properties) {
    super(dbName);
    this.dbProperties_ = properties;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    analyzer.getDb(dbName_, Privilege.ALTER);
  }

  @Override
  public TAlterDbParams toThrift() {
    TAlterDbParams params = super.toThrift();
    params.setAlter_type(TAlterDbType.SET_DBPROPERTIES);

    TAlterDbSetDbPropertiesParams setDbPropertiesParams
        = new TAlterDbSetDbPropertiesParams();

    setDbPropertiesParams.setProperties(dbProperties_);
    params.setSet_dbproperties_params(setDbPropertiesParams);

    return params;
  }
}