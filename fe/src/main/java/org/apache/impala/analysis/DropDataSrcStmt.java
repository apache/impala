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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TDropDataSourceParams;

import com.google.common.base.Preconditions;

/**
 * Represents a DROP DATA SOURCE statement.
 */
public class DropDataSrcStmt extends StatementBase {

  private final String dataSrcName_;
  private final boolean ifExists_;

  public DropDataSrcStmt(String dataSrcName, boolean ifExists) {
    Preconditions.checkNotNull(dataSrcName);
    this.dataSrcName_ = dataSrcName.toLowerCase();
    this.ifExists_ = ifExists;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (!MetastoreShim.validateName(dataSrcName_) ||
        (!ifExists_ && analyzer.getCatalog().getDataSource(dataSrcName_) == null)) {
      throw new AnalysisException(
          Analyzer.DATA_SRC_DOES_NOT_EXIST_ERROR_MSG + dataSrcName_);
    }
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append("DROP DATA SOURCE ");
    if (ifExists_) sb.append("IF EXISTS ");
    sb.append(dataSrcName_);
    return sb.toString();
  }

  public TDropDataSourceParams toThrift() {
    return new TDropDataSourceParams(dataSrcName_).setIf_exists(ifExists_);
  }
}
