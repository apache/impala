// Copyright 2014 Cloudera Inc.
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

import com.cloudera.impala.thrift.TSetQueryOptionRequest;
import com.google.common.base.Preconditions;

/**
 * Representation of a SET query options statement.
 */
public class SetStmt extends StatementBase {
  private final String key_;
  private final String value_;

  public SetStmt(String key, String value) {
    Preconditions.checkArgument((key == null) == (value == null));
    Preconditions.checkArgument(key == null || !key.isEmpty());
    key_ = key;
    value_ = value;
  }

  @Override
  public String toSql() {
    if (key_ == null) return "SET";
    Preconditions.checkNotNull(value_);
    return "SET " + ToSqlUtils.getIdentSql(key_) + "='" + value_ + "'";
  }

  @Override
  public void analyze(Analyzer analyzer) {
    // Query option key is validated by the backend.
  }

  public TSetQueryOptionRequest toThrift() {
    TSetQueryOptionRequest request = new TSetQueryOptionRequest();
    if (key_ != null) {
      request.setKey(key_);
      request.setValue(value_);
    }
    return request;
  }
}
