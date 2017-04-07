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

import org.apache.impala.thrift.TSetQueryOptionRequest;

import com.google.common.base.Preconditions;

/**
 * Representation of a SET query options statement.
 */
public class SetStmt extends StatementBase {
  private final String key_;
  private final String value_;
  private final boolean isSetAll_;

  // This key is deprecated in Impala 2.0; COMPRESSION_CODEC_KEY replaces this
  private static final String DEPRECATED_PARQUET_CODEC_KEY = "PARQUET_COMPRESSION_CODEC";
  private static final String COMPRESSION_CODEC_KEY = "COMPRESSION_CODEC";

  // maps the given key name to a key defined in the thrift file
  private static String resolveThriftKey(String key) {
    if (key.toLowerCase().equals(DEPRECATED_PARQUET_CODEC_KEY.toLowerCase())) {
      return COMPRESSION_CODEC_KEY;
    }
    return key;
  }

  public SetStmt(String key, String value, boolean isSetAll) {
    Preconditions.checkArgument((key == null) == (value == null));
    Preconditions.checkArgument(key == null || !key.isEmpty());
    Preconditions.checkArgument(!isSetAll || (key == null && value == null) );
    key_ = key;
    value_ = value;
    isSetAll_ = isSetAll;
  }

  @Override
  public String toSql() {
    if (key_ == null) {
      if (isSetAll_) return "SET ALL";
      return "SET";
    }
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
      request.setKey(resolveThriftKey(key_));
      request.setValue(value_);
    }
    if (isSetAll_) request.setIs_set_all(true);
    return request;
  }
}
