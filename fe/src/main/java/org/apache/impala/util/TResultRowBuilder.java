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

package org.apache.impala.util;

import org.apache.impala.common.PrintUtils;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TResultRow;

/**
 * Utility class for building TResultRows.
 */
public class TResultRowBuilder {
  private final TResultRow row_ = new TResultRow();

  public TResultRowBuilder add(long val) {
    TColumnValue colVal = new TColumnValue();
    colVal.setLong_val(val);
    row_.addToColVals(colVal);
    return this;
  }

  public TResultRowBuilder add(double val) {
    TColumnValue colVal = new TColumnValue();
    colVal.setDouble_val(val);
    row_.addToColVals(colVal);
    return this;
  }

  public TResultRowBuilder add(String val) {
    TColumnValue colVal = new TColumnValue();
    colVal.setString_val(val);
    row_.addToColVals(colVal);
    return this;
  }

  public TResultRowBuilder add(boolean val) {
    TColumnValue colVal = new TColumnValue();
    colVal.setBool_val(val);
    row_.addToColVals(colVal);
    return this;
  }

  public TResultRowBuilder addBytes(long val) {
    TColumnValue colVal = new TColumnValue();
    colVal.setString_val(PrintUtils.printBytes(val));
    row_.addToColVals(colVal);
    return this;
  }

  public TResultRowBuilder reset() {
    row_.clear();
    return this;
  }

  public TResultRow get() { return row_; }
}
