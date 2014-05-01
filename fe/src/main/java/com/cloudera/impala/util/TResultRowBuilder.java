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

package com.cloudera.impala.util;

import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TResultRow;

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
