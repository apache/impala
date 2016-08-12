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

import org.apache.impala.thrift.TColumnValue;

/**
 * Utility functions for working with TColumnValue objects.
 */
public class TColumnValueUtil {
  /**
   * Extract numeric value from TColumnValue.
   */
  public static double getNumericVal(TColumnValue val) {
    if (val.isSetByte_val()) {
      return (double) val.byte_val;
    } else if (val.isSetShort_val()) {
      return (double) val.short_val;
    } else if (val.isSetInt_val()) {
      return (double) val.int_val;
    } else if (val.isSetLong_val()) {
      return (double) val.long_val;
    } else if (val.isSetDouble_val()) {
      return (double) val.double_val;
    } else if (val.isSetString_val()) {
      // we always return decimals as strings, even with as_ascii=false
      // in Expr::GetValue()
      try {
        return Double.valueOf(val.string_val);
      } catch (NumberFormatException e) {
        return 0;
      }
    }
    return 0;
  }
}
