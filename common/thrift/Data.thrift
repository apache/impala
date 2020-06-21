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

namespace cpp impala
namespace java org.apache.impala.thrift

// this is a union over all possible return types
struct TColumnValue {
  1: optional bool bool_val
  6: optional byte byte_val
  7: optional i16 short_val
  2: optional i32 int_val
  3: optional i64 long_val
  4: optional double double_val
  5: optional string string_val
  8: optional binary binary_val
  9: optional binary timestamp_val
 10: optional binary decimal_val
 11: optional i32 date_val
}

struct TResultRow {
  1: list<TColumnValue> colVals
}

// A union over all possible return types for a column of data
// Currently only used by ExternalDataSource types
struct TColumnData {
  // One element in the list for every row in the column indicating if there is
  // a value in the vals list or a null.
  1: required list<bool> is_null;

  // Only one is set, only non-null values are set.
  2: optional list<bool> bool_vals;
  3: optional list<byte> byte_vals;
  4: optional list<i16> short_vals;
  5: optional list<i32> int_vals;
  6: optional list<i64> long_vals;
  7: optional list<double> double_vals;
  8: optional list<string> string_vals;
  9: optional list<binary> binary_vals;
}
