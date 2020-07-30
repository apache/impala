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

#pragma once

#include "udf/udf.h"

namespace impala {

using impala_udf::BigIntVal;
using impala_udf::DoubleVal;
using impala_udf::FloatVal;
using impala_udf::FunctionContext;
using impala_udf::StringVal;

class DataSketchesFunctions {
public:
  /// 'serialized_sketch' is expected as a serialized Apache DataSketches HLL sketch. If
  /// it is not then the query fails. Otherwise, returns the count(distinct) estimate
  /// from the sketch.
  static BigIntVal DsHllEstimate(FunctionContext* ctx,
      const StringVal& serialized_sketch);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not then the query fails. 'rank' is used to identify which item (estimate) to
  /// return from the sketched dataset. E.g. 0.1 means the item where 10% of the sketched
  /// dataset is lower or equals to this particular item. 'rank' should be in the range
  /// of [0,1]. Otherwise this function returns error.
  static FloatVal DsKllQuantile(FunctionContext* ctx, const StringVal& serialized_sketch,
      const DoubleVal& rank);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails.
  /// Returns the number of input values fed to 'serialized_sketch'.
  static BigIntVal DsKllN(FunctionContext* ctx, const StringVal& serialized_sketch);
};

}

