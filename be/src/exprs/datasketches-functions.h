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
  /// it is not, then the query fails. Otherwise, returns the count(distinct) estimate
  /// from the sketch.
  static BigIntVal DsHllEstimate(FunctionContext* ctx,
      const StringVal& serialized_sketch);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails. 'rank' is used to identify which item (estimate)
  /// to return from the sketched dataset. E.g. 0.1 means the item where 10% of the
  /// sketched dataset is lower or equals to this particular item. 'rank' should be in
  /// the range of [0,1]. Otherwise this function returns error.
  static FloatVal DsKllQuantile(FunctionContext* ctx, const StringVal& serialized_sketch,
      const DoubleVal& rank);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails.
  /// Returns the number of input values fed to 'serialized_sketch'.
  static BigIntVal DsKllN(FunctionContext* ctx, const StringVal& serialized_sketch);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails. This function returns a value in the range of [0,1]
  /// where e.g. 0.2 means that 'probe_value' is greater than the 20% of the values in
  /// 'serialized_sketch'. Note, this is an approximate calculation.
  static DoubleVal DsKllRank(FunctionContext* ctx, const StringVal& serialized_sketch,
      const FloatVal& probe_value);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails. This function is similar to DsKllQuantile() but
  /// this one can receive multiple ranks and returns a comma separated string that
  /// contains the results for all the given ranks.
  /// Note, this function is meant to return an Array of floats as the result but with
  /// that we have to wait for the complex type support. Tracking Jira is IMPALA-9520.
  static StringVal DsKllQuantilesAsString(FunctionContext* ctx,
      const StringVal& serialized_sketch, int num_args, const DoubleVal* args);

  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails.
  /// 'args' holds one or more numbers that will be used as ranges to divide the input
  /// of the sketch. E.g. [1.0, 3.5, 10.1] will create the following ranges:
  ///     (-inf, 1.0), [1.0, 3.5), [3.5, 10.1), [10.1, +inf)
  /// This function returns a comma separated string that contains the probability of
  /// having an item in each of the received ranges. E.g. a return value of 0.2 means
  /// that approximately 20% of the items are in that given range.
  /// Note, this function is meant to return an Array of doubles as the result but with
  /// that we have to wait for the complex type support. Tracking Jira is IMPALA-9520.
  static StringVal DsKllPMFAsString(FunctionContext* ctx,
      const StringVal& serialized_sketch, int num_args, const FloatVal* args);


  /// 'serialized_sketch' is expected as a serialized Apache DataSketches KLL sketch. If
  /// it is not, then the query fails.
  /// 'args' holds one or more numbers that will be used as ranges to divide the input
  /// of the sketch. E.g. [1.0, 3.5, 10.1] will create the following ranges:
  ///     (-inf, 1.0), (-inf, 3.5), (-inf, 10.1), (-inf, +inf)
  /// This function returns a comma separated string that contains the probability of
  /// having an item in each of the received ranges. E.g. a return value of 0.2 means
  /// that approximately 20% of the items are in that given range.
  /// Note, this function is meant to return an Array of doubles as the result but with
  /// that we have to wait for the complex type support. Tracking Jira is IMPALA-9520.
  static StringVal DsKllCDFAsString(FunctionContext* ctx,
      const StringVal& serialized_sketch, int num_args, const FloatVal* args);

private:
  enum PMFCDF {
    PMF,
    CDF
  };

  /// Helper functions for DsKllPMFAsString() and DsKllCDFAsString(). 'mode' indicates
  /// whether get_PMF() or get_CDF() should be invoked on the KLL sketch.
  static StringVal GetDsKllPMFOrCDF(FunctionContext* ctx,
      const StringVal& serialized_sketch, int num_args, const FloatVal* args,
      PMFCDF mode);
};

}

