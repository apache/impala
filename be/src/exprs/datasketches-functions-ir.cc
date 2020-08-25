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

#include "exprs/datasketches-functions.h"

#include "exprs/datasketches-common.h"
#include "gutil/strings/substitute.h"
#include "thirdparty/datasketches/hll.hpp"
#include "thirdparty/datasketches/kll_sketch.hpp"
#include "udf/udf-internal.h"

namespace impala {

using strings::Substitute;

BigIntVal DataSketchesFunctions::DsHllEstimate(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  datasketches::hll_sketch sketch(DS_SKETCH_CONFIG, DS_HLL_TYPE);
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return BigIntVal::null();
  }
  return sketch.get_estimate();
}

FloatVal DataSketchesFunctions::DsKllQuantile(FunctionContext* ctx,
    const StringVal& serialized_sketch, const DoubleVal& rank) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return FloatVal::null();
  if (rank.val < 0.0 || rank.val > 1.0) {
    ctx->SetError("Rank parameter should be in the range of [0,1]");
    return FloatVal::null();
  }
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return FloatVal::null();
  }
  try {
    return sketch.get_quantile(rank.val);
  } catch (const std::exception& e) {
    ctx->SetError(Substitute("Error while getting quantile from DataSketches KLL. "
        "Message: $0", e.what()).c_str());
    return FloatVal::null();
  }
}

BigIntVal DataSketchesFunctions::DsKllN(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return BigIntVal::null();
  }
  return sketch.get_n();
}

DoubleVal DataSketchesFunctions::DsKllRank(FunctionContext* ctx,
    const StringVal& serialized_sketch, const FloatVal& probe_value) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return DoubleVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return DoubleVal::null();
  }
  return sketch.get_rank(probe_value.val);
}

StringVal DataSketchesFunctions::DsKllQuantilesAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const DoubleVal* args) {
  DCHECK(num_args > 0);
  if (args == nullptr) return StringVal::null();
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  if (RaiseErrorForNullOrNaNInput(ctx, num_args, args)) return StringVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  double quantiles_input[(unsigned int)num_args];
  for (int i = 0; i < num_args; ++i) quantiles_input[i] = args[i].val;
  try {
    std::vector<float> results = sketch.get_quantiles(quantiles_input, num_args);
    return DsKllVectorResultToStringVal(ctx, results);
  } catch(const std::exception& e) {
    ctx->SetError(Substitute("Error while getting quantiles from DataSketches KLL. "
        "Message: $0", e.what()).c_str());
    return StringVal::null();
  }
}

StringVal DataSketchesFunctions::GetDsKllPMFOrCDF(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const FloatVal* args,
    PMFCDF mode) {
  DCHECK(num_args > 0);
  if (args == nullptr || args->is_null) return StringVal::null();
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  if (RaiseErrorForNullOrNaNInput(ctx, num_args, args)) return StringVal::null();
  datasketches::kll_sketch<float> sketch;
  if (!DeserializeDsSketch(serialized_sketch, &sketch)) {
    LogSketchDeserializationError(ctx);
    return StringVal::null();
  }
  float input_ranges[(unsigned int)num_args];
  for (int i = 0; i < num_args; ++i) input_ranges[i] = args[i].val;
  try {
    std::vector<double> results = (mode == PMF) ?
        sketch.get_PMF(input_ranges, num_args) : sketch.get_CDF(input_ranges, num_args);
    return DsKllVectorResultToStringVal(ctx, results);
  } catch(const std::exception& e) {
    ctx->SetError(Substitute("Error while running DataSketches KLL function. "
        "Message: $0", e.what()).c_str());
    return StringVal::null();
  }
  return StringVal::null();
}

StringVal DataSketchesFunctions::DsKllPMFAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const FloatVal* args) {
  return GetDsKllPMFOrCDF(ctx, serialized_sketch, num_args, args, PMF);
}

StringVal DataSketchesFunctions::DsKllCDFAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const FloatVal* args) {
  return GetDsKllPMFOrCDF(ctx, serialized_sketch, num_args, args, CDF);
}

}

