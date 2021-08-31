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
#include "thirdparty/datasketches/cpc_sketch.hpp"
#include "thirdparty/datasketches/cpc_union.hpp"
#include "thirdparty/datasketches/theta_sketch.hpp"
#include "thirdparty/datasketches/theta_union.hpp"
#include "thirdparty/datasketches/theta_intersection.hpp"
#include "thirdparty/datasketches/theta_a_not_b.hpp"
#include "thirdparty/datasketches/kll_sketch.hpp"
#include "udf/udf-internal.h"

namespace impala {

using strings::Substitute;

BigIntVal DataSketchesFunctions::DsHllEstimate(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  try {
    auto sketch = datasketches::hll_sketch::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    return sketch.get_estimate();
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return BigIntVal::null();
  }
}

StringVal DataSketchesFunctions::DsHllEstimateBoundsAsString(
    FunctionContext* ctx, const StringVal& serialized_sketch) {
  return DsHllEstimateBoundsAsString(ctx, serialized_sketch, DS_DEFAULT_KAPPA);
}

StringVal DataSketchesFunctions::DsHllEstimateBoundsAsString(
    FunctionContext* ctx, const StringVal& serialized_sketch, const IntVal& kappa) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0 || kappa.is_null)
    return StringVal::null();
  if (UNLIKELY(kappa.val < 1 || kappa.val > 3)) {
    ctx->SetError("Kappa must be 1, 2 or 3");
    return StringVal::null();
  }
  try {
    auto sketch = datasketches::hll_sketch::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    std::stringstream buffer;
    buffer << sketch.get_estimate() << "," << sketch.get_lower_bound(kappa.val) << ","
           << sketch.get_upper_bound(kappa.val);
    return StringStreamToStringVal(ctx, buffer);
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return StringVal::null();
  }
}

StringVal DataSketchesFunctions::DsHllUnionF(FunctionContext* ctx,
    const StringVal& serialized_sketch1, const StringVal& serialized_sketch2) {
  datasketches::hll_union union_sketch(DS_SKETCH_CONFIG);
  if (!serialized_sketch1.is_null && serialized_sketch1.len > 0) {
    try {
      union_sketch.update(datasketches::hll_sketch::deserialize(serialized_sketch1.ptr,
          serialized_sketch1.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return StringVal::null();
    }
  }
  if (!serialized_sketch2.is_null && serialized_sketch2.len > 0) {
    try {
      union_sketch.update(datasketches::hll_sketch::deserialize(serialized_sketch2.ptr,
          serialized_sketch2.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return StringVal::null();
    }
  }
  auto bytes = union_sketch.get_result(DS_HLL_TYPE).serialize_compact();
  StringVal result(ctx, bytes.size());
  memcpy(result.ptr, bytes.data(), bytes.size());
  return result;
}

StringVal DataSketchesFunctions::DsHllStringify(FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  try {
    auto sketch = datasketches::hll_sketch::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    string str = sketch.to_string(true, false, false, false);
    StringVal dst(ctx, str.size());
    memcpy(dst.ptr, str.c_str(), str.size());
    return dst;
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return StringVal::null();
  }
}

BigIntVal DataSketchesFunctions::DsCpcEstimate(
    FunctionContext* ctx, const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return BigIntVal::null();
  try {
    auto sketch = datasketches::cpc_sketch::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    return sketch.get_estimate();
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return BigIntVal::null();
  }
}

StringVal DataSketchesFunctions::DsCpcStringify(
    FunctionContext* ctx, const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  try {
    auto sketch = datasketches::cpc_sketch::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    string str = sketch.to_string();
    StringVal dst(ctx, str.size());
    memcpy(dst.ptr, str.c_str(), str.size());
    return dst;
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return StringVal::null();
  }
}

StringVal DataSketchesFunctions::DsCpcUnionF(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  datasketches::cpc_union union_sketch;
  // Update two sketches to cpc_union
  if (!update_sketch_to_cpc_union(ctx, first_serialized_sketch, union_sketch)) {
    return StringVal::null();
  }
  if (!update_sketch_to_cpc_union(ctx, second_serialized_sketch, union_sketch)) {
    return StringVal::null();
  }
  auto bytes = union_sketch.get_result().serialize();
  StringVal result(ctx, bytes.size());
  memcpy(result.ptr, bytes.data(), bytes.size());
  return result;
}

BigIntVal DataSketchesFunctions::DsThetaEstimate(
    FunctionContext* ctx, const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return 0;
  try {
    auto sketch = datasketches::compact_theta_sketch::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    return sketch.get_estimate();
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return BigIntVal::null();
  }
}

StringVal DataSketchesFunctions::DsThetaExclude(FunctionContext* ctx,
    const StringVal& serialized_sketch1, const StringVal& serialized_sketch2) {
  // Note, A and B refer to the two input sketches in the order A-not-B.
  // if A is null return null.
  // if A is not null, B is null return copyA.
  // other return A-not-B.
  if (!serialized_sketch1.is_null && serialized_sketch1.len > 0) {
    if (serialized_sketch2.is_null || serialized_sketch2.len == 0) {
      return StringVal::CopyFrom(ctx, serialized_sketch1.ptr, serialized_sketch1.len);
    }
    // A and B are not null, call a_not_b.compute()
    datasketches::theta_a_not_b a_not_b;
    try {
      auto bytes = a_not_b.compute(
        datasketches::compact_theta_sketch::deserialize(serialized_sketch1.ptr,
            serialized_sketch1.len),
        datasketches::compact_theta_sketch::deserialize(serialized_sketch2.ptr,
            serialized_sketch2.len)
      ).serialize();
      StringVal result(ctx, bytes.size());
      memcpy(result.ptr, bytes.data(), bytes.size());
      return result;
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
    }
  }
  return StringVal::null();
}

StringVal DataSketchesFunctions::DsThetaUnionF(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  datasketches::theta_union union_sketch = datasketches::theta_union::builder().build();
  // Update two sketches to theta_union
  if (!update_sketch_to_theta_union(ctx, first_serialized_sketch, union_sketch)) {
    return StringVal::null();
  }
  if (!update_sketch_to_theta_union(ctx, second_serialized_sketch, union_sketch)) {
    return StringVal::null();
  }
  auto bytes = union_sketch.get_result().serialize();
  StringVal result(ctx, bytes.size());
  memcpy(result.ptr, bytes.data(), bytes.size());
  return result;
}

StringVal DataSketchesFunctions::DsThetaIntersectF(FunctionContext* ctx,
    const StringVal& first_serialized_sketch, const StringVal& second_serialized_sketch) {
  datasketches::theta_intersection intersection_sketch;
  // Update two sketches to theta_intersection
  // Note that if one of the sketches is null, null is returned.
  if (!update_sketch_to_theta_intersection(
          ctx, first_serialized_sketch, intersection_sketch)) {
    return StringVal::null();
  }
  if (!update_sketch_to_theta_intersection(
          ctx, second_serialized_sketch, intersection_sketch)) {
    return StringVal::null();
  }
  auto bytes = intersection_sketch.get_result().serialize();
  StringVal result(ctx, bytes.size());
  memcpy(result.ptr, bytes.data(), bytes.size());
  return result;
}

FloatVal DataSketchesFunctions::DsKllQuantile(FunctionContext* ctx,
    const StringVal& serialized_sketch, const DoubleVal& rank) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return FloatVal::null();
  if (rank.val < 0.0 || rank.val > 1.0) {
    ctx->SetError("Rank parameter should be in the range of [0,1]");
    return FloatVal::null();
  }
  try {
    auto sketch = datasketches::kll_sketch<float>::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
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
  try {
    auto sketch = datasketches::kll_sketch<float>::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    return sketch.get_n();
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return BigIntVal::null();
  }
}

DoubleVal DataSketchesFunctions::DsKllRank(FunctionContext* ctx,
    const StringVal& serialized_sketch, const FloatVal& probe_value) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return DoubleVal::null();
  try {
    auto sketch = datasketches::kll_sketch<float>::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    return sketch.get_rank(probe_value.val);
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return DoubleVal::null();
  }
}

StringVal DataSketchesFunctions::DsKllQuantilesAsString(FunctionContext* ctx,
    const StringVal& serialized_sketch, int num_args, const DoubleVal* args) {
  DCHECK(num_args > 0);
  if (args == nullptr) return StringVal::null();
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  if (RaiseErrorForNullOrNaNInput(ctx, num_args, args)) return StringVal::null();
  try {
    auto sketch = datasketches::kll_sketch<float>::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    double quantiles_input[(unsigned int)num_args];
    for (int i = 0; i < num_args; ++i) quantiles_input[i] = args[i].val;
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
  try {
    auto sketch = datasketches::kll_sketch<float>::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    float input_ranges[(unsigned int)num_args];
    for (int i = 0; i < num_args; ++i) input_ranges[i] = args[i].val;
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

StringVal DataSketchesFunctions::DsKllStringify( FunctionContext* ctx,
    const StringVal& serialized_sketch) {
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return StringVal::null();
  try {
    auto sketch = datasketches::kll_sketch<float>::deserialize(serialized_sketch.ptr,
        serialized_sketch.len);
    string str = sketch.to_string(false, false);
    StringVal dst(ctx, str.size());
    memcpy(dst.ptr, str.c_str(), str.size());
    return dst;
  } catch (const std::exception& e) {
    LogSketchDeserializationError(ctx, e);
    return StringVal::null();
  }
}

}
