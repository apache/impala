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

#include <sstream>
#include <vector>

#include "common/status.h"
#include "thirdparty/datasketches/hll.hpp"
#include "thirdparty/datasketches/cpc_union.hpp"
#include "thirdparty/datasketches/theta_union.hpp"
#include "thirdparty/datasketches/theta_intersection.hpp"
#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::DoubleVal;
using impala_udf::FloatVal;
using impala_udf::StringVal;

/// Config for DataSketches HLL algorithm to set the size of each entry within the
/// sketch.
const datasketches::target_hll_type DS_HLL_TYPE = datasketches::target_hll_type::HLL_4;

/// Config for DataSketches HLL algorithm to set the number of buckets to hold. Number of
/// buckets equals 2^DS_SKETCH_CONFIG.
const int DS_SKETCH_CONFIG = 12;

/// Similar to DS_SKETCH_CONFIG, the value must be between 4 and 21. Note that CPC is
/// configured as 11 because it is comparable to an HLL sketch of 12.
const int DS_CPC_SKETCH_CONFIG = 11;

/// 'kappa' is a number of standard deviations from the mean: 1, 2 or 3 (default 2).
const int DS_DEFAULT_KAPPA = 2;

/// Logs a common error message saying that sketch deserialization failed.
void LogSketchDeserializationError(FunctionContext* ctx, const std::exception& e);

/// Auxiliary function that receives a sketch and returns the serialized version of
/// it wrapped into a StringVal.
template<typename Sketch>
StringVal SerializeDsSketch(FunctionContext* ctx, const Sketch& sketch) {
  auto bytes = sketch.serialize();
  StringVal result(ctx, bytes.size());
  memcpy(result.ptr, bytes.data(), bytes.size());
  return result;
}

/// Helper function that receives an std::stringstream and converts it to StringVal. Uses
/// 'ctx' for memory allocation.
StringVal StringStreamToStringVal(FunctionContext* ctx,
    const std::stringstream& str_stream);

/// Helper function that receives a serialized DataSketches CPC sketch in
/// 'serialized_sketch', deserializes it and update the deserialized sketch to 'sketch'.
/// Returns false if the deserialization fails (the error log will be written),
/// true otherwise.
bool update_sketch_to_cpc_union(FunctionContext* ctx, const StringVal& serialized_sketch,
    datasketches::cpc_union& sketch);

/// Helper function that receives a serialized DataSketches Theta sketch in
/// 'serialized_sketch', deserializes it and update the deserialized sketch to 'sketch'.
/// Returns false if the deserialization fails (the error log will be written),
/// true otherwise.
bool update_sketch_to_theta_union(FunctionContext* ctx,
    const StringVal& serialized_sketch, datasketches::theta_union& sketch);

/// Helper function that receives a serialized DataSketches Theta sketch in
/// 'serialized_sketch', deserializes it and update the deserialized sketch to 'sketch'.
/// Returns false if 'serialized_sketch' is empty or deserialization fails (the error log
/// will be written), true otherwise.
bool update_sketch_to_theta_intersection(FunctionContext* ctx,
    const StringVal& serialized_sketch, datasketches::theta_intersection& sketch);

/// Helper function that receives a vector and returns a comma separated StringVal that
/// holds the items from the vector keeping the order.
template<class T>
StringVal DsKllVectorResultToStringVal(FunctionContext* ctx,
    const std::vector<T>& kll_result);

/// Helper function to check if there is a NULL or NaN in the array represented by
/// 'num_args' and 'args'. Sets an error to 'ctx' if NULL or NaN is found and returns
/// true. Returns false otherwise.
template<class T>
bool RaiseErrorForNullOrNaNInput(FunctionContext* ctx, int num_args, const T* args);

}
