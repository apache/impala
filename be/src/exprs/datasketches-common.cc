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

#include "datasketches-common.h"

#include "common/logging.h"
#include "udf/udf-internal.h"
#include "gutil/strings/substitute.h"
#include "thirdparty/datasketches/cpc_sketch.hpp"
#include "thirdparty/datasketches/kll_sketch.hpp"
#include "thirdparty/datasketches/theta_sketch.hpp"

namespace impala {

using datasketches::hll_sketch;
using datasketches::cpc_sketch;
using datasketches::kll_sketch;
using datasketches::theta_sketch;
using datasketches::compact_theta_sketch;
using impala_udf::StringVal;
using std::stringstream;
using std::vector;

void LogSketchDeserializationError(FunctionContext* ctx, const std::exception& e) {
  ctx->SetError(strings::Substitute("Unable to deserialize sketch: $0",
      e.what()).c_str());
}

StringVal StringStreamToStringVal(FunctionContext* ctx, const stringstream& str_stream) {
  string str = str_stream.str();
  StringVal dst(ctx, str.size());
  memcpy(dst.ptr, str.c_str(), str.size());
  return dst;
}

bool update_sketch_to_cpc_union(FunctionContext* ctx,
    const StringVal& serialized_sketch, datasketches::cpc_union& union_sketch) {
  if (!serialized_sketch.is_null && serialized_sketch.len > 0) {
    try {
      union_sketch.update(datasketches::cpc_sketch::deserialize(serialized_sketch.ptr,
          serialized_sketch.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return false;
    }
  }
  return true;
}

bool update_sketch_to_theta_union(FunctionContext* ctx,
    const StringVal& serialized_sketch, datasketches::theta_union& sketch) {
  if (!serialized_sketch.is_null && serialized_sketch.len > 0) {
    try {
      sketch.update(datasketches::compact_theta_sketch::deserialize(serialized_sketch.ptr,
          serialized_sketch.len));
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
      return false;
    }
  }
  return true;
}

bool update_sketch_to_theta_intersection(FunctionContext* ctx,
    const StringVal& serialized_sketch, datasketches::theta_intersection& sketch) {
  if (!serialized_sketch.is_null && serialized_sketch.len > 0) {
    try {
      sketch.update(datasketches::compact_theta_sketch::deserialize(serialized_sketch.ptr,
          serialized_sketch.len));
      return true;
    } catch (const std::exception& e) {
      LogSketchDeserializationError(ctx, e);
    }
  }
  return false;
}

template<class T>
StringVal DsKllVectorResultToStringVal(FunctionContext* ctx,
    const vector<T>& kll_result) {
  std::stringstream result_stream;
  for(int i = 0; i < kll_result.size(); ++i) {
    if (i > 0) result_stream << ",";
    result_stream << kll_result[i];
  }
  return StringStreamToStringVal(ctx, result_stream);
}

template StringVal DsKllVectorResultToStringVal(FunctionContext* ctx,
    const vector<float>& kll_result);
template StringVal DsKllVectorResultToStringVal(FunctionContext* ctx,
    const vector<double>& kll_result);

template<class T>
bool RaiseErrorForNullOrNaNInput(FunctionContext* ctx, int num_args, const T* args) {
  DCHECK(num_args > 0);
  DCHECK(args != nullptr);
  for (int i = 0; i < num_args; ++i) {
    if (args[i].is_null || std::isnan(args[i].val)) {
      ctx->SetError("NULL or NaN provided in the input list.");
      return true;
    }
  }
  return false;
}

template bool RaiseErrorForNullOrNaNInput(FunctionContext* ctx, int num_args,
    const DoubleVal* args);
template bool RaiseErrorForNullOrNaNInput(FunctionContext* ctx, int num_args,
    const FloatVal* args);

}
