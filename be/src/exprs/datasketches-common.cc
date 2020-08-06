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
#include "thirdparty/datasketches/kll_sketch.hpp"

namespace impala {

using datasketches::hll_sketch;
using datasketches::kll_sketch;
using impala_udf::StringVal;
using std::stringstream;

void LogSketchDeserializationError(FunctionContext* ctx) {
  ctx->SetError("Unable to deserialize sketch.");
}

template<class T>
bool DeserializeDsSketch(const StringVal& serialized_sketch, T* sketch) {
  DCHECK(sketch != nullptr);
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return false;
  try {
    *sketch = T::deserialize((void*)serialized_sketch.ptr, serialized_sketch.len);
    return true;
  } catch (const std::exception&) {
    // One reason of throwing from deserialization is that the input string is not a
    // serialized sketch.
    return false;
  }
}

template bool DeserializeDsSketch(const StringVal& serialized_sketch,
    hll_sketch* sketch);
template bool DeserializeDsSketch(const StringVal& serialized_sketch,
    kll_sketch<float>* sketch);

StringVal StringStreamToStringVal(FunctionContext* ctx, const stringstream& str_stream) {
  string str = str_stream.str();
  StringVal dst(ctx, str.size());
  memcpy(dst.ptr, str.c_str(), str.size());
  return dst;
}

}

