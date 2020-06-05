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

namespace impala {

using datasketches::hll_sketch;
using impala_udf::StringVal;

void LogSketchDeserializationError(FunctionContext* ctx) {
  ctx->SetError("Unable to deserialize sketch.");
}

bool DeserializeHllSketch(const StringVal& serialized_sketch, hll_sketch* sketch) {
  DCHECK(sketch != nullptr);
  if (serialized_sketch.is_null || serialized_sketch.len == 0) return false;
  try {
    *sketch = hll_sketch::deserialize((void*)serialized_sketch.ptr,
        serialized_sketch.len);
    return true;
  } catch (const std::invalid_argument&) {
    // Deserialization throws if the input string is not a serialized sketch.
    return false;
  }
}

}

