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

#include "common/status.h"
#include "thirdparty/datasketches/hll.hpp"
#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::StringVal;

/// Config for DataSketches HLL algorithm to set the size of each entry within the
/// sketch.
const datasketches::target_hll_type DS_HLL_TYPE = datasketches::target_hll_type::HLL_4;

/// Config for DataSketches HLL algorithm to set the number of buckets to hold. Number of
/// buckets equals 2^DS_SKETCH_CONFIG.
const int DS_SKETCH_CONFIG = 12;

/// Logs a common error message saying that sketch deserialization failed.
void LogSketchDeserializationError(FunctionContext* ctx);

/// Receives a serialized DataSketches sketch (either Hll or KLL) in
/// 'serialized_sketch', deserializes it and puts the deserialized sketch into 'sketch'.
/// The outgoing 'sketch' will hold the same configs as 'serialized_sketch' regardless of
/// what was provided when it was constructed before this function call. Returns false if
/// the deserialization fails, true otherwise.
template<class T>
bool DeserializeDsSketch(const StringVal& serialized_sketch, T* sketch)
    WARN_UNUSED_RESULT;

/// Helper function that receives an std::stringstream and converts it to StringVal. Uses
/// 'ctx' for memory allocation.
StringVal StringStreamToStringVal(FunctionContext* ctx,
    const std::stringstream& str_stream);
}

