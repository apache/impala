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

#include "util/variant-util.h"

#include <rapidjson/writer.h>

#include "common/names.h"
#include "runtime/variant-value.h"

namespace impala {

Status VariantToJson(const uint8_t* metadata_data, uint32_t metadata_len,
    const uint8_t* value_data, uint32_t value_len, string* json_out) {
  VariantMetadata metadata;
  RETURN_IF_ERROR(metadata.Init(metadata_data, metadata_len));
  VariantValue value(value_data, value_len, &metadata);
  return value.ToJson(json_out);
}

Status VariantToJson(impala_udf::FunctionContext* ctx,
    const uint8_t* metadata_data, uint32_t metadata_len,
    const uint8_t* value_data, uint32_t value_len,
    impala_udf::StringVal* result) {
  VariantMetadata metadata;
  RETURN_IF_ERROR(metadata.Init(metadata_data, metadata_len));
  VariantValue value(value_data, value_len, &metadata);
  return value.ToJson(ctx, result);
}

}  // namespace impala
