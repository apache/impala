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

#include <ostream>
#include <utility>

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

Status VariantSlotToJson(const VariantSlot* slot, string* json_out) {
  DCHECK(slot != nullptr);
  return VariantToJson(slot->metadata.UPtr(), slot->metadata.Len(),
      slot->value.UPtr(), slot->value.Len(), json_out);
}

Status VariantSlotToJson(const VariantSlot* slot, std::ostream* out) {
  DCHECK(slot != nullptr);
  VariantMetadata metadata;
  RETURN_IF_ERROR(metadata.Init(slot->metadata.UPtr(), slot->metadata.Len()));
  VariantValue value(slot->value.UPtr(), slot->value.Len(), &metadata);
  return value.ToJson(out);
}

}  // namespace impala
