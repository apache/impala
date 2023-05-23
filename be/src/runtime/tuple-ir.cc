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

#include "runtime/tuple.h"

#include "runtime/string-value.h"

namespace impala {

// Used to force the compilation of the CodegenTypes struct.
void Tuple::dummy(Tuple::CodegenTypes*) {}

bool Tuple::CopyStrings(const char* err_ctx, RuntimeState* state,
    const SlotOffsets* string_slot_offsets, int num_string_slots, MemPool* pool,
    Status* status) noexcept {
  int64_t total_len = 0;
  for (int i = 0; i < num_string_slots; ++i) {
    if (IsNull(string_slot_offsets[i].null_indicator_offset)) continue;
    StringValue* sv = GetStringSlot(string_slot_offsets[i].tuple_offset);
    if (sv->IsSmall()) continue;
    total_len += sv->Len();
  }
  char* buf = AllocateStrings(err_ctx, state, total_len, pool, status);
  if (UNLIKELY(buf == nullptr)) return false;
  for (int i = 0; i < num_string_slots; ++i) {
    if (IsNull(string_slot_offsets[i].null_indicator_offset)) continue;
    StringValue* sv = GetStringSlot(string_slot_offsets[i].tuple_offset);
    if (sv->IsSmall()) continue;
    StringValue::SimpleString s = sv->ToSimpleString();
    if (s.len == 0) continue;
    memcpy(buf, s.ptr, s.len);
    sv->SetPtr(buf);
    buf += s.len;
  }
  return true;
}
}
