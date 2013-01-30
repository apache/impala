// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime/tuple.h"

#include <vector>

#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"
#include "util/debug-util.h"

using namespace std;

namespace impala {

  const char* Tuple::LLVM_CLASS_NAME = "class.impala::Tuple";

Tuple* Tuple::DeepCopy(const TupleDescriptor& desc, MemPool* pool, bool convert_ptrs) {
  Tuple* result = reinterpret_cast<Tuple*>(pool->Allocate(desc.byte_size()));
  DeepCopy(result, desc, pool, convert_ptrs);
  return result;
}

void Tuple::DeepCopy(Tuple* dst, const TupleDescriptor& desc, MemPool* pool,
                     bool convert_ptrs) {
  memcpy(dst, this, desc.byte_size());
  // allocate in the same pool and then copy all non-null string slots
  for (vector<SlotDescriptor*>::const_iterator i = desc.string_slots().begin();
       i != desc.string_slots().end(); ++i) {
    DCHECK_EQ((*i)->type(), TYPE_STRING);
    if (!dst->IsNull((*i)->null_indicator_offset())) {
      StringValue* string_v = dst->GetStringSlot((*i)->tuple_offset());
      int offset = pool->GetCurrentOffset();
      char* string_copy = reinterpret_cast<char*>(pool->Allocate(string_v->len));
      memcpy(string_copy, string_v->ptr, string_v->len);
      string_v->ptr = (convert_ptrs ? reinterpret_cast<char*>(offset) : string_copy);
    }
  }
}

void Tuple::DeepCopy(const TupleDescriptor& desc, char** data, int* offset,
                     bool convert_ptrs) {
  Tuple* dst = reinterpret_cast<Tuple*>(*data);
  memcpy(dst, this, desc.byte_size());
  *data += desc.byte_size();
  *offset += desc.byte_size();
  for (vector<SlotDescriptor*>::const_iterator i = desc.string_slots().begin();
       i != desc.string_slots().end(); ++i) {
    DCHECK_EQ((*i)->type(), TYPE_STRING);
    if (!dst->IsNull((*i)->null_indicator_offset())) {
      StringValue* string_v = dst->GetStringSlot((*i)->tuple_offset());
      memcpy(*data, string_v->ptr, string_v->len);
      string_v->ptr = (convert_ptrs ? reinterpret_cast<char*>(*offset) : *data);
      *data += string_v->len;
      *offset += string_v->len;
    }
  }
}

}
