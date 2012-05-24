// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

}
