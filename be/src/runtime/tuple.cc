// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/tuple.h"

#include <vector>

#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

using namespace std;

namespace impala {

Tuple* Tuple::DeepCopy(const TupleDescriptor& desc, MemPool* pool) {
  Tuple* result = reinterpret_cast<Tuple*>(pool->Allocate(desc.byte_size()));
  memcpy(result, this, desc.byte_size());
  // allocate in the same pool and then copy all string slots
  for (vector<SlotDescriptor*>::const_iterator i = desc.slots().begin();
       i != desc.slots().end(); ++i) {
    if ((*i)->type() == TYPE_STRING) {
      StringValue* string_v = result->GetStringSlot((*i)->tuple_offset());
      char* string_copy = pool->Allocate(string_v->len);
      memcpy(string_copy, string_v->ptr, string_v->len);
      string_v->ptr = string_copy;
    }
  }
  return result;
}

}
