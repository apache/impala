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

#include "exprs/slot-ref.h"
#include "runtime/collection-value.h"
#include "runtime/tuple-row.h"

namespace impala {

CollectionVal SlotRef::GetCollectionVal(
    ScalarExprEvaluator* eval, const TupleRow* row) const {
  DCHECK(type_.IsCollectionType());
  Tuple* t = row->GetTuple(tuple_idx_);
  if (t == NULL || t->IsNull(null_indicator_offset_)) return CollectionVal::null();
  CollectionValue* coll_value =
      reinterpret_cast<CollectionValue*>(t->GetSlot(slot_offset_));
  return CollectionVal(coll_value->ptr, coll_value->num_tuples);
}

} // namespace impala