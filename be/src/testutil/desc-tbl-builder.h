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

#ifndef IMPALA_TESTUTIL_ROW_DESC_BUILDER_H_
#define IMPALA_TESTUTIL_ROW_DESC_BUILDER_H_

#include "runtime/runtime-state.h"

namespace impala {

class ObjectPool;
class TupleDescBuilder;

/// Aids in the construction of a DescriptorTbl by declaring tuples and slots
/// associated with those tuples.
/// TupleIds are monotonically increasing from 0 for each DeclareTuple, and
/// SlotIds increase similarly, but are always greater than all TupleIds.
/// Unlike FE, slots are not reordered based on size, and padding is not addded.
//
/// Example usage:
/// DescriptorTblBuilder builder;
/// builder.DeclareTuple() << TYPE_TINYINT << TYPE_TIMESTAMP; // gets TupleId 0
/// builder.DeclareTuple() << TYPE_FLOAT; // gets TupleId 1
/// DescriptorTbl desc_tbl = builder.Build();
class DescriptorTblBuilder {
 public:
  DescriptorTblBuilder(ObjectPool* object_pool);

  TupleDescBuilder& DeclareTuple();
  DescriptorTbl* Build();

 private:
  /// Owned by caller.
  ObjectPool* obj_pool_;

  std::vector<TupleDescBuilder*> tuples_descs_;

  TTupleDescriptor BuildTuple(
      const std::vector<ColumnType>& slot_types, TDescriptorTable* thrift_desc_tbl,
      int* tuple_id, int* slot_id);
};

class TupleDescBuilder {
 public:
  TupleDescBuilder& operator<< (const ColumnType& slot_type) {
    slot_types_.push_back(slot_type);
    return *this;
  }

  std::vector<ColumnType> slot_types() const { return slot_types_; }

 private:
  std::vector<ColumnType> slot_types_;
};

}

#endif
