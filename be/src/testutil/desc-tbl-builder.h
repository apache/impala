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

#ifndef IMPALA_TESTUTIL_ROW_DESC_BUILDER_H_
#define IMPALA_TESTUTIL_ROW_DESC_BUILDER_H_

#include <vector>

#include "runtime/runtime-state.h"
#include "runtime/types.h"
#include "gen-cpp/Descriptors_types.h"

namespace impala {

class Frontend;
class ObjectPool;
class TupleDescBuilder;
class DescriptorTbl;

/// Aids in the construction of a DescriptorTbl by declaring tuples and slots
/// associated with those tuples.
/// The descriptor table is constructed by calling into the FE via JNI, such that
/// the tuple mem layouts mimic real queries. All id assignments happen in the FE.
///
/// Example usage:
/// DescriptorTblBuilder builder;
/// builder.DeclareTuple() << TYPE_TINYINT << TYPE_TIMESTAMP;
/// builder.DeclareTuple() << TYPE_FLOAT;
/// DescriptorTbl desc_tbl = builder.Build();
class DescriptorTblBuilder {
 public:
  DescriptorTblBuilder(Frontend* fe, ObjectPool* object_pool);

  TupleDescBuilder& DeclareTuple();

  // Allows to set a TableDescriptor on TDescriptorTable.
  // Only one can be set.
  void SetTableDescriptor(const TTableDescriptor& table_desc);

  DescriptorTbl* Build();

 private:
  /// Both owned by caller.
  Frontend* fe_;
  ObjectPool* obj_pool_;

  std::vector<TupleDescBuilder*> tuples_descs_;
  TDescriptorTable thrift_desc_tbl_;
};

class TupleDescBuilder {
 public:
  TupleDescBuilder& operator<< (const ColumnType& slot_type) {
    slot_types_.push_back(slot_type);
    return *this;
  }

  TupleDescBuilder& operator<< (PrimitiveType slot_type) {
    return *this << ColumnType(slot_type);
  }

  std::vector<ColumnType> slot_types() const { return slot_types_; }

 private:
  std::vector<ColumnType> slot_types_;
};

}

#endif
