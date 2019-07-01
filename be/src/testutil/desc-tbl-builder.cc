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

#include "testutil/desc-tbl-builder.h"
#include "util/bit-util.h"

#include "common/object-pool.h"
#include "service/frontend.h"
#include "runtime/descriptors.h"

#include "common/names.h"

namespace impala {

DescriptorTblBuilder::DescriptorTblBuilder(Frontend* fe, ObjectPool* obj_pool)
  : fe_(fe), obj_pool_(obj_pool) {
  DCHECK(fe != NULL);
  DCHECK(obj_pool_ != NULL);
}

TupleDescBuilder& DescriptorTblBuilder::DeclareTuple() {
  TupleDescBuilder* tuple_builder = obj_pool_->Add(new TupleDescBuilder());
  tuples_descs_.push_back(tuple_builder);
  return *tuple_builder;
}

void DescriptorTblBuilder::SetTableDescriptor(const TTableDescriptor& table_desc) {
  DCHECK(thrift_desc_tbl_.tableDescriptors.empty())
      << "Only one TableDescriptor can be set.";
  thrift_desc_tbl_.tableDescriptors.push_back(table_desc);
}

DescriptorTbl* DescriptorTblBuilder::Build() {
  DCHECK(!tuples_descs_.empty());

  TBuildTestDescriptorTableParams params;
  for (int i = 0; i < tuples_descs_.size(); ++i) {
    params.slot_types.push_back(vector<TColumnType>());
    vector<TColumnType>& tslot_types = params.slot_types.back();
    const vector<ColumnType>& slot_types = tuples_descs_[i]->slot_types();
    for (const ColumnType& slot_type : slot_types) {
      tslot_types.push_back(slot_type.ToThrift());
    }
  }

  Status buildDescTblStatus = fe_->BuildTestDescriptorTable(params, &thrift_desc_tbl_);
  DCHECK(buildDescTblStatus.ok()) << buildDescTblStatus.GetDetail();

  DescriptorTbl* desc_tbl;
  Status status = DescriptorTbl::CreateInternal(obj_pool_, thrift_desc_tbl_, &desc_tbl);
  DCHECK(status.ok()) << status.GetDetail();
  return desc_tbl;
}

}
