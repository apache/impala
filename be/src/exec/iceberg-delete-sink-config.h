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

#pragma once

#include <map>
#include "exec/table-sink-base.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class IcebergDeleteSinkConfig : public TableSinkBaseConfig {
 public:
  DataSink* CreateSink(RuntimeState* state) const override;

  ~IcebergDeleteSinkConfig() override {}

  std::map<THash128, TIcebergDeletionVector> referenced_deletion_vectors;

 protected:
  Status Init(const TDataSink& tsink, const RowDescriptor* input_row_desc,
      FragmentState* state) override;
};

}