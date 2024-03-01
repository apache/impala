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

#include "exec/iceberg-delete-sink-config.h"

#include "common/object-pool.h"
#include "common/status.h"
#include "exec/iceberg-buffered-delete-sink.h"
#include "exprs/scalar-expr.h"
#include "runtime/mem-pool.h"

namespace impala {

DataSink* IcebergDeleteSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
    new IcebergBufferedDeleteSink(sink_id, *this, state));
}

Status IcebergDeleteSinkConfig::Init(
    const TDataSink& tsink, const RowDescriptor* input_row_desc, FragmentState* state) {
  RETURN_IF_ERROR(DataSinkConfig::Init(tsink, input_row_desc, state));
  DCHECK(tsink_->__isset.table_sink);
  DCHECK(tsink_->table_sink.__isset.iceberg_delete_sink);
  RETURN_IF_ERROR(
      ScalarExpr::Create(tsink_->table_sink.iceberg_delete_sink.partition_key_exprs,
          *input_row_desc_, state, &partition_key_exprs_));
  return Status::OK();
}

}
