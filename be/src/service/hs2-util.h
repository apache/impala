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

#include "gen-cpp/ImpalaHiveServer2Service.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/TCLIService_types.h"

namespace impala {

class RowBatch;
class ScalarExprEvaluator;

typedef apache::hive::service::cli::thrift::TColumnValue TColumnValueHive;

/// Utility methods for converting from Impala (either an Expr result or a TColumnValue)
/// to Hive types (either a thrift::TColumnValue (V1->V5) or a TColumn (V6->).

/// For V6->
void TColumnValueToHS2TColumn(const TColumnValue& col_val, const TColumnType& type,
    uint32_t row_idx, apache::hive::service::cli::thrift::TColumn* column);

/// Evaluate 'expr_eval' over the row [start_idx, start_idx + num_rows) from 'batch' into
/// 'column' with 'type' starting at output_row_idx. The caller is responsible for
/// calling RuntimeState::GetQueryStatus() to check for expression evaluation errors. If
/// 'stringify_map_keys' is true, converts map keys to strings; see IMPALA-11778.
/// 'expected_result_count' is used for reserving space in the result vectors.
/// For V6->
void ExprValuesToHS2TColumn(ScalarExprEvaluator* expr_eval, const TColumnType& type,
    RowBatch* batch, int start_idx, int num_rows, uint32_t output_row_idx,
     int expected_result_count, bool stringify_map_keys,
     apache::hive::service::cli::thrift::TColumn* column);

/// For V1->V5
void TColumnValueToHS2TColumnValue(const TColumnValue& col_val, const TColumnType& type,
    apache::hive::service::cli::thrift::TColumnValue* hs2_col_val);

/// For V1->V5
void ExprValueToHS2TColumnValue(const void* value, const TColumnType& type,
    apache::hive::service::cli::thrift::TColumnValue* hs2_col_val);

/// Combine two null columns by appending 'from' to 'to', starting at 'num_rows_before' in
/// 'from', 'start_idx' in 'to', and proceeding for 'num_rows_added' rows.
void StitchNulls(uint32_t num_rows_before, uint32_t num_rows_added, uint32_t start_idx,
    const std::string& from, std::string* to);

void PrintTColumnValue(const apache::hive::service::cli::thrift::TColumnValue& colval,
    std::stringstream* out);

/// Utility method for converting from Hive TColumnValue to Impala TColumnValue.
TColumnValue ConvertToTColumnValue(
    const apache::hive::service::cli::thrift::TColumnDesc& desc,
    const apache::hive::service::cli::thrift::TColumnValue& hive_colval);

/// Utility method for printing Impala TColumnValue.
void PrintTColumnValue(const impala::TColumnValue& colval, std::stringstream* out);
std::string PrintTColumnValue(const impala::TColumnValue& colval);

/// Return true if one field in value is set. Return false otherwise.
bool isOneFieldSet(const impala::TColumnValue& value);

apache::hive::service::cli::thrift::TTypeEntry ColumnToHs2Type(
    const TColumnType& columnType);
}
