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

#include "service/query-result-set.h"

#include <sstream>
#include <boost/scoped_ptr.hpp>

#include <rapidjson/ostreamwrapper.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/writer.h>

#include "exprs/scalar-expr-evaluator.h"
#include "exprs/slot-ref.h"
#include "rpc/thrift-util.h"
#include "runtime/complex-value-writer.inline.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"
#include "runtime/types.h"
#include "service/hs2-util.h"
#include "util/bit-util.h"

#include "common/names.h"

using ThriftTColumn = apache::hive::service::cli::thrift::TColumn;
using ThriftTColumnValue = apache::hive::service::cli::thrift::TColumnValue;
using apache::hive::service::cli::thrift::TProtocolVersion;
using apache::hive::service::cli::thrift::TRow;
using apache::hive::service::cli::thrift::TRowSet;

namespace {

/// Ascii output precision for double/float
constexpr int ASCII_PRECISION = 16;
}

namespace impala {

/// Ascii result set for Beeswax. Rows are returned in ascii text encoding, using "\t" as
/// column delimiter.
class AsciiQueryResultSet : public QueryResultSet {
 public:
  /// Rows are added into 'rowset'.
  AsciiQueryResultSet(const TResultSetMetadata& metadata, vector<string>* rowset,
      bool stringify_map_keys)
    : metadata_(metadata), result_set_(rowset), stringify_map_keys_(stringify_map_keys) {
    types_.reserve(metadata.columns.size());
    for (int i = 0; i < metadata.columns.size(); ++i) {
      types_.push_back(ColumnType::FromThrift(metadata_.columns[i].columnType));
    }
  }

  virtual ~AsciiQueryResultSet() {}

  /// Evaluate 'expr_evals' over rows in 'batch', convert to ASCII using "\t" as column
  /// delimiter and store it in this result set.
  /// TODO: Handle complex types.
  virtual Status AddRows(const vector<ScalarExprEvaluator*>& expr_evals, RowBatch* batch,
      int start_idx, int num_rows) override;

  /// Convert TResultRow to ASCII using "\t" as column delimiter and store it in this
  /// result set.
  virtual Status AddOneRow(const TResultRow& row) override;

  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) override;
  virtual int64_t ByteSize(int start_idx, int num_rows) override;
  virtual size_t size() override { return result_set_->size(); }

 private:
  /// Metadata of the result set
  const TResultSetMetadata& metadata_;

  /// Points to the result set to be filled. Not owned by this object.
  vector<string>* result_set_;

  // If true, converts map keys to strings; see IMPALA-11778.
  const bool stringify_map_keys_;

  // De-serialized column metadata
  vector<ColumnType> types_;
};

/// Result set container for Hive protocol versions >= V6, where results are returned in
/// column-orientation.
class HS2ColumnarResultSet : public QueryResultSet {
 public:
  HS2ColumnarResultSet(const TResultSetMetadata& metadata, TRowSet* rowset,
      bool stringify_map_keys, int expected_result_count);

  virtual ~HS2ColumnarResultSet() {}

  /// Evaluate 'expr_evals' over rows in 'batch' and convert to the HS2 columnar
  /// representation.
  virtual Status AddRows(const vector<ScalarExprEvaluator*>& expr_evals, RowBatch* batch,
      int start_idx, int num_rows) override;

  /// Add a row from a TResultRow
  virtual Status AddOneRow(const TResultRow& row) override;

  /// Copy all columns starting at 'start_idx' and proceeding for a maximum of 'num_rows'
  /// from 'other' into this result set
  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) override;

  virtual int64_t ByteSize(int start_idx, int num_rows) override;
  virtual size_t size() override { return num_rows_; }

 private:
  /// Metadata of the result set
  const TResultSetMetadata& metadata_;

  /// Points to the TRowSet to be filled. The row set
  /// this points to may be owned by
  /// this object, in which case owned_result_set_ is set.
  TRowSet* result_set_;

  /// Set to result_set_ if result_set_ is owned.
  boost::scoped_ptr<TRowSet> owned_result_set_;

  int64_t num_rows_;

  // If true, converts map keys to strings; see IMPALA-11778.
  const bool stringify_map_keys_;

  // Expected number of result rows that will be returned with this
  // fetch request. Used to reserve results vector memory.
  const int expected_result_count_;

  void InitColumns();
};

/// Row oriented result set for HiveServer2, used to serve HS2 requests with protocol
/// version <= V5.
class HS2RowOrientedResultSet : public QueryResultSet {
 public:
  /// Rows are added into rowset.
  HS2RowOrientedResultSet(const TResultSetMetadata& metadata, TRowSet* rowset);

  virtual ~HS2RowOrientedResultSet() {}

  /// Evaluate 'expr_evals' over rows in 'batch' and convert to the HS2 row-oriented
  /// representation of TRows stored in a TRowSet.
  virtual Status AddRows(const vector<ScalarExprEvaluator*>& expr_evals, RowBatch* batch,
      int start_idx, int num_rows) override;

  /// Convert TResultRow to HS2 TRow and store it in a TRowSet
  virtual Status AddOneRow(const TResultRow& row) override;

  virtual int AddRows(const QueryResultSet* other, int start_idx, int num_rows) override;
  virtual int64_t ByteSize(int start_idx, int num_rows) override;
  virtual size_t size() override { return result_set_->rows.size(); }

 private:
  /// Metadata of the result set
  const TResultSetMetadata& metadata_;

  /// Points to the TRowSet to be filled. The row set
  /// this points to may be owned by
  /// this object, in which case owned_result_set_ is set.
  TRowSet* result_set_;

  /// Set to result_set_ if result_set_ is owned.
  scoped_ptr<TRowSet> owned_result_set_;
};

QueryResultSet* QueryResultSet::CreateAsciiQueryResultSet(
    const TResultSetMetadata& metadata, vector<string>* rowset, bool stringify_map_keys) {
  return new AsciiQueryResultSet(metadata, rowset, stringify_map_keys);
}

QueryResultSet* QueryResultSet::CreateHS2ResultSet(
    TProtocolVersion::type version, const TResultSetMetadata& metadata, TRowSet* rowset,
    bool stringify_map_keys, int expected_result_count) {
  if (version < TProtocolVersion::HIVE_CLI_SERVICE_PROTOCOL_V6) {
    return new HS2RowOrientedResultSet(metadata, rowset);
  } else {
    return new HS2ColumnarResultSet(
        metadata, rowset, stringify_map_keys, expected_result_count);
  }
}

//////////////////////////////////////////////////////////////////////////////////////////

Status AsciiQueryResultSet::AddRows(const vector<ScalarExprEvaluator*>& expr_evals,
    RowBatch* batch, int start_idx, int num_rows) {
  DCHECK_GE(batch->num_rows(), start_idx + num_rows);
  int num_col = expr_evals.size();
  DCHECK_EQ(num_col, metadata_.columns.size());
  vector<int> scales;
  scales.reserve(num_col);
  for (ScalarExprEvaluator* expr_eval : expr_evals) {
    scales.push_back(expr_eval->output_scale());
  }
  // Round up to power-of-two to avoid accidentally quadratic behaviour from repeated
  // small increases in size.
  result_set_->reserve(
      BitUtil::RoundUpToPowerOfTwo(result_set_->size() + num_rows - start_idx));
  stringstream out_stream;
  out_stream.precision(ASCII_PRECISION);
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    for (int i = 0; i < num_col; ++i) {
      // ODBC-187 - ODBC can only take "\t" as the delimiter
      out_stream << (i > 0 ? "\t" : "");

      if (!types_[i].IsComplexType()) {
        RawValue::PrintValue(expr_evals[i]->GetValue(it.Get()), types_[i],
            scales[i], &out_stream);
      } else {
        PrintComplexValue(expr_evals[i], it.Get(), &out_stream, types_[i],
            stringify_map_keys_);
      }
    }
    result_set_->push_back(out_stream.str());
    out_stream.str("");
  }
  return Status::OK();
}

void QueryResultSet::PrintComplexValue(ScalarExprEvaluator* expr_eval,
    const TupleRow* row, stringstream *stream, const ColumnType& type,
    bool stringify_map_keys) {
  DCHECK(type.IsComplexType());
  const ScalarExpr& scalar_expr = expr_eval->root();
  // Currently scalar_expr can be only a slot ref as no functions return complex types.
  DCHECK(scalar_expr.IsSlotRef());
  void* value = expr_eval->GetValue(row);

  if (value == nullptr) {
    (*stream) << RawValue::NullLiteral(/*top-level*/ true);
    return;
  }

  rapidjson::BasicOStreamWrapper<stringstream> wrapped_stream(*stream);
  rapidjson::Writer<rapidjson::BasicOStreamWrapper<stringstream>> writer(wrapped_stream);

  if (type.IsCollectionType()) {
    const CollectionValue* collection_val = static_cast<const CollectionValue*>(value);
    const TupleDescriptor* item_tuple_desc = scalar_expr.GetCollectionTupleDesc();
    DCHECK(item_tuple_desc != nullptr);

    ComplexValueWriter<rapidjson::BasicOStreamWrapper<stringstream>>
        complex_value_writer(&writer, stringify_map_keys);
    complex_value_writer.CollectionValueToJSON(*collection_val, type.type,
            item_tuple_desc);
  } else {
    DCHECK(type.IsStructType());
    const StructVal* struct_val = static_cast<const StructVal*>(value);
    const SlotDescriptor* slot_desc =
        static_cast<const SlotRef&>(scalar_expr).GetSlotDescriptor();
    DCHECK(slot_desc != nullptr);
    DCHECK_EQ(type, slot_desc->type());

    ComplexValueWriter<rapidjson::BasicOStreamWrapper<stringstream>>
        complex_value_writer(&writer, stringify_map_keys);
    complex_value_writer.StructValToJSON(*struct_val, *slot_desc);
  }
}

Status AsciiQueryResultSet::AddOneRow(const TResultRow& row) {
  int num_col = row.colVals.size();
  DCHECK_EQ(num_col, metadata_.columns.size());
  stringstream out_stream;
  out_stream.precision(ASCII_PRECISION);
  for (int i = 0; i < num_col; ++i) {
    // ODBC-187 - ODBC can only take "\t" as the delimiter
    if (i > 0) out_stream << '\t';
    PrintTColumnValue(out_stream, row.colVals[i]);
  }
  result_set_->push_back(out_stream.str());
  return Status::OK();
}

int AsciiQueryResultSet::AddRows(
    const QueryResultSet* other, int start_idx, int num_rows) {
  const AsciiQueryResultSet* o = static_cast<const AsciiQueryResultSet*>(other);
  if (start_idx >= o->result_set_->size()) return 0;
  const int rows_added =
      min(static_cast<size_t>(num_rows), o->result_set_->size() - start_idx);
  result_set_->insert(result_set_->end(), o->result_set_->begin() + start_idx,
      o->result_set_->begin() + start_idx + rows_added);
  return rows_added;
}

int64_t AsciiQueryResultSet::ByteSize(int start_idx, int num_rows) {
  int64_t bytes = 0;
  const int end = min(static_cast<size_t>(num_rows), result_set_->size() - start_idx);
  for (int i = start_idx; i < start_idx + end; ++i) {
    bytes += sizeof(result_set_[i]) + result_set_[i].size();
  }
  return bytes;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// Utility functions for computing the size of HS2 Thrift structs in bytes.
inline int64_t ByteSize(const ThriftTColumnValue& val) {
  return sizeof(val) + val.stringVal.value.size();
}

int64_t ByteSize(const TRow& row) {
  int64_t bytes = sizeof(row);
  for (const ThriftTColumnValue& c : row.colVals) {
    bytes += ByteSize(c);
  }
  return bytes;
}

// Returns the size, in bytes, of a Hive TColumn structure, only taking into account those
// values in the range [start_idx, end_idx).
uint32_t TColumnByteSize(const ThriftTColumn& col, uint32_t start_idx, uint32_t end_idx) {
  DCHECK_LE(start_idx, end_idx);
  uint32_t num_rows = end_idx - start_idx;
  if (num_rows == 0) return 0L;

  if (col.__isset.boolVal) return (num_rows * sizeof(bool)) + col.boolVal.nulls.size();
  if (col.__isset.byteVal) return num_rows + col.byteVal.nulls.size();
  if (col.__isset.i16Val) return (num_rows * sizeof(int16_t)) + col.i16Val.nulls.size();
  if (col.__isset.i32Val) return (num_rows * sizeof(int32_t)) + col.i32Val.nulls.size();
  if (col.__isset.i64Val) return (num_rows * sizeof(int64_t)) + col.i64Val.nulls.size();
  if (col.__isset.doubleVal) {
    return (num_rows * sizeof(double)) + col.doubleVal.nulls.size();
  }
  if (col.__isset.stringVal) {
    uint32_t bytes = 0;
    for (int i = start_idx; i < end_idx; ++i) bytes += col.stringVal.values[i].size();
    return bytes + col.stringVal.nulls.size();
  }

  return 0;
}
}

// Result set container for Hive protocol versions >= V6, where results are returned in
// column-orientation.
HS2ColumnarResultSet::HS2ColumnarResultSet(
    const TResultSetMetadata& metadata, TRowSet* rowset, bool stringify_map_keys,
    int expected_result_count)
  : metadata_(metadata), result_set_(rowset), num_rows_(0),
    stringify_map_keys_(stringify_map_keys),
    expected_result_count_(expected_result_count) {
  if (rowset == NULL) {
    owned_result_set_.reset(new TRowSet());
    result_set_ = owned_result_set_.get();
  }
  InitColumns();
}

Status HS2ColumnarResultSet::AddRows(const vector<ScalarExprEvaluator*>& expr_evals,
    RowBatch* batch, int start_idx, int num_rows) {
  DCHECK_GE(batch->num_rows(), start_idx + num_rows);
  int expected_result_count =
      std::max((int64_t) expected_result_count_, num_rows + num_rows_);
  int num_col = expr_evals.size();
  DCHECK_EQ(num_col, metadata_.columns.size());
  for (int i = 0; i < num_col; ++i) {
    const TColumnType& type = metadata_.columns[i].columnType;
    ScalarExprEvaluator* expr_eval = expr_evals[i];
    ExprValuesToHS2TColumn(expr_eval, type, batch, start_idx, num_rows, num_rows_,
        expected_result_count, stringify_map_keys_, &(result_set_->columns[i]));
  }
  num_rows_ += num_rows;
  return Status::OK();
}

// Add a row from a TResultRow
Status HS2ColumnarResultSet::AddOneRow(const TResultRow& row) {
  int num_col = row.colVals.size();
  DCHECK_EQ(num_col, metadata_.columns.size());
  for (int i = 0; i < num_col; ++i) {
    TColumnValueToHS2TColumn(row.colVals[i], metadata_.columns[i].columnType, num_rows_,
        &(result_set_->columns[i]));
  }
  ++num_rows_;
  return Status::OK();
}

// Copy all columns starting at 'start_idx' and proceeding for a maximum of 'num_rows'
// from 'other' into this result set
int HS2ColumnarResultSet::AddRows(
    const QueryResultSet* other, int start_idx, int num_rows) {
  const HS2ColumnarResultSet* o = static_cast<const HS2ColumnarResultSet*>(other);
  DCHECK_EQ(metadata_.columns.size(), o->metadata_.columns.size());
  if (start_idx >= o->num_rows_) return 0;
  const int rows_added = min<int64_t>(num_rows, o->num_rows_ - start_idx);
  for (int j = 0; j < metadata_.columns.size(); ++j) {
    ThriftTColumn* from = &o->result_set_->columns[j];
    ThriftTColumn* to = &result_set_->columns[j];
    const TColumnType& colType = metadata_.columns[j].columnType;
    TPrimitiveType::type primitiveType = colType.types[0].scalar_type.type;
    if (colType.types[0].type != TTypeNodeType::SCALAR) {
      DCHECK(from->__isset.stringVal);
      primitiveType = TPrimitiveType::STRING;
    }
    switch (primitiveType) {
      case TPrimitiveType::NULL_TYPE:
      case TPrimitiveType::BOOLEAN:
        StitchNulls(
            num_rows_, rows_added, start_idx, from->boolVal.nulls, &(to->boolVal.nulls));
        to->boolVal.values.insert(to->boolVal.values.end(),
            from->boolVal.values.begin() + start_idx,
            from->boolVal.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::TINYINT:
        StitchNulls(
            num_rows_, rows_added, start_idx, from->byteVal.nulls, &(to->byteVal.nulls));
        to->byteVal.values.insert(to->byteVal.values.end(),
            from->byteVal.values.begin() + start_idx,
            from->byteVal.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::SMALLINT:
        StitchNulls(
            num_rows_, rows_added, start_idx, from->i16Val.nulls, &(to->i16Val.nulls));
        to->i16Val.values.insert(to->i16Val.values.end(),
            from->i16Val.values.begin() + start_idx,
            from->i16Val.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::INT:
        StitchNulls(
            num_rows_, rows_added, start_idx, from->i32Val.nulls, &(to->i32Val.nulls));
        to->i32Val.values.insert(to->i32Val.values.end(),
            from->i32Val.values.begin() + start_idx,
            from->i32Val.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::BIGINT:
        StitchNulls(
            num_rows_, rows_added, start_idx, from->i64Val.nulls, &(to->i64Val.nulls));
        to->i64Val.values.insert(to->i64Val.values.end(),
            from->i64Val.values.begin() + start_idx,
            from->i64Val.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::FLOAT:
      case TPrimitiveType::DOUBLE:
        StitchNulls(num_rows_, rows_added, start_idx, from->doubleVal.nulls,
            &(to->doubleVal.nulls));
        to->doubleVal.values.insert(to->doubleVal.values.end(),
            from->doubleVal.values.begin() + start_idx,
            from->doubleVal.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::TIMESTAMP:
      case TPrimitiveType::DATE:
      case TPrimitiveType::DECIMAL:
      case TPrimitiveType::STRING:
      case TPrimitiveType::VARCHAR:
      case TPrimitiveType::CHAR:
        StitchNulls(num_rows_, rows_added, start_idx, from->stringVal.nulls,
            &(to->stringVal.nulls));
        to->stringVal.values.insert(to->stringVal.values.end(),
            from->stringVal.values.begin() + start_idx,
            from->stringVal.values.begin() + start_idx + rows_added);
        break;
      case TPrimitiveType::BINARY:
        StitchNulls(num_rows_, rows_added, start_idx, from->binaryVal.nulls,
            &(to->binaryVal.nulls));
        to->binaryVal.values.insert(to->binaryVal.values.end(),
            from->binaryVal.values.begin() + start_idx,
            from->binaryVal.values.begin() + start_idx + rows_added);
        break;
      default:
        DCHECK(false) << "Unsupported type: "
                      << TypeToString(ThriftToType(
                             metadata_.columns[j].columnType.types[0].scalar_type.type));
        break;
    }
  }
  num_rows_ += rows_added;
  return rows_added;
}

int64_t HS2ColumnarResultSet::ByteSize(int start_idx, int num_rows) {
  const int end = min(start_idx + num_rows, (int)size());
  int64_t bytes = 0L;
  for (const ThriftTColumn& c : result_set_->columns) {
    bytes += TColumnByteSize(c, start_idx, end);
  }
  return bytes;
}

void HS2ColumnarResultSet::InitColumns() {
  result_set_->__isset.columns = true;
  for (const TColumn& col_input : metadata_.columns) {
    const vector<TTypeNode>& type_nodes = col_input.columnType.types;
    DCHECK(type_nodes.size() > 0);
    ThriftTColumn col_output;
    if (type_nodes[0].type == TTypeNodeType::STRUCT
        || type_nodes[0].type == TTypeNodeType::ARRAY
        || type_nodes[0].type == TTypeNodeType::MAP) {
      // Return structs, arrays and maps as string.
      col_output.__isset.stringVal = true;
    } else {
      DCHECK(type_nodes.size() == 1);
      DCHECK(type_nodes[0].__isset.scalar_type);
      TPrimitiveType::type input_type = type_nodes[0].scalar_type.type;
      switch (input_type) {
        case TPrimitiveType::NULL_TYPE:
        case TPrimitiveType::BOOLEAN:
          col_output.__isset.boolVal = true;
          break;
        case TPrimitiveType::TINYINT:
          col_output.__isset.byteVal = true;
          break;
        case TPrimitiveType::SMALLINT:
          col_output.__isset.i16Val = true;
          break;
        case TPrimitiveType::INT:
          col_output.__isset.i32Val = true;
          break;
        case TPrimitiveType::BIGINT:
          col_output.__isset.i64Val = true;
          break;
        case TPrimitiveType::FLOAT:
        case TPrimitiveType::DOUBLE:
          col_output.__isset.doubleVal = true;
          break;
        case TPrimitiveType::TIMESTAMP:
        case TPrimitiveType::DATE:
        case TPrimitiveType::DECIMAL:
        case TPrimitiveType::VARCHAR:
        case TPrimitiveType::CHAR:
        case TPrimitiveType::STRING:
          col_output.__isset.stringVal = true;
          break;
        case TPrimitiveType::BINARY:
          col_output.__isset.binaryVal = true;
          break;
        default:
          DCHECK(false) << "Unhandled column type: "
              << TypeToString(ThriftToType(input_type));
      }
    }
    result_set_->columns.push_back(col_output);
  }
}

HS2RowOrientedResultSet::HS2RowOrientedResultSet(
    const TResultSetMetadata& metadata, TRowSet* rowset)
  : metadata_(metadata), result_set_(rowset) {
  if (rowset == NULL) {
    owned_result_set_.reset(new TRowSet());
    result_set_ = owned_result_set_.get();
  }
}

Status HS2RowOrientedResultSet::AddRows(const vector<ScalarExprEvaluator*>& expr_evals,
    RowBatch* batch, int start_idx, int num_rows) {
  DCHECK_GE(batch->num_rows(), start_idx + num_rows);
  int num_col = expr_evals.size();
  DCHECK_EQ(num_col, metadata_.columns.size());
  result_set_->rows.reserve(
      BitUtil::RoundUpToPowerOfTwo(result_set_->rows.size() + num_rows - start_idx));
  FOREACH_ROW_LIMIT(batch, start_idx, num_rows, it) {
    result_set_->rows.push_back(TRow());
    TRow& trow = result_set_->rows.back();
    trow.colVals.resize(num_col);
    for (int i = 0; i < num_col; ++i) {
      ExprValueToHS2TColumnValue(expr_evals[i]->GetValue(it.Get()),
          metadata_.columns[i].columnType, &(trow.colVals[i]));
    }
  }
  return Status::OK();
}

Status HS2RowOrientedResultSet::AddOneRow(const TResultRow& row) {
  int num_col = row.colVals.size();
  DCHECK_EQ(num_col, metadata_.columns.size());
  result_set_->rows.push_back(TRow());
  TRow& trow = result_set_->rows.back();
  trow.colVals.resize(num_col);
  for (int i = 0; i < num_col; ++i) {
    TColumnValueToHS2TColumnValue(
        row.colVals[i], metadata_.columns[i].columnType, &(trow.colVals[i]));
  }
  return Status::OK();
}

int HS2RowOrientedResultSet::AddRows(
    const QueryResultSet* other, int start_idx, int num_rows) {
  const HS2RowOrientedResultSet* o = static_cast<const HS2RowOrientedResultSet*>(other);
  if (start_idx >= o->result_set_->rows.size()) return 0;
  const int rows_added =
      min(static_cast<size_t>(num_rows), o->result_set_->rows.size() - start_idx);
  for (int i = start_idx; i < start_idx + rows_added; ++i) {
    result_set_->rows.push_back(o->result_set_->rows[i]);
  }
  return rows_added;
}

int64_t HS2RowOrientedResultSet::ByteSize(int start_idx, int num_rows) {
  int64_t bytes = 0;
  const int end =
      min(static_cast<size_t>(num_rows), result_set_->rows.size() - start_idx);
  for (int i = start_idx; i < start_idx + end; ++i) {
    bytes += impala::ByteSize(result_set_->rows[i]);
  }
  return bytes;
}
}
