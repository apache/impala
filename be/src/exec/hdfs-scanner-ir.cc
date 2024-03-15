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

#include "exec/hdfs-scanner.h"
#include "exec/text-converter.h"
#include "exec/text-converter.inline.h"
#include "runtime/row-batch.h"
#include "util/string-parser.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"

#include "common/names.h"

using namespace impala;

// Functions in this file are cross compiled to IR with clang.  These functions
// are modified at runtime with a query specific codegen'd WriteTuple

// This function will output tuples to the row batch from parsed field locations.
// The fields locations should be aligned to the start of the tuple (field at 0 is
// the first materialized slot).
// This function takes more arguments than are strictly necessary (they could be
// computed inside this function) but this is done to minimize the clang dependencies,
// specifically, calling function on the scan node.
int HdfsScanner::WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row,
    FieldLocation* fields, int num_tuples, int max_added_tuples,
    int slots_per_tuple, int row_idx_start, bool copy_strings) {
  DCHECK(tuple_ != NULL);
  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
  uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);

  uint8_t error[slots_per_tuple];
  memset(error, 0, slots_per_tuple);

  int tuples_returned = 0;

  // Loop through the fields and materialize all the tuples
  for (int i = 0; i < num_tuples; ++i) {
    uint8_t error_in_row = false;
    // Materialize a single tuple.  This function will be replaced by a codegen'd
    // function.
    if (WriteCompleteTuple(pool, fields, tuple, tuple_row, template_tuple_,
          error, &error_in_row)) {
      if (copy_strings) {
        if (UNLIKELY(!tuple->CopyStrings("HdfsScanner::WriteAlignedTuples()",
              state_, string_slot_offsets_.data(), string_slot_offsets_.size(), pool,
              &parse_status_))) {
          return -1;
        }
      }
      ++tuples_returned;
      tuple_mem += tuple_byte_size();
      tuple_row_mem += sizeof(Tuple*);
      tuple = reinterpret_cast<Tuple*>(tuple_mem);
      tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    }

    // Report parse errors
    if (UNLIKELY(error_in_row)) {
      if (!ReportTupleParseError(fields, error)) return -1;
    }

    // Advance to the start of the next tuple
    fields += slots_per_tuple;

    if (tuples_returned == max_added_tuples) {
      break;
    }
  }

  return tuples_returned;
}

ScalarExprEvaluator* HdfsScanner::GetConjunctEval(int idx) const {
  return (*conjunct_evals_)[idx];
}

void StringToDecimalSymbolDummy() {
  // Force linker to to link the object file containing these functions.
  StringToDecimal4(nullptr, 0, 0, 0, false, nullptr);
  StringToDecimal8(nullptr, 0, 0, 0, false, nullptr);
  StringToDecimal16(nullptr, 0, 0, 0, false, nullptr);
}

bool HdfsScanner::EvalRuntimeFilter(int i, TupleRow* row) {
  LocalFilterStats* stats = &filter_stats_[i];
  const FilterContext* ctx = filter_ctxs_[i];
  ++stats->total_possible;
  if (stats->enabled_for_row && ctx->filter->HasFilter()) {
    // Evaluating IN-list filter is much slower than evaluating the corresponding bloom
    // filter. Skip it until we improve its performance.
    if (ctx->filter->is_in_list_filter()) return true;
    ++stats->considered;
    if (!ctx->Eval(row)) {
      ++stats->rejected;
      return false;
    }
  }
  return true;
}

bool HdfsScanner::TextConverterWriteSlotInterpretedIR(HdfsScanner* hdfs_scanner,
    int slot_idx, Tuple* tuple, const char* data, int len, MemPool* pool) {
  constexpr bool copy_string = false;

  int need_escape = false;
  if (UNLIKELY(len < 0)) {
    len = -len;
    need_escape = true;
  }

  SlotDescriptor* slot_desc = hdfs_scanner->scan_node_->materialized_slots()[slot_idx];
  return hdfs_scanner->text_converter_->WriteSlot(slot_desc, tuple, data, len,
       copy_string, need_escape, pool);
}

// Define the string parsing functions for llvm.  Stamp out the templated functions
#ifdef IR_COMPILE
using ParseResult = StringParser::ParseResult;
extern "C"
bool IrStringToBool(const char* s, int len, ParseResult* result) {
  return StringParser::StringToBool(s, len, result);
}

extern "C"
int8_t IrStringToInt8(const char* s, int len, ParseResult* result) {
  return StringParser::StringToInt<int8_t>(s, len, result);
}

extern "C"
int16_t IrStringToInt16(const char* s, int len, ParseResult* result) {
  return StringParser::StringToInt<int16_t>(s, len, result);
}

extern "C"
int32_t IrStringToInt32(const char* s, int len, ParseResult* result) {
  return StringParser::StringToInt<int32_t>(s, len, result);
}

extern "C"
int64_t IrStringToInt64(const char* s, int len, ParseResult* result) {
  return StringParser::StringToInt<int64_t>(s, len, result);
}

extern "C"
float IrStringToFloat(const char* s, int len, ParseResult* result) {
  return StringParser::StringToFloat<float>(s, len, result);
}

extern "C"
double IrStringToDouble(const char* s, int len, ParseResult* result) {
  return StringParser::StringToFloat<double>(s, len, result);
}

extern "C"
void IrStringToTimestamp(TimestampValue* out, const char* s, int len,
    ParseResult* result) {
  *out = StringParser::StringToTimestamp(s, len, result);
}

extern "C"
DateValue IrStringToDate(const char* s, int len, ParseResult* result) {
  return StringParser::StringToDate(s, len, result);
}

extern "C"
Decimal4Value IrStringToDecimal4(const char* s, int len, int type_precision,
    int type_scale, ParseResult* result)  {
  auto ret = StringToDecimal4(s, len, type_precision, type_scale, false, result);
  if (*result != ParseResult::PARSE_SUCCESS) *result = ParseResult::PARSE_FAILURE;
  return ret;
}

extern "C"
Decimal8Value IrStringToDecimal8(const char* s, int len, int type_precision,
    int type_scale, ParseResult* result)  {
  auto ret = StringToDecimal8(s, len, type_precision, type_scale, false, result);
  if (*result != ParseResult::PARSE_SUCCESS) *result = ParseResult::PARSE_FAILURE;
  return ret;
}

extern "C"
Decimal16Value IrStringToDecimal16(const char* s, int len, int type_precision,
    int type_scale, ParseResult* result)  {
  auto ret = StringToDecimal16(s, len, type_precision, type_scale, false, result);
  if (*result != ParseResult::PARSE_SUCCESS) *result = ParseResult::PARSE_FAILURE;
  return ret;
}

extern "C"
bool IrIsNullString(const char* data, int len) {
  return data == NULL || (len == 2 && data[0] == '\\' && data[1] == 'N');
}

extern "C"
bool IrGenericIsNullString(const char* s, int slen, const char* n, int nlen) {
  return s == NULL || (slen == nlen && StringCompare(s, slen, n, nlen, slen) == 0);
}
#endif
