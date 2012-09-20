// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "text-converter.h"

#include <boost/algorithm/string.hpp>

#include "runtime/runtime-state.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "util/string-parser.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/mem-pool.h"

namespace impala {

inline bool TextConverter::WriteSlot(const SlotDescriptor* slot_desc, Tuple* tuple,
    const char* data, int len, bool copy_string, bool need_escape, MemPool* pool) {
  if (len == 0 && slot_desc->type() != TYPE_STRING) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return true;
  } else if (len == 2 && data[0] == '\\' && data[1] == 'N') {
    // Hive encodes NULLs as '\N'
    tuple->SetNull(slot_desc->null_indicator_offset());
    return true;
  } 
    
  StringParser::ParseResult parse_result = StringParser::PARSE_SUCCESS;
  void* slot = tuple->GetSlot(slot_desc->tuple_offset());

  // Parse the raw-text data. Translate the text string to internal format.
  switch (slot_desc->type()) {
    case TYPE_STRING: {
      StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
      str_slot->ptr = const_cast<char*>(data);
      str_slot->len = len;
      if (len != 0 && (copy_string || need_escape)) {
        DCHECK(pool != NULL);
        char* slot_data = reinterpret_cast<char*>(pool->Allocate(len));
        if (need_escape) {
          UnescapeString(data, slot_data, &str_slot->len);
        } else {
          memcpy(slot_data, data, str_slot->len);
        }
        str_slot->ptr = slot_data;
      }
      break;
    }
    case TYPE_BOOLEAN:
      *reinterpret_cast<bool*>(slot) =
        StringParser::StringToBool(data, len, &parse_result);
      break;
    case TYPE_TINYINT:
      *reinterpret_cast<int8_t*>(slot) =
        StringParser::StringToInt<int8_t>(data, len, &parse_result);
      break;
    case TYPE_SMALLINT:
      *reinterpret_cast<int16_t*>(slot) =
        StringParser::StringToInt<int16_t>(data, len, &parse_result);
      break;
    case TYPE_INT:
      *reinterpret_cast<int32_t*>(slot) =
        StringParser::StringToInt<int32_t>(data, len, &parse_result);
      break;
    case TYPE_BIGINT:
      *reinterpret_cast<int64_t*>(slot) =
        StringParser::StringToInt<int64_t>(data, len, &parse_result);
      break;
    case TYPE_FLOAT:
      *reinterpret_cast<float*>(slot) =
        StringParser::StringToFloat<float>(data, len, &parse_result);
      break;
    case TYPE_DOUBLE:
      *reinterpret_cast<double*>(slot) =
        StringParser::StringToFloat<double>(data, len, &parse_result);
      break;
    case TYPE_TIMESTAMP: {
      TimestampValue* ts_slot = reinterpret_cast<TimestampValue*>(slot);
      *ts_slot = TimestampValue(data, len);
      if (ts_slot->NotADateTime()) {
        parse_result = StringParser::PARSE_FAILURE;
      }
      break;
    }
    default:
      DCHECK(false) << "bad slot type: " << TypeToString(slot_desc->type());
      break;
  }

  // TODO: add warning for overflow case
  if (parse_result == StringParser::PARSE_FAILURE) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return false;
  }

  return true;
}

}

