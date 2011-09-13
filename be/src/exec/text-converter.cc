// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "text-converter.h"
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "runtime/mem-pool.h"

using namespace boost;
using namespace impala;

TextConverter::TextConverter(bool strings_are_quoted, char escape_char, MemPool* var_len_pool)
  : strings_are_quoted_(strings_are_quoted),
    escape_char_(escape_char),
    var_len_pool_(var_len_pool) {
}

bool TextConverter::ConvertAndWriteSlotBytes(const char* begin, const char* end, Tuple* tuple,
    const SlotDescriptor* slot_desc, bool copy_string, bool unescape_string) {
  // Check for null columns.
  // The below code implies that unquoted empty strings
  // such as "...,,..." become NULLs, and not empty strings.
  if (begin == end) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return true;
  }
  // Will be changed in conversion functions for error checking.
  char* end_ptr = const_cast<char*>(end);
  // TODO: Handle out-of-range conditions.
  switch (slot_desc->type()) {
    case TYPE_BOOLEAN: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      if (iequals(begin, "true")) {
        *reinterpret_cast<char*>(slot) = true;
      } else if (iequals(begin, "false")) {
        *reinterpret_cast<char*>(slot) = false;
      } else {
        // Inconvertible value. Set to NULL after switch statement.
        end_ptr = const_cast<char*>(begin);
      }
      break;
    }
    case TYPE_TINYINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<char*>(slot) =
          static_cast<char>(strtol(begin, &end_ptr, 0));
      break;
    }
    case TYPE_SMALLINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<short*>(slot) =
          static_cast<short>(strtol(begin, &end_ptr, 0));
      break;
    }
    case TYPE_INT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<int*>(slot) =
          static_cast<int>(strtol(begin, &end_ptr, 0));
      break;
    }
    case TYPE_BIGINT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<long*>(slot) = strtol(begin, &end_ptr, 0);
      break;
    }
    case TYPE_FLOAT: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<float*>(slot) =
          static_cast<float>(strtod(begin, &end_ptr));
      break;
    }
    case TYPE_DOUBLE: {
      void* slot = tuple->GetSlot(slot_desc->tuple_offset());
      *reinterpret_cast<double*>(slot) = strtod(begin, &end_ptr);
      break;
    }
    case TYPE_STRING: {
      StringValue* slot = tuple->GetStringSlot(slot_desc->tuple_offset());
      const char* data_start = NULL;
      if (strings_are_quoted_) {
        // take out 2 characters for the quotes
        slot->len = end - begin - 2;
        // skip the quote char at the beginning
        data_start = begin + 1;
      } else {
        slot->len = end - begin;
        data_start = begin;
      }
      if (!copy_string) {
        DCHECK(!unescape_string);
        slot->ptr = const_cast<char*>(data_start);
      } else {
        char* slot_data = reinterpret_cast<char*>(var_len_pool_->Allocate(slot->len));
        if (unescape_string) {
          UnescapeString(data_start, slot_data, &slot->len);
        } else {
          memcpy(slot_data, data_start, slot->len);
        }
        slot->ptr = slot_data;
      }
      break;
    }
    default:
      DCHECK(false) << "bad slot type: " << TypeToString(slot_desc->type());
  }
  // Set NULL if inconvertible.
  if (*end_ptr != '\0' && slot_desc->type() != TYPE_STRING) {
    tuple->SetNull(slot_desc->null_indicator_offset());
    return false;
  }

  return true;
}

void TextConverter::UnescapeString(const char* src, char* dest, int* len) {
  char* dest_ptr = dest;
  const char* end = src + *len;
  while (src < end) {
    if (*src == escape_char_) {
      ++src;
    } else {
      *dest_ptr++ = *src++;
    }
  }
  char* dest_start = reinterpret_cast<char*>(dest);
  *len = dest_ptr - dest_start;
}
