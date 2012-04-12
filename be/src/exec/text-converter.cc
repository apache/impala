// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "runtime/runtime-state.h"
#include "text-converter.h"
#include <boost/algorithm/string.hpp>
#include <glog/logging.h>
#include "runtime/descriptors.h"
#include "runtime/tuple.h"
#include "util/string-parser.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "runtime/mem-pool.h"

using namespace boost;
using namespace impala;
using namespace std;

TextConverter::TextConverter(char escape_char, MemPool* var_len_pool)
  : escape_char_(escape_char),
    var_len_pool_(var_len_pool) {
}

void TextConverter::UnescapeString(StringValue* value) {
  char* new_data = reinterpret_cast<char*>(var_len_pool_->Allocate(value->len));
  UnescapeString(value->ptr, new_data, &value->len);
  value->ptr = new_data;
}

void TextConverter::UnescapeString(const char* src, char* dest, int* len) {
  char* dest_ptr = dest;
  const char* end = src + *len;
  bool escape_next_char = false;
  while (src < end) {
    if (*src == escape_char_) {
      escape_next_char = !escape_next_char;
    } else {
      escape_next_char = false;
    }
    if (escape_next_char) {
      ++src;
    } else {
      *dest_ptr++ = *src++;
    }
  }
  char* dest_start = reinterpret_cast<char*>(dest);
  *len = dest_ptr - dest_start;
}
