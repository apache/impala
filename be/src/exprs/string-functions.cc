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

// The functions in this file are specifically not cross-compiled to IR because there
// is no signifcant performance benefit to be gained.

#include "exprs/string-functions.h"

#include <gutil/strings/util.h>
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/error/en.h>
#include <rapidjson/writer.h>

#include "exprs/anyval-util.h"
#include "util/string-util.h"
#include "util/string-parser.h"

#include "common/names.h"
#include "cast-functions.h"

using namespace rapidjson;
using namespace impala_udf;

namespace impala {

#define RETURN_IF_OOM(stmt, result_on_err) \
  try {stmt;} catch (const std::bad_alloc& e) { \
  VLOG_QUERY << "Allocation failed: " << e.what(); return result_on_err; }

#define RETURN_NULL_IF_OOM(stmt) RETURN_IF_OOM(stmt, StringVal::null());

/// RapidJSON combines different types using templates. A class containing all required
/// interface can be an allocator.
/// This class is a wrapper of FunctionContext for RapidJSON to allocate tracked memory.
class JsonUdfAllocator {
 public:
  static const bool kNeedFree = false;

  JsonUdfAllocator() { DCHECK(false) << "Default constructor should not be used"; }

  JsonUdfAllocator(FunctionContext* ctx): ctx_(ctx) {}

  void* Malloc(size_t size) {
    if (!size) return nullptr;
    StringVal buffer(ctx_, size);
    if (UNLIKELY(buffer.is_null)) {
      // RapidJSON cannot handle allocation failures. We can only throw an exception
      // to stop it.
      throw std::bad_alloc();
    }
    return buffer.ptr;
  }

  void* Realloc(void* originalPtr, size_t originalSize, size_t newSize) {
    if (originalPtr == nullptr && newSize > 0) return Malloc(newSize);
    if (newSize == 0) return nullptr;
    // Do not shrink if new size is smaller than original
    if (originalSize >= newSize) return originalPtr;
    void* newBuffer = Malloc(newSize);
    if (originalSize > 0) memcpy(newBuffer, originalPtr, originalSize);
    // Don't need to free the original space. The allocated memory will be recycled in
    // bulk after the UDF returns
    return newBuffer;
  }

  // The allocated memory will be recycled in bulk after the UDF returns
  static void Free(void* ptr) {}

 private:
  FunctionContext* ctx_;
};

/// RapidJSON requires input strings that end with a trailing '\0'. Since StringVal
/// doesn't have a trailing '\0', we need a wrapper Stream for it.
/// A class containing all required interface can be a usable stream for RapidJSON.
class StringValStream {
 public:
  typedef UTF8<>::Ch Ch;

  StringValStream(const StringVal *str)
    : pos_(reinterpret_cast<Ch*>(str->ptr)),
      head_(reinterpret_cast<Ch*>(str->ptr)),
      tail_(reinterpret_cast<Ch*>(str->ptr + str->len)) {}

  char Peek() const {
    if (pos_ == tail_) return 0;
    return *pos_;
  }

  char Take() {
    if (pos_ == tail_) return 0;
    return *pos_++;
  }

  size_t Tell() const { return pos_ - head_; }

  // Interfaces that should not be called
  char* PutBegin() { DCHECK(false); return 0; }
  void Put(Ch) { DCHECK(false); }
  size_t PutEnd(Ch*) { DCHECK(false); return 0; }

  const Ch* pos_;    // Current read position.
  const Ch* head_;   // Original head of the string.
  const Ch* tail_;   // Original tail of the string.
};

typedef GenericDocument<UTF8<>, JsonUdfAllocator> JsonUdfDocument;
typedef GenericValue<UTF8<>, JsonUdfAllocator> JsonUdfValue;
typedef GenericStringBuffer<UTF8<>, JsonUdfAllocator> JsonUdfStrBuffer;
typedef Writer<JsonUdfStrBuffer, UTF8<>, UTF8<>, JsonUdfAllocator> JsonUdfWriter;

static StringVal ToStringVal(FunctionContext* ctx, const JsonUdfValue& values,
    JsonUdfAllocator* allocator) {
  DCHECK(values.IsArray());
  if (values.Empty()) return StringVal::null();
  JsonUdfStrBuffer sb(allocator);
  JsonUdfWriter writer(sb, allocator);
  JsonUdfValue res;
  if (values.Size() == 1) {
    const JsonUdfValue& v = values[0];
    if (v.IsNull()) return StringVal::null();
    if (v.IsString()) {
      // RapidJSON will quote the strings. It's incompatible with Hive's behavior when
      // the string is at the root, so we convert it ourselves here.
      return StringVal::CopyFrom(ctx, reinterpret_cast<const uint8_t*>(v.GetString()),
          v.GetStringLength());
    }
    RETURN_NULL_IF_OOM(v.Accept(writer));
  } else {  // multiple selected items, return an array string
    RETURN_NULL_IF_OOM(values.Accept(writer));
  }
  const char* res_ptr = sb.GetString();
  return StringVal::CopyFrom(ctx, reinterpret_cast<const uint8_t*>(res_ptr),
      sb.GetSize());
}

// Extract all the values for 'key' where objects in 'queue' contain that key.
// Replace the contents of queue with the values found.
static void SelectByKey(const string& key, JsonUdfValue* queue,
    JsonUdfAllocator* allocator) {
  SizeType old_items = queue->Size();  // RapidJson uses SizeType instead of size_t
  const char* key_ptr = key.c_str();
  JsonUdfValue item;
  for (SizeType i = 0; i < old_items; ++i) {
    item = (*queue)[i];
    if (!item.IsObject() || !item.HasMember(key_ptr)) continue;
    queue->PushBack(item[key_ptr], *allocator);
  }
  queue->Erase(queue->Begin(), queue->Begin() + old_items);
}

// Extract all the values for 'index' where arrays in 'queue' contain that index.
// Replace the contents of queue with the values found.
static void SelectByIndex(const int index, JsonUdfValue* queue,
    JsonUdfAllocator* allocator) {
  DCHECK(queue->IsArray());
  SizeType old_items = queue->Size();
  for (SizeType i = 0; i < old_items; ++i) {
    JsonUdfValue& item = (*queue)[i];
    if (!item.IsArray() || index >= item.Capacity()) continue;
    queue->PushBack(item[index], *allocator);
  }
  queue->Erase(queue->Begin(), queue->Begin() + old_items);
}

// Expand all arrays in the queue and replace the contents of queue with them.
static void ExpandArrays(JsonUdfValue* queue, JsonUdfAllocator* allocator) {
  DCHECK(queue->IsArray());
  SizeType old_items = queue->Size();
  for (SizeType i = 0; i < old_items; ++i) {
    if (!(*queue)[i].IsArray()) continue;
    for (auto& v : (*queue)[i].GetArray()) queue->PushBack(v, *allocator);
  }
  queue->Erase(queue->Begin(), queue->Begin() + old_items);
}

// Extract all values of the objects in queue and replace the contents of queue with them
static void ExtractValues(JsonUdfValue* queue, JsonUdfAllocator* allocator) {
  SizeType old_items = queue->Size();
  for (SizeType i = 0; i < old_items; ++i) {
    if (!(*queue)[i].IsObject()) continue;
    for (auto& m : (*queue)[i].GetObject()) queue->PushBack(m.value, *allocator);
  }
  queue->Erase(queue->Begin(), queue->Begin() + old_items);
}

/// Process wildcard(*) in value selection. path_idx is the index after the wildcard in
/// path_str. Return next unprocessed index in path_str. Return -1 for errors.
static int ProcessWildcardKey(FunctionContext* ctx, const StringVal& path_str,
    int path_idx, JsonUdfValue* queue, JsonUdfAllocator* allocator) {
  DCHECK(queue->IsArray());
  const uint8_t* path = path_str.ptr;
  while (path_idx < path_str.len) {
    if (path[path_idx] == '[' || path[path_idx] == '.') break;
    if (path[path_idx] != ' ') {
      string msg = Substitute("Failed to parse json path '$0': "
          "Encountered '$1' in position $2, expects ' ', '[' or '.'",
          AnyValUtil::ToString(path_str), static_cast<char>(path[path_idx]), path_idx);
      ctx->SetError(msg.c_str());
      return -1;
    }
    ++path_idx;
  }
  RETURN_IF_OOM(ExtractValues(queue, allocator), -1);
  return path_idx;
}

/// Process wildcard(*) in array selection. path_idx is the index after the wildcard in
/// path_str. Return next unprocessed index in path_str. Return -1 for errors.
static int ProcessWildcardIndex(FunctionContext* ctx, const StringVal& path_str,
    int path_idx, JsonUdfValue* queue, JsonUdfAllocator* allocator) {
  const uint8_t* path = path_str.ptr;
  while (path_idx < path_str.len && path[path_idx] != ']') {
    if (path[path_idx] != ' ') { // have something else illegal
      string msg = Substitute("Failed to parse json path '$0': "
          "Encountered '$1' in position $2, expects ' ' or ']'",
          AnyValUtil::ToString(path_str), static_cast<char>(path[path_idx]), path_idx);
      ctx->SetError(msg.c_str());
      return -1;
    }
    ++path_idx;
  }
  if (path_idx == path_str.len) {
    string msg = Substitute("Unclosed brackets in json path '$0'",
        AnyValUtil::ToString(path_str));
    ctx->SetError(msg.c_str());
    return -1;
  }
  RETURN_IF_OOM(ExpandArrays(queue, allocator), -1);
  return path_idx + 1;  // path_idx points at ']'
}

/// Process number in array selection. path_idx points at the start of the number in
/// path_str. Return next unprocessed index in path_str. Return -1 for errors.
static int ProcessNumberIndex(FunctionContext* ctx, const StringVal& path_str,
    int path_idx, JsonUdfValue* queue, JsonUdfAllocator* allocator) {
  const uint8_t* path = path_str.ptr;
  const char* number_start = reinterpret_cast<const char*>(path + path_idx);
  int i = path_idx;
  while (i < path_str.len && path[i] != ']') ++i;
  if (i == path_str.len) {
    string msg = Substitute("Unclosed brackets in json path '$0'",
        AnyValUtil::ToString(path_str));
    ctx->SetError(msg.c_str());
    return -1;
  }
  StringParser::ParseResult parse_res;
  int index = StringParser::StringToInt<int>(number_start, i - path_idx, &parse_res);
  if (parse_res != StringParser::PARSE_SUCCESS || index < 0) {
    const char* failure;
    if (parse_res == StringParser::PARSE_FAILURE) {
      failure = "Failed to parse json path '$0': Expected number at position $1";
    } else if (parse_res == StringParser::PARSE_OVERFLOW) {
      failure = "Failed to parse json path '$0': Index too large at position $1";
    } else {
      DCHECK(parse_res == StringParser::PARSE_SUCCESS && index < 0);
      failure = "Failed to parse json path '$0': Negative index at position $1";
    }
    string msg = Substitute(failure, AnyValUtil::ToString(path_str), path_idx);
    ctx->SetError(msg.c_str());
    return -1;
  }
  RETURN_IF_OOM(SelectByIndex(index, queue, allocator), -1);
  return i + 1;  // i points at ']'
}

/// Parse json_str into Document. Return false for errors.
static bool ParseStringVal(FunctionContext* ctx, const StringVal& json_str,
    JsonUdfDocument* doc) {
  StringValStream stream(&json_str);
  RETURN_IF_OOM(doc->ParseStream(stream), false);
  if (doc->HasParseError()) {
    string msg = Substitute("Failed to parse json at position $0 since: $1."
        " Json string:\n$2", doc->GetErrorOffset(),
        GetParseError_En(doc->GetParseError()), AnyValUtil::ToString(json_str));
    ctx->AddWarning(msg.c_str());
    return false;
  }
  return true;
}

// Initial capacity of the BFS queue used in GetJsonObjectImpl
static const int INITIAL_QUEUE_CAPACITY = 64;

/// TODO(IMPALA-7610): parse the JSON path and cache it so we don't need to parse it
/// everytime
StringVal StringFunctions::GetJsonObjectImpl(FunctionContext* ctx,
    const StringVal& json_str, const StringVal& path_str) {
  if (UNLIKELY(json_str.is_null || json_str.len == 0)) return StringVal::null();
  if (UNLIKELY(path_str.is_null || path_str.len == 0)) {
    ctx->SetError("Empty json path");
    return StringVal::null();
  }
  int beg = 0;
  // Strip off preceding whitespace.
  while (beg < path_str.len && path_str.ptr[beg] == ' ') beg++;
  if (UNLIKELY(beg == path_str.len || path_str.ptr[beg] != '$')) {
    // Here we use '$$' to escape '$' in Substitute
    string msg = Substitute("Failed to parse json path '$0': Should start with '$$'",
        AnyValUtil::ToString(path_str));
    ctx->SetError(msg.c_str());
    return StringVal::null();
  }

  JsonUdfAllocator allocator(ctx);
  JsonUdfDocument document(&allocator);
  if (!ParseStringVal(ctx, json_str, &document)) return StringVal::null();

  // BFS to extract selected values. We use array of RapidJson instead of std::vector to
  // track its memory.
  JsonUdfValue queue(kArrayType);
  RETURN_NULL_IF_OOM(queue.Reserve(INITIAL_QUEUE_CAPACITY, allocator));
  RETURN_NULL_IF_OOM(queue.PushBack(document, allocator));
  const uint8_t* path = path_str.ptr;
  const uint8_t* path_end = path + path_str.len;
  for (int i = beg + 1; i < path_str.len;) {
    // Each round we extract new items into the queue. Old items will be removed.
    switch (path[i]) {
      case '$': {
        string msg = Substitute("Failed to parse json path '$0':"
            " $$ should only be placed at start", AnyValUtil::ToString(path_str));
        ctx->SetError(msg.c_str());
        return StringVal::null();
      }
      case '.': {
        // Hive does not skip the heading and trailing whitespaces since it simply splits
        // the json path by '.'. We should keep the same behavior with MySQL. See
        // JSON_EXTRACT in MySQL(5.7+).
        for (++i; i < path_str.len && path[i] == ' '; ++i);  // skip whitespaces
        if (i == path_str.len) {
          string msg = Substitute("Failed to parse json path '$0': Found a trailing '.'",
              AnyValUtil::ToString(path_str));
          ctx->SetError(msg.c_str());
          return StringVal::null();
        }
        if (path[i] == '*') {
          i = ProcessWildcardKey(ctx, path_str, ++i, &queue, &allocator);
          if (i < 0) return StringVal::null();
          break;
        }
        const uint8_t* start = path + i;
        const uint8_t* end = FindEndOfIdentifier(start, path_end);
        // Set error if looking for an empty key
        if (end == nullptr) {
          string msg = Substitute(
              "Failed to parse json path '$0': Expected key at position $1",
              AnyValUtil::ToString(path_str), i);
          ctx->SetError(msg.c_str());
          return StringVal::null();
        }
        // Convert to string to automatically null terminate.
        string key = string(start, end);
        RETURN_NULL_IF_OOM(SelectByKey(key, &queue, &allocator));
        i += (end - start);
        break;
      }
      case '[': {
        // TODO(IMPALA-7611) support range syntax like [2 to 7] and keyword `last`.
        // Hive has not supported it yet but MySQL does since 8.0.2. See
        // https://dev.mysql.com/worklog/task/?id=9831 and
        // https://github.com/mysql/mysql-server/commit/9f4678a
        for (++i; i < path_str.len && path[i] == ' '; ++i);  // skip whitespaces
        if (i == path_str.len) return StringVal::null();
        if (path[i] == '*') {
          i = ProcessWildcardIndex(ctx, path_str, ++i, &queue, &allocator);
          if (i < 0) return StringVal::null();
          break;
        }
        // else it should be a number
        i = ProcessNumberIndex(ctx, path_str, i, &queue, &allocator);
        if (i < 0) return StringVal::null();
        break;
      }
      case ' ':
        ++i;
        break;
      default: {
        string msg = Substitute(
            "Failed to parse json path '$0': Unexpected char '$1' at position $2",
            AnyValUtil::ToString(path_str), static_cast<char>(path[i]), i);
        ctx->SetError(msg.c_str());
        return StringVal::null();
      }
    }
  }

  return ToStringVal(ctx, queue, &allocator);
}
}
