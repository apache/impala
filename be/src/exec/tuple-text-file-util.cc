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

#include <unordered_map>

#include "exec/tuple-text-file-reader.h"
#include "exec/tuple-text-file-util.h"

#include "common/names.h"

using std::unordered_map;

namespace impala {

const char* DEBUG_TUPLE_CACHE_BAD_POSTFIX = ".bad";

struct ReferenceRowCount {
  uint64_t reference_rows_count;
  uint64_t comparison_rows_count;
  ReferenceRowCount() : reference_rows_count(0), comparison_rows_count(0) {}
  void setCounts(uint64_t ref_rows_cnt, uint64_t cmp_rows_cnt) {
    reference_rows_count = ref_rows_cnt;
    comparison_rows_count = cmp_rows_cnt;
  }
};
typedef unordered_map<string, ReferenceRowCount> ReferenceRowsMap;
typedef std::function<Status(const string&)> TupleCacheRowFunction;
// Applies fn to each row read from reader. Returns the number of rows read.
static Status ForEachRow(
    TupleTextFileReader* reader, TupleCacheRowFunction& fn, int64_t* row_count) {
  DCHECK(reader != nullptr);
  DCHECK(row_count != nullptr);
  bool eos = false;
  int64_t num_rows = 0;
  Status read_status;
  string row_str;
  do {
    RETURN_IF_ERROR(reader->GetNext(&row_str, &eos));
    RETURN_IF_ERROR(fn(row_str));
    num_rows++;
  } while (!eos);
  *row_count = num_rows;
  return Status::OK();
}

static Status VerifyRowsFromComparisonFile(const string& ref_file_path,
    ReferenceRowsMap& cache, TupleTextFileReader* reader, int64_t* row_count) {
  TupleCacheRowFunction verify_fn = [&ref_file_path, &cache, reader](const string& str) {
    auto iter = cache.find(str);
    if (iter == cache.end()) {
      return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
          Substitute("Result '$0' of file '$1' doesn't exist in the reference file: '$2'",
              str, reader->GetPath() + DEBUG_TUPLE_CACHE_BAD_POSTFIX,
              ref_file_path + DEBUG_TUPLE_CACHE_BAD_POSTFIX));
    }
    iter->second.comparison_rows_count++;
    return Status::OK();
  };
  return ForEachRow(reader, verify_fn, row_count);
}

static Status InsertRowsFromReferenceFile(
    ReferenceRowsMap& cache, TupleTextFileReader* reader, int64_t* row_count) {
  TupleCacheRowFunction insert_fn = [&cache](const string& str) {
    cache[str].reference_rows_count++;
    return Status::OK();
  };
  return ForEachRow(reader, insert_fn, row_count);
}

static Status VerifyRowCount(ReferenceRowsMap& cache) {
  for (const auto& it : cache) {
    DCHECK_GE(it.second.reference_rows_count, 0);
    if (it.second.reference_rows_count == 0) {
      return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
          Substitute("Row count abnormal for key '$0', '$1' vs '$2'", it.first,
              it.second.reference_rows_count, it.second.comparison_rows_count));
    }
    if (it.second.reference_rows_count != it.second.comparison_rows_count) {
      return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
          Substitute("Row count doesn't match for key '$0', '$1' vs '$2'", it.first,
              it.second.reference_rows_count, it.second.comparison_rows_count));
    }
  }
  return Status::OK();
}

Status TupleTextFileUtil::VerifyRows(
    const string& cmp_file_path, const string& ref_file_path) {
  ReferenceRowsMap cache;
  int64_t ref_row_count = 0;
  int64_t cmp_row_count = 0;
  {
    TupleTextFileReader ref_reader(ref_file_path);
    RETURN_IF_ERROR(ref_reader.Open());
    RETURN_IF_ERROR(InsertRowsFromReferenceFile(cache, &ref_reader, &ref_row_count));
  }
  // Verify all the rows.
  {
    TupleTextFileReader cmp_reader(cmp_file_path);
    RETURN_IF_ERROR(cmp_reader.Open());
    RETURN_IF_ERROR(
        VerifyRowsFromComparisonFile(ref_file_path, cache, &cmp_reader, &cmp_row_count));
  }

  // Verify all the row counts.
  if (ref_row_count != cmp_row_count) {
    return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
        Substitute(
            "Row count different. Reference file '$0': '$1', comparison file '$2': '$3'",
            ref_file_path + DEBUG_TUPLE_CACHE_BAD_POSTFIX, ref_row_count,
            cmp_file_path + DEBUG_TUPLE_CACHE_BAD_POSTFIX, cmp_row_count));
  }
  return VerifyRowCount(cache);
}

static Status CacheFileCmp(
    const std::string& path_a, const std::string& path_b, bool* passed) {
  DCHECK(passed != nullptr);
  *passed = false;

  // Create readers for the two files.
  TupleTextFileReader reader_a(path_a);
  TupleTextFileReader reader_b(path_b);

  // Open both files.
  if (!reader_a.Open().ok()) {
    return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
        Substitute("Failed to open file '$0'", path_a + DEBUG_TUPLE_CACHE_BAD_POSTFIX));
  }
  if (!reader_b.Open().ok()) {
    return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
        Substitute("Failed to open file '$0'", path_b + DEBUG_TUPLE_CACHE_BAD_POSTFIX));
  }

  // Compare file sizes.
  int file1_length = reader_a.GetFileSize();
  int file2_length = reader_b.GetFileSize();
  if (file1_length != file2_length || file1_length == TUPLE_TEXT_FILE_SIZE_ERROR) {
    return Status(TErrorCode::TUPLE_CACHE_INCONSISTENCY,
        Substitute("Size of file '$0' (size: $1) and '$2' (size: $3) are different",
            path_a + DEBUG_TUPLE_CACHE_BAD_POSTFIX, file1_length,
            path_b + DEBUG_TUPLE_CACHE_BAD_POSTFIX, file2_length));
  }
  // Reset readers to the beginning of the files.
  reader_a.Rewind();
  reader_b.Rewind();

  // Compare the content line by line.
  string line_a, line_b;
  bool eos_a = false, eos_b = false;
  *passed = true;
  while (true) {
    // Read the next line from each file.
    RETURN_IF_ERROR(reader_a.GetNext(&line_a, &eos_a));
    RETURN_IF_ERROR(reader_b.GetNext(&line_b, &eos_b));
    // If both files reached the end, the comparison is complete.
    if (eos_a && eos_b) break;
    DCHECK_EQ(eos_a, eos_b);
    // If the lines differ, the files are not identical.
    if (line_a != line_b) {
      *passed = false;
      break;
    }
  }
  return Status::OK();
}

Status TupleTextFileUtil::VerifyDebugDumpCache(
    const string& file_name, const string& ref_file_name, bool* passed) {
  DCHECK(passed != nullptr);
  *passed = true;
  DCHECK(!ref_file_name.empty());
  DCHECK(!file_name.empty());
  // This is the fast path to compare the content of the files, passed will be set
  // to true if all contents are the same.
  return CacheFileCmp(ref_file_name, file_name, passed);
}
} // namespace impala
