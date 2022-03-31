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

#include "exec/parquet/parquet-common.h"

namespace impala {

/// Mapping of Parquet codec enums to Impala enums
const THdfsCompression::type PARQUET_TO_IMPALA_CODEC[] = {
  THdfsCompression::NONE,
  THdfsCompression::SNAPPY,
  THdfsCompression::GZIP,
  THdfsCompression::LZO,
  THdfsCompression::BROTLI,
  THdfsCompression::LZ4_BLOCKED,
  THdfsCompression::ZSTD
};

const int PARQUET_TO_IMPALA_CODEC_SIZE =
    sizeof(PARQUET_TO_IMPALA_CODEC) / sizeof(PARQUET_TO_IMPALA_CODEC[0]);

/// Mapping of Impala codec enums to Parquet enums
const parquet::CompressionCodec::type IMPALA_TO_PARQUET_CODEC[] = {
  parquet::CompressionCodec::UNCOMPRESSED,
  parquet::CompressionCodec::SNAPPY, // DEFAULT
  parquet::CompressionCodec::GZIP,   // GZIP
  parquet::CompressionCodec::GZIP,   // DEFLATE
  // Placeholder for BZIP2 which isn't a valid parquet codec.
  parquet::CompressionCodec::SNAPPY, // BZIP2
  parquet::CompressionCodec::SNAPPY,
  parquet::CompressionCodec::SNAPPY, // SNAPPY_BLOCKED
  parquet::CompressionCodec::LZO,
  parquet::CompressionCodec::LZ4,
  parquet::CompressionCodec::GZIP,   // ZLIB
  parquet::CompressionCodec::ZSTD,
  parquet::CompressionCodec::BROTLI,
  parquet::CompressionCodec::LZ4     // LZ4_BLOCKED
};

const int IMPALA_TO_PARQUET_CODEC_SIZE =
    sizeof(IMPALA_TO_PARQUET_CODEC) / sizeof(IMPALA_TO_PARQUET_CODEC[0]);

THdfsCompression::type ConvertParquetToImpalaCodec(
    parquet::CompressionCodec::type codec) {
  DCHECK_GE(codec, 0);
  DCHECK_LT(codec, PARQUET_TO_IMPALA_CODEC_SIZE);
  return PARQUET_TO_IMPALA_CODEC[codec];
}

parquet::CompressionCodec::type ConvertImpalaToParquetCodec(
    THdfsCompression::type codec) {
  DCHECK_GE(codec, 0);
  DCHECK_LT(codec, IMPALA_TO_PARQUET_CODEC_SIZE);
  return IMPALA_TO_PARQUET_CODEC[codec];
}

void GetRowRangeForPage(const parquet::RowGroup& row_group,
    const parquet::OffsetIndex& offset_index, int page_idx, RowRange* row_range) {
  const auto& page_locations = offset_index.page_locations;
  DCHECK_LT(page_idx, page_locations.size());
  row_range->first = page_locations[page_idx].first_row_index;
  if (page_idx == page_locations.size() - 1) {
    row_range->last = row_group.num_rows - 1;
  } else {
    row_range->last = page_locations[page_idx + 1].first_row_index - 1;
  }
}

void GetRowRangeForPageRange(const parquet::RowGroup& row_group,
    const parquet::OffsetIndex& offset_index, const PageRange& page_range,
    RowRange* row_range) {
  const auto& page_locations = offset_index.page_locations;
  DCHECK_GE(page_range.first, 0);
  DCHECK_LE(page_range.first, page_range.last);
  DCHECK_LT(page_range.last, page_locations.size());
  row_range->first = page_locations[page_range.first].first_row_index;
  if (page_range.last == page_locations.size() - 1) {
    row_range->last = row_group.num_rows - 1;
  } else {
    row_range->last = page_locations[page_range.last + 1].first_row_index - 1;
  }
}

static bool ValidateRowRangesData(const vector<RowRange>& skip_ranges,
    const int64_t num_rows) {
  for (auto& range : skip_ranges) {
    if (range.first > range.last || range.first < 0 || range.last >= num_rows) {
      return false;
    }
  }
  return true;
}

bool ComputeCandidateRanges(const int64_t num_rows, vector<RowRange>* skip_ranges,
    vector<RowRange>* candidate_ranges) {
  if (!ValidateRowRangesData(*skip_ranges, num_rows)) return false;
  sort(skip_ranges->begin(), skip_ranges->end());
  candidate_ranges->clear();
  // 'skip_end' tracks the end of a continuous range of rows that needs to be skipped.
  // 'skip_ranges' are sorted, so we can start at the beginning.
  int skip_end = -1;
  for (auto& skip_range : *skip_ranges) {
    if (skip_end + 1 >= skip_range.first) {
      // We can extend 'skip_end' to the end of 'skip_range'.
      if (skip_end < skip_range.last) skip_end = skip_range.last;
    } else {
      // We found a gap in 'skip_ranges', i.e. a row range that is not covered by
      // 'skip_ranges'.
      candidate_ranges->push_back({skip_end + 1, skip_range.first - 1});
      // Let's track the end of the next continuous range that needs to be skipped.
      skip_end = skip_range.last;
    }
  }
  // If the last skip ended before 'range_end', add the remaining range to
  // the filtered ranges.
  if (skip_end < num_rows - 1) {
    candidate_ranges->push_back({skip_end + 1, num_rows - 1});
  }
  return true;
}

static bool ValidatePageLocations(const vector<parquet::PageLocation>& page_locations,
    const int64_t num_rows) {
  int last_valid_idx = -1;
  for (int i = 0; i < page_locations.size(); ++i) {
    auto& page_loc = page_locations[i];
    if (!IsValidPageLocation(page_loc, num_rows)) {
      // Skip page locations for empty pages.
      if (page_loc.compressed_page_size == 0) {
        continue;
      } else {
        return false;
      }
    }
    if (last_valid_idx != -1) {
      auto& last_valid_page = page_locations[last_valid_idx];
      // 'first_row_index' must have progressed in a non-empty page.
      if (page_loc.first_row_index <= last_valid_page.first_row_index) return false;
    }
    last_valid_idx = i;
  }
  return true;
}

static bool RangesIntersect(const RowRange& lhs,
    const RowRange& rhs) {
  int64_t higher_first = std::max(lhs.first, rhs.first);
  int64_t lower_last = std::min(lhs.last, rhs.last);
  return higher_first <= lower_last;
}

bool ComputeCandidatePages(
    const vector<parquet::PageLocation>& page_locations,
    const vector<RowRange>& candidate_ranges,
    const int64_t num_rows, vector<int>* candidate_pages) {
  if (!ValidatePageLocations(page_locations, num_rows)) return false;

  int range_idx = 0;
  int page_idx = 0;
  while (page_idx < page_locations.size()) {
    auto& page_location = page_locations[page_idx];
    if (page_location.compressed_page_size == 0) {
      ++page_idx;
      continue;
    }
    int next_page_idx = page_idx + 1;
    while (next_page_idx < page_locations.size() &&
           page_locations[next_page_idx].compressed_page_size == 0) {
      ++next_page_idx;
    }
    int64_t page_start = page_location.first_row_index;
    int64_t page_end = next_page_idx < page_locations.size() ?
                       page_locations[next_page_idx].first_row_index - 1 :
                       num_rows - 1;
    while (range_idx < candidate_ranges.size() &&
        candidate_ranges[range_idx].last < page_start) {
      ++range_idx;
    }
    if (range_idx >= candidate_ranges.size()) break;
    if (RangesIntersect(candidate_ranges[range_idx], {page_start, page_end})) {
      candidate_pages->push_back(page_idx);
    }
    page_idx = next_page_idx;
  }
  // When there are candidate ranges, then we should have at least one candidate page.
  if (!candidate_ranges.empty() && candidate_pages->empty()) return false;
  return true;
}

bool ParquetTimestampDecoder::GetTimestampInfoFromSchema(const parquet::SchemaElement& e,
    Precision& precision, bool& needs_conversion) {
  if (e.type == parquet::Type::INT96) {
    // Metadata does not contain information about being UTC normalized or not. The
    // caller may override 'needs_conversion' depending on flags and writer.
    needs_conversion = false;
    precision = NANO;
    return true;
  } else if (e.type != parquet::Type::INT64) {
    // Timestamps can be only encoded as INT64 or INT96, return false for other types.
    return false;
  }

  if (e.__isset.logicalType) {
    if (!e.logicalType.__isset.TIMESTAMP) return false;

    // Logical type (introduced in PARQUET-1253) contains explicit information about
    // being UTC normalized or not.
    needs_conversion = e.logicalType.TIMESTAMP.isAdjustedToUTC;

    if (e.logicalType.TIMESTAMP.unit.__isset.MILLIS) {
      precision = ParquetTimestampDecoder::MILLI;
    }
    else if (e.logicalType.TIMESTAMP.unit.__isset.MICROS) {
      precision = ParquetTimestampDecoder::MICRO;
    }
    else if (e.logicalType.TIMESTAMP.unit.__isset.NANOS) {
      precision = ParquetTimestampDecoder::NANO;
    } else {
      return false;
    }
  } else if (e.__isset.converted_type) {
    // Converted type does not contain information about being UTC normalized or not.
    // Timestamp with converted type but without logical type are/were never written
    // by Impala, so it is assumed that the writer is Parquet-mr and that timezone
    // conversion is needed.
    needs_conversion = true;
    if (e.converted_type == parquet::ConvertedType::TIMESTAMP_MILLIS) {
      precision = ParquetTimestampDecoder::MILLI;
    }
    else if (e.converted_type == parquet::ConvertedType::TIMESTAMP_MICROS) {
      precision = ParquetTimestampDecoder::MICRO;
    } else {
      // There is no TIMESTAMP_NANO converted type.
      return false;
    }
  } else {
    // Either logical or converted type must be set for int64 timestamps.
    return false;
  }
  return true;
}

ParquetTimestampDecoder::ParquetTimestampDecoder(const parquet::SchemaElement& e,
    const Timezone* timezone, bool convert_int96_timestamps) {
  bool needs_conversion = false;
  bool valid_schema = GetTimestampInfoFromSchema(e, precision_, needs_conversion);
  DCHECK(valid_schema); // Invalid schemas should be rejected in an earlier step.
  if (e.type == parquet::Type::INT96 && convert_int96_timestamps) needs_conversion = true;
  if (needs_conversion) timezone_ = timezone;
}

void ParquetTimestampDecoder::ConvertMinStatToLocalTime(TimestampValue* v) const {
  DCHECK(timezone_ != nullptr);
  if (!v->HasDateAndTime()) return;
  TimestampValue repeated_period_start;
  v->UtcToLocal(*timezone_, &repeated_period_start);
  if (repeated_period_start.HasDateAndTime()) *v = repeated_period_start;
}

void ParquetTimestampDecoder::ConvertMaxStatToLocalTime(TimestampValue* v) const {
  DCHECK(timezone_ != nullptr);
  if (!v->HasDateAndTime()) return;
  TimestampValue repeated_period_end;
  v->UtcToLocal(*timezone_, nullptr, &repeated_period_end);
  if (repeated_period_end.HasDateAndTime()) *v = repeated_period_end;
}
}
