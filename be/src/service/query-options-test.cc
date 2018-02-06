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

#include "service/query-options.h"

#include <boost/preprocessor/seq/for_each.hpp>
#include <boost/preprocessor/tuple/to_seq.hpp>

#include "runtime/runtime-filter.h"
#include "testutil/gtest-util.h"
#include "util/mem-info.h"

using namespace boost;
using namespace impala;
using namespace std;

constexpr int32_t I32_MAX = numeric_limits<int32_t>::max();
constexpr int64_t I64_MAX = numeric_limits<int64_t>::max();

// A option definition is a key string and a pointer to the actual value in an instance of
// TQueryOptions. For example, the OptionDef for prefetch_mode is
// OptionDef{"prefetch_mode", &options.prefetch_mode}.
// The pointer is used to fetch the field and check equality in test functions.
template<typename T> struct OptionDef {
  using OptionType = T;
  const char* option_name;
  T* option_field;
};

// Expand MAKE_OPTIONDEF(option) into OptionDef{"option", &options.option}
#define MAKE_OPTIONDEF(name) OptionDef<decltype(options.name)>{#name, &options.name}

// Range defines the lower/upper bound restriction for byte/integer options.
template<typename T> struct Range {
  T lower_bound;
  T upper_bound;
};

// A ValueDef{str, value} defines that str should be parsed into value.
template<typename T> struct ValueDef {
  const char* str;
  T value;
};

// A EnumCase consists of an OptionDef and a list of valid value definitions.
// For example, the EnumCase for prefetch_mode is
// EnumCase<TPrefetchMode::type>{OptionDef{"prefetch_mode", &options.prefetch_mode},
//     {{"None", TPrefetchMode::None}, {"HT_BUCKET", TPrefetchMode::HT_BUCKET},}}
template<typename T> struct EnumCase {
  OptionDef<T> option_def;
  vector<ValueDef<T>> value_defs;
};

// Create a shortcut function to test if 'value' would be successfully read as expected
// value.
template<typename T>
auto MakeTestOkFn(TQueryOptions& options, OptionDef<T> option_def) {
  return [&options, option_def](const char* str, const T& expected_value) {
    EXPECT_OK(SetQueryOption(option_def.option_name, str, &options, nullptr));
    EXPECT_EQ(expected_value, *option_def.option_field);
  };
}

// Create a shortcut function to test if 'value' would result into a failure.
template<typename T>
auto MakeTestErrFn(TQueryOptions& options, OptionDef<T> option_def) {
  return [&options, option_def](const char* str) {
    EXPECT_FALSE(SetQueryOption(option_def.option_name, str, &options, nullptr).ok());
  };
}

TEST(QueryOptions, SetNonExistentOptions) {
  TQueryOptions options;
  EXPECT_FALSE(SetQueryOption("", "bar", &options, nullptr).ok());
  EXPECT_FALSE(SetQueryOption("foo", "bar", &options, nullptr).ok());
}

// Test a set of test cases, where a test case is a pair of OptionDef, and a pair of the
// lower and the upper bound.
template<typename T>
void TestByteCaseSet(TQueryOptions& options,
    const vector<pair<OptionDef<T>, Range<T>>>& case_set) {
  for (const auto& test_case: case_set) {
    const OptionDef<T>& option_def = test_case.first;
    const Range<T>& range = test_case.second;
    auto TestOk = MakeTestOkFn(options, option_def);
    auto TestError = MakeTestErrFn(options, option_def);
    for (T boundary: {range.lower_bound, range.upper_bound}) {
      // Byte options treat -1 as 0, which means infinite.
      if (boundary == -1) boundary = 0;
      TestOk(to_string(boundary).c_str(), boundary);
      TestOk((to_string(boundary) + "B").c_str(), boundary);
    }
    TestError(to_string(range.lower_bound - 1).c_str());
    TestError(to_string(static_cast<uint64_t>(range.upper_bound) + 1).c_str());
    TestError("1tb");
    TestError("1%");
    TestError("1%B");
    TestError("1B%");
    TestError("Not a number!");
    ValueDef<int64_t> common_values[] = {
        {"0",   0},
        {"1 B", 1},
        {"0Kb", 0},
        {"4G",  4ll * 1024 * 1024 * 1024},
        {"-1M", -1024 * 1024}
    };
    for (const auto& value_def : common_values) {
      if (value_def.value >= range.lower_bound && value_def.value <= range.upper_bound) {
        TestOk(value_def.str, value_def.value);
      } else {
        TestError(value_def.str);
      }
    }
  }
}

// Test byte size options. Byte size options accept both integers or integers with a
// suffix like "KB". They treat -1 and 0 as infinite. Some of them have a lower bound and
// an upper bound.
TEST(QueryOptions, SetByteOptions) {
  TQueryOptions options;
  // key and its valid range: [(key, (min, max))]
  vector<pair<OptionDef<int64_t>, Range<int64_t>>> case_set_i64 {
      {MAKE_OPTIONDEF(mem_limit),             {-1, I64_MAX}},
      {MAKE_OPTIONDEF(max_scan_range_length), {-1, I64_MAX}},
      {MAKE_OPTIONDEF(buffer_pool_limit),     {-1, I64_MAX}},
      {MAKE_OPTIONDEF(max_row_size),          {1, ROW_SIZE_LIMIT}},
      {MAKE_OPTIONDEF(parquet_file_size),     {-1, I32_MAX}},
      {MAKE_OPTIONDEF(compute_stats_min_sample_size), {-1, I64_MAX}},
  };
  vector<pair<OptionDef<int32_t>, Range<int32_t>>> case_set_i32 {
      {MAKE_OPTIONDEF(runtime_filter_min_size),
          {RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE,
              RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE}},
      // Lower limit for runtime_filter_max_size is FLAGS_min_buffer_size which has a
      // default value of is 8KB.
      {MAKE_OPTIONDEF(runtime_filter_max_size),
          {8 * 1024, RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE}},
      {MAKE_OPTIONDEF(runtime_bloom_filter_size),
          {RuntimeFilterBank::MIN_BLOOM_FILTER_SIZE,
              RuntimeFilterBank::MAX_BLOOM_FILTER_SIZE}}
  };
  TestByteCaseSet(options, case_set_i64);
  TestByteCaseSet(options, case_set_i32);
}

// Test an enum test case. It may or may not accept integers.
template<typename T>
void TestEnumCase(TQueryOptions& options, const EnumCase<T>& test_case,
    bool accept_integer) {
  auto TestOk = MakeTestOkFn(options, test_case.option_def);
  auto TestError = MakeTestErrFn(options, test_case.option_def);
  TestError("foo");
  TestError("1%");
  TestError("-1");
  // Test max enum value + 1
  TestError(to_string(test_case.value_defs.back().value + 1).c_str());
  for (const auto& value_def: test_case.value_defs) {
    TestOk(value_def.str, value_def.value);
    string numeric_str = to_string(value_def.value);
    if (accept_integer) {
      TestOk(numeric_str.c_str(), value_def.value);
    } else {
      TestError(numeric_str.c_str());
    }
  }
}

// Test enum options. Some enum options accept integers while others don't.
TEST(QueryOptions, SetEnumOptions) {
  // A set of macros expanding to an EnumDef.
  // ENTRY(_, TPrefetchMode, None) expands to {"None", TPrefetchMode::None},
#define ENTRY(_, prefix, entry) {BOOST_PP_STRINGIZE(entry), prefix::entry},

  // ENTRIES(TPrefetchMode, (None)(HT_BUCKET)) expands to
  // {"None", TPrefetchMode::None}, {"HT_BUCKET", TPrefetchMode::HT_BUCKET},
#define ENTRIES(prefix, name) BOOST_PP_SEQ_FOR_EACH(ENTRY, prefix, name)

  // CASE(prefetch_mode, TPrefetchMode, (NONE, HT_BUCKET)) expands to:
  // EnumCase{OptionDef{"prefetch_mode", &options.prefetch_mode},
  //     {{"None", TPrefetchMode::None}, {"HT_BUCKET", TPrefetchMode::HT_BUCKET},}}
#define CASE(key, enumtype, enums) EnumCase<decltype(options.key)> { \
    MAKE_OPTIONDEF(key), {ENTRIES(enumtype, BOOST_PP_TUPLE_TO_SEQ(enums))}}

  TQueryOptions options;
  TestEnumCase(options, CASE(prefetch_mode, TPrefetchMode, (NONE, HT_BUCKET)), true);
  TestEnumCase(options, CASE(default_join_distribution_mode, TJoinDistributionMode,
      (BROADCAST, SHUFFLE)), true);
  TestEnumCase(options, CASE(explain_level, TExplainLevel,
      (MINIMAL, STANDARD, EXTENDED, VERBOSE)), true);
  TestEnumCase(options, CASE(parquet_fallback_schema_resolution,
      TParquetFallbackSchemaResolution, (POSITION, NAME)), true);
  TestEnumCase(options, CASE(parquet_array_resolution, TParquetArrayResolution,
      (THREE_LEVEL, TWO_LEVEL, TWO_LEVEL_THEN_THREE_LEVEL)), true);
  TestEnumCase(options, CASE(seq_compression_mode, THdfsSeqCompressionMode,
      (BLOCK, RECORD)), false);
  TestEnumCase(options, CASE(compression_codec, THdfsCompression,
      (NONE, GZIP, BZIP2, DEFAULT, SNAPPY, SNAPPY_BLOCKED)), false);
#undef CASE
#undef ENTRIES
#undef ENTRY
}

// Test integer options. Some of them have lower/upper bounds.
TEST(QueryOptions, SetIntOptions) {
  TQueryOptions options;
  // List of pairs of Key and its valid range
  pair<OptionDef<int32_t>, Range<int32_t>> case_set[] {
      {MAKE_OPTIONDEF(runtime_filter_wait_time_ms),    {0, I32_MAX}},
      {MAKE_OPTIONDEF(mt_dop),                         {0, 64}},
      {MAKE_OPTIONDEF(disable_codegen_rows_threshold), {0, I32_MAX}},
      {MAKE_OPTIONDEF(max_num_runtime_filters),        {0, I32_MAX}},
      {MAKE_OPTIONDEF(batch_size),                     {0, 65536}},
      {MAKE_OPTIONDEF(query_timeout_s),                {0, I32_MAX}},
      {MAKE_OPTIONDEF(exec_time_limit_s),              {0, I32_MAX}},
  };
  for (const auto& test_case : case_set) {
    const OptionDef<int32_t>& option_def = test_case.first;
    const Range<int32_t>& range = test_case.second;
    auto TestOk = MakeTestOkFn(options, option_def);
    auto TestError = MakeTestErrFn(options, option_def);
    TestError("1M");
    TestError("0B");
    TestError("1%");
    TestOk(to_string(range.lower_bound).c_str(), range.lower_bound);
    TestOk(to_string(range.upper_bound).c_str(), range.upper_bound);
    TestError(to_string(int64_t(range.lower_bound) - 1).c_str());
    TestError(to_string(int64_t(range.upper_bound) + 1).c_str());
  }
}

// Test options with non regular validation rule
TEST(QueryOptions, SetSpecialOptions) {
  // REPLICA_PREFERENCE has unsettable enum values: cache_rack(1) & disk_rack(3)
  TQueryOptions options;
  {
    OptionDef<TReplicaPreference::type> key_def = MAKE_OPTIONDEF(replica_preference);
    auto TestOk = MakeTestOkFn(options, key_def);
    auto TestError = MakeTestErrFn(options, key_def);
    TestOk("cache_local", TReplicaPreference::CACHE_LOCAL);
    TestOk("0", TReplicaPreference::CACHE_LOCAL);
    TestError("cache_rack");
    TestError("1");
    TestOk("disk_local", TReplicaPreference::DISK_LOCAL);
    TestOk("2", TReplicaPreference::DISK_LOCAL);
    TestError("disk_rack");
    TestError("3");
    TestOk("remote", TReplicaPreference::REMOTE);
    TestOk("4", TReplicaPreference::REMOTE);
    TestError("5");
  }
  // SCRATCH_LIMIT is a byte option where, unlike other byte options, -1 is not treated as
  // 0
  {
    OptionDef<int64_t> key_def = MAKE_OPTIONDEF(scratch_limit);
    auto TestOk = MakeTestOkFn(options, key_def);
    auto TestError = MakeTestErrFn(options, key_def);
    TestOk("-1", -1);
    TestOk("0", 0);
    TestOk("4GB", 4ll * 1024 * 1024 * 1024);
    TestError("-1MB");
    TestError("1tb");
    TestError("1%");
    TestError("1%B");
    TestError("1B%");
    TestOk("1 B", 1);
    TestError("Not a number!");
  }
  // DEFAULT_SPILLABLE_BUFFER_SIZE and MIN_SPILLABLE_BUFFER_SIZE accepts -1, 0 and memory
  // value that is power of 2
  {
    OptionDef<int64_t> key_def_default = MAKE_OPTIONDEF(default_spillable_buffer_size);
    OptionDef<int64_t> key_def_min = MAKE_OPTIONDEF(min_spillable_buffer_size);
    for (const auto& key_def : {key_def_min, key_def_default}) {
      auto TestOk = MakeTestOkFn(options, key_def);
      auto TestError = MakeTestErrFn(options, key_def);
      TestOk("-1", 0);
      TestOk("-1B", 0);
      TestOk("0", 0);
      TestOk("0B", 0);
      TestOk("1", 1);
      TestOk("2", 2);
      TestError("3");
      TestOk("1K", 1024);
      TestOk("2MB", 2 * 1024 * 1024);
      TestOk("32G", 32ll * 1024 * 1024 * 1024);
      TestError("10MB");
      TestOk(to_string(SPILLABLE_BUFFER_LIMIT).c_str(), SPILLABLE_BUFFER_LIMIT);
      TestError(to_string(2 * SPILLABLE_BUFFER_LIMIT).c_str());
    }
  }
  // MAX_ROW_SIZE should be between 1 and ROW_SIZE_LIMIT
  {
    OptionDef<int64_t> key_def = MAKE_OPTIONDEF(max_row_size);
    auto TestError = MakeTestErrFn(options, key_def);
    TestError("-1");
    TestError("0");
    TestError(to_string(ROW_SIZE_LIMIT + 1).c_str());
  }
  // RUNTIME_FILTER_MAX_SIZE should not be less than FLAGS_min_buffer_size
  {
    OptionDef<int32_t> key_def = MAKE_OPTIONDEF(runtime_filter_max_size);
    auto TestOk = MakeTestOkFn(options, key_def);
    auto TestError = MakeTestErrFn(options, key_def);
    TestOk("128KB", 128 * 1024);
    TestError("8191"); // default value of FLAGS_min_buffer_size is 8KB
    TestOk("64KB", 64 * 1024);
  }
}

TEST(QueryOptions, ParseQueryOptions) {
  QueryOptionsMask expectedMask;
  expectedMask.set(TImpalaQueryOptions::NUM_NODES);
  expectedMask.set(TImpalaQueryOptions::MEM_LIMIT);

  {
    TQueryOptions options;
    QueryOptionsMask mask;
    EXPECT_OK(ParseQueryOptions("num_nodes=1,mem_limit=42", &options, &mask));
    EXPECT_EQ(options.num_nodes, 1);
    EXPECT_EQ(options.mem_limit, 42);
    EXPECT_EQ(mask, expectedMask);
  }

  {
    TQueryOptions options;
    QueryOptionsMask mask;
    Status status = ParseQueryOptions("num_nodes=1,mem_limit:42,foo=bar,mem_limit=42",
        &options, &mask);
    EXPECT_EQ(options.num_nodes, 1);
    EXPECT_EQ(options.mem_limit, 42);
    EXPECT_EQ(mask, expectedMask);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.msg().details().size(), 2);
  }
}

TEST(QueryOptions, MapOptionalDefaultlessToEmptyString) {
  TQueryOptions options;
  map<string, string> map;

  TQueryOptionsToMap(options, &map);
  // No default set
  EXPECT_EQ(map["COMPRESSION_CODEC"], "");
  EXPECT_EQ(map["MT_DOP"], "");
  // Has defaults
  EXPECT_EQ(map["EXPLAIN_LEVEL"], "1");
}

/// Overlay a with b. batch_size is set in both places.
/// num_nodes is set in a and is missing from the bit
/// mask. mt_dop is only set in b.
TEST(QueryOptions, TestOverlay) {
  TQueryOptions a;
  TQueryOptions b;
  QueryOptionsMask mask;

  // overlay
  a.__set_batch_size(1);
  b.__set_batch_size(2);
  mask.set(TImpalaQueryOptions::BATCH_SIZE);

  // no overlay
  a.__set_num_nodes(3);

  // missing mask on overlay
  b.__set_debug_action("ignored"); // no mask set

  // overlay; no original value
  b.__set_mt_dop(4);
  mask.set(TImpalaQueryOptions::MT_DOP);

  TQueryOptions dst = a;
  OverlayQueryOptions(b, mask, &dst);

  EXPECT_EQ(2, dst.batch_size);
  EXPECT_TRUE(dst.__isset.batch_size);
  EXPECT_EQ(3, dst.num_nodes);
  EXPECT_EQ(a.debug_action, dst.debug_action);
  EXPECT_EQ(4, dst.mt_dop);
  EXPECT_TRUE(dst.__isset.mt_dop);
}

TEST(QueryOptions, ResetToDefaultViaEmptyString) {
  // MT_DOP has no default; resetting should do nothing.
  {
    TQueryOptions options;
    EXPECT_TRUE(SetQueryOption("MT_DOP", "", &options, NULL).ok());
    EXPECT_FALSE(options.__isset.mt_dop);
  }

  // Set and then reset; check mask too.
  {
    TQueryOptions options;
    QueryOptionsMask mask;

    EXPECT_FALSE(options.__isset.mt_dop);
    EXPECT_TRUE(SetQueryOption("MT_DOP", "3", &options, &mask).ok());
    EXPECT_TRUE(mask[TImpalaQueryOptions::MT_DOP]);
    EXPECT_TRUE(SetQueryOption("MT_DOP", "", &options, &mask).ok());
    EXPECT_FALSE(options.__isset.mt_dop);
    // After reset, mask should be clear.
    EXPECT_FALSE(mask[TImpalaQueryOptions::MT_DOP]);
  }

  // Reset should reset to defaults for something that has set defaults.
  {
    TQueryOptions options;

    EXPECT_TRUE(SetQueryOption("EXPLAIN_LEVEL", "3", &options, NULL).ok());
    EXPECT_TRUE(SetQueryOption("EXPLAIN_LEVEL", "", &options, NULL).ok());
    EXPECT_TRUE(options.__isset.explain_level);
    EXPECT_EQ(options.explain_level, 1);
  }
}

IMPALA_TEST_MAIN();
