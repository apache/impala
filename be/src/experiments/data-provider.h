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

#pragma once

#include <math.h>
#include <cstdint>
#include <iosfwd>
#include <limits>
#include <boost/generator_iterator.hpp>
#include <boost/random/linear_congruential.hpp>
#include <boost/random/uniform_int.hpp>
#include <boost/random/uniform_real.hpp>
#include <boost/random/variate_generator.hpp>
#include <boost/scoped_ptr.hpp>

#include "runtime/mem-pool.h"
#include "runtime/types.h"
#include "runtime/string-value.h"
#include "util/runtime-profile.h"

/// This is a test utility class that can generate data that is similar to the tuple
/// data we use.
/// It can accept columns descriptions and generates rows (in batches) with an iterator
/// interface.
//
/// See data-provider-test.cc on how to use this.
//
/// TODO: provide a way to have better control over the pool strings are allocated to
/// TODO: provide a way to control data skew.  This is pretty easy with the boost rand
/// classes.
class DataProvider {
 public:
  struct Value {
    union {
      bool b;
      int8_t int8;
      int16_t int16;
      int32_t int32;
      int64_t int64;
      float f;
      double d;
    };
    impala::StringValue s;
  };

  /// How the data should be generated.
  enum DataGen {
    UNIFORM_RANDOM,
    SEQUENTIAL,
  };

  class ColDesc {
   public:
    /// Create a column desc with min/max range and the data gen type
    template<typename T>
    static ColDesc Create(const T& min, const T& max, DataGen gen = UNIFORM_RANDOM);

   private:
    friend class DataProvider;

    /// Generates a column value between [min,max) for this column.
    /// d is a random value between [0,1] and i is the row index.
    template<typename T>
    T Generate(double d, int i) const;


    ColDesc(impala::PrimitiveType type, int bytes) {
      this->type = type;
      this->bytes = bytes;
    }

    /// Default generator - used for int and float types
    template<typename T>
    T Generate(double d, int i, T min, T max) const {
      switch (gen_type) {
        case UNIFORM_RANDOM:
          return (T)(d * (max - min) + min);
        case SEQUENTIAL:
          return (T)(i % (int64_t)(max - min) + min);
      }
      return 0;
    }

    impala::PrimitiveType type;
    Value min, max;
    DataGen gen_type;
    int bytes;
  };

  /// Create a data provider object with a pool for allocating memory and a
  /// profile to collect metrics.
  DataProvider(impala::MemPool* pool, impala::RuntimeProfile* profile);

  /// Reset the generator with the column description.
  ///  - num_rows: total rows to generate
  ///  - batch_size: size of generated batches from NextBatch
  /// Data returned via previous NextBatch calls is no longer valid
  void Reset(int num_rows, int batch_size, const std::vector<ColDesc>& columns);

  /// Sets the seed to use for randomly generated data.  The default generator will
  /// use seed(0)
  void SetSeed(int seed);

  /// The size of a row (tuple size)
  int row_size() const { return row_size_; }

  /// The total number of rows that will be generated
  int total_rows() const { return num_rows_; }

  /// Generated the next batch, returning a pointer to the start of the batch
  /// and the number of rows generated.
  /// Returns NULL/0 when the generator is done.
  void* NextBatch(int* rows_returned);

  /// Print the row data in csv format.
  void Print(std::ostream*, char* data, int num_rows) const;

 private:
  impala::MemPool* pool_;
  impala::RuntimeProfile* profile_;
  int num_rows_;
  int batch_size_;
  int rows_returned_;
  boost::scoped_ptr<char> data_;
  int row_size_;
  boost::minstd_rand rand_generator_;
  std::vector<ColDesc> cols_;

  impala::RuntimeProfile::Counter* bytes_generated_;
};

template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<bool>(
    const bool& min, const bool &max, DataGen gen) {
  ColDesc c(impala::TYPE_BOOLEAN, 1);
  c.min.b = min;
  c.max.b = max;
  c.gen_type = gen;
  return c;
}
template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<int8_t>(
    const int8_t& min, const int8_t& max, DataGen gen) {
  ColDesc c(impala::TYPE_TINYINT, 1);
  c.min.int8 = min;
  c.max.int8 = max;
  c.gen_type = gen;
  return c;
}
template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<int16_t>(
    const int16_t& min, const int16_t& max, DataGen gen) {
  ColDesc c(impala::TYPE_SMALLINT, 2);
  c.min.int16 = min;
  c.max.int16 = max;
  c.gen_type = gen;
  return c;
}
template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<int32_t>(
    const int32_t& min, const int32_t& max, DataGen gen) {
  ColDesc c(impala::TYPE_INT, 4);
  c.min.int32 = min;
  c.max.int32 = max;
  c.gen_type = gen;
  return c;
}
template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<int64_t>(
    const int64_t& min, const int64_t& max, DataGen gen) {
  ColDesc c(impala::TYPE_BIGINT, 8);
  c.min.int64 = min;
  c.max.int64 = max;
  c.gen_type = gen;
  return c;
}
template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<float>(
    const float& min, const float& max, DataGen gen) {
  ColDesc c(impala::TYPE_FLOAT, 4);
  c.min.f = min;
  c.max.f = max;
  c.gen_type = gen;
  return c;
}
template<>
inline DataProvider::ColDesc DataProvider::ColDesc::Create<double>(
    const double& min, const double& max, DataGen gen) {
  ColDesc c(impala::TYPE_DOUBLE, 8);
  c.min.d = min;
  c.max.d = max;
  c.gen_type = gen;
  return c;
}
template<> inline
DataProvider::ColDesc DataProvider::ColDesc::Create<impala::StringValue>(
    const impala::StringValue& min, const impala::StringValue& max, DataGen gen) {
  ColDesc c(impala::TYPE_STRING, 16);
  c.min.s = min;
  c.max.s = max;
  c.gen_type = gen;
  return c;
}


template<> inline bool DataProvider::ColDesc::Generate<bool>(double d, int i) const {
  switch (gen_type) {
    case UNIFORM_RANDOM:
      return (int)(round(d * max.b - min.b)) + min.b;
    case SEQUENTIAL:
      return (i % 2) ? true : false;
  }
  return false;
}
template<> inline int8_t DataProvider::ColDesc::Generate<int8_t>(double d, int i) const {
  return Generate<int8_t>(d, i, min.int8, max.int8);
}
template<> inline int16_t DataProvider::ColDesc::Generate<int16_t>(double d, int i) const {
  return Generate<int16_t>(d, i, min.int16, max.int16);
}
template<> inline int32_t DataProvider::ColDesc::Generate<int32_t>(double d, int i) const {
  return Generate<int32_t>(d, i, min.int32, max.int32);
}
template<> inline int64_t DataProvider::ColDesc::Generate<int64_t>(double d, int i) const {
  return Generate<int64_t>(d, i, min.int64, max.int64);
}
template<> inline float DataProvider::ColDesc::Generate<float>(double d, int i) const {
  return Generate<float>(d, i, min.f, max.f);
}
template<> inline double DataProvider::ColDesc::Generate<double>(double d, int i) const {
  return Generate<double>(d, i, min.d, max.d);
}
