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

#include "experiments/data-provider.h"

#include <algorithm>
#include <stdlib.h>
#include <math.h>
#include <iostream>

#include "util/runtime-profile-counters.h"

#include "common/names.h"

using boost::minstd_rand;
using boost::uniform_real;
using boost::variate_generator;
using namespace impala;
using std::min;

DataProvider::DataProvider(MemPool* pool, RuntimeProfile* profile) :
  pool_(pool),
  profile_(profile),
  num_rows_(0),
  batch_size_(0),
  rows_returned_(0),
  data_(NULL),
  row_size_(0) {
  SetSeed(0);

  bytes_generated_ = ADD_COUNTER(profile, "BytesGenerated", TUnit::BYTES);
}

void DataProvider::Reset(int num_rows, int batch_size, const vector<DataProvider::ColDesc>& cols) {
  num_rows_ = num_rows;
  batch_size_ = batch_size;
  rows_returned_ = 0;
  row_size_ = 0;
  cols_ = cols;
  for (int i = 0; i < cols_.size(); ++i) {
    row_size_ += cols[i].bytes;
  }
  data_.reset(new char[row_size_ * batch_size_]);
  COUNTER_SET(bytes_generated_, 0);
}

void DataProvider::SetSeed(int seed) {
  rand_generator_.seed(seed);
}

void RandString(MemPool* pool, StringValue* result,
    const StringValue& min, const StringValue& max, double r,
    variate_generator<minstd_rand&, uniform_real<>>& rand) {
  int min_len = min.Len();
  int max_len = max.Len();
  int len = r * (max_len - min_len) + min_len;
  char* ptr = reinterpret_cast<char*>(pool->Allocate(len));
  result->Assign(ptr, len);

  for (int i = 0; i < len; ++i) {
    int min_char = i < min_len ? min.Ptr()[i] : 'a';
    int max_char = (i < max_len ? max.Ptr()[i] : 'z') + 1;
    ptr[i] = rand() * (max_char - min_char) + min_char;
  }
}

void* DataProvider::NextBatch(int* rows_returned) {
  int num_rows = min(batch_size_, num_rows_ - rows_returned_);
  *rows_returned = num_rows;
  if (num_rows == 0) return NULL;
  COUNTER_ADD(bytes_generated_, num_rows * row_size_);

  uniform_real<> dist(0,1);
  variate_generator<minstd_rand&, uniform_real<>> rand_double(rand_generator_, dist);

  char* data = data_.get();
  for (int i = 0, row_idx = rows_returned_; i < num_rows; ++i, ++row_idx) {
    for (int j = 0; j < cols_.size(); ++j) {
      double r = rand_double();
      const ColDesc& col = cols_[j];
      switch (col.type) {
        case TYPE_BOOLEAN:
          *reinterpret_cast<bool*>(data) = col.Generate<bool>(r, row_idx);
          break;
        case TYPE_TINYINT:
          *reinterpret_cast<int8_t*>(data) = col.Generate<int8_t>(r, row_idx);
          break;
        case TYPE_SMALLINT:
          *reinterpret_cast<int16_t*>(data) = col.Generate<int16_t>(r, row_idx);
          break;
        case TYPE_INT:
          *reinterpret_cast<int32_t*>(data) = col.Generate<int32_t>(r, row_idx);
          break;
        case TYPE_BIGINT:
          *reinterpret_cast<int64_t*>(data) = col.Generate<int64_t>(r, row_idx);
          break;
        case TYPE_FLOAT:
          *reinterpret_cast<float*>(data) = col.Generate<float>(r, row_idx);
          break;
        case TYPE_DOUBLE:
          *reinterpret_cast<double*>(data) = col.Generate<double>(r, row_idx);
          break;
        case TYPE_VARCHAR:
        case TYPE_STRING: {
          // TODO: generate sequential strings
          StringValue* str = reinterpret_cast<StringValue*>(data);
          RandString(pool_, str, col.min.s, col.max.s, r, rand_double);
          break;
        }
        default:
          break;
      }
      data += col.bytes;
    }
  }
  rows_returned_ += num_rows;
  return reinterpret_cast<void*>(data_.get());
}

void DataProvider::Print(ostream* stream, char* data, int rows) const {
  char* next_col = reinterpret_cast<char*>(data);
  for (int i = 0; i < rows; ++i) {
    for (int j = 0; j < cols_.size(); ++j) {
      switch (cols_[j].type) {
        case TYPE_BOOLEAN:
          *stream << (*reinterpret_cast<int8_t*>(next_col) ? "true" : "false");
          break;
        case TYPE_TINYINT:
          *stream << (int)*reinterpret_cast<int8_t*>(next_col);
          break;
        case TYPE_SMALLINT:
          *stream << *reinterpret_cast<int16_t*>(next_col);
          break;
        case TYPE_INT:
          *stream << *reinterpret_cast<int32_t*>(next_col);
          break;
        case TYPE_BIGINT:
          *stream << *reinterpret_cast<int64_t*>(next_col);
          break;
        case TYPE_FLOAT:
          *stream << *reinterpret_cast<float*>(next_col);
          break;
        case TYPE_DOUBLE:
          *stream << *reinterpret_cast<double*>(next_col);
          break;
        case TYPE_STRING:
        case TYPE_VARCHAR:
          *stream << *reinterpret_cast<StringValue*>(next_col);
          break;
        default:
          *stream << "BAD" << endl;
          return;
      }
      if (j != cols_.size() - 1) *stream << ", ";
      next_col += cols_[j].bytes;
    }
    *stream << endl;
  }
}
