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

#include "util/bit-packing.inline.h"

#include "runtime/date-value.h"
#include "runtime/decimal-value.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

namespace impala {

// Instantiate all of the templated functions needed by the rest of Impala.
#define INSTANTIATE_UNPACK_VALUES(OUT_TYPE)                                       \
  template std::pair<const uint8_t*, int64_t> BitPacking::UnpackValues<OUT_TYPE>( \
      int bit_width, const uint8_t* __restrict__ in, int64_t in_bytes,            \
      int64_t num_values, OUT_TYPE* __restrict__ out);

INSTANTIATE_UNPACK_VALUES(uint8_t);
INSTANTIATE_UNPACK_VALUES(uint16_t);
INSTANTIATE_UNPACK_VALUES(uint32_t);
INSTANTIATE_UNPACK_VALUES(uint64_t);

#define INSTANTIATE_UNPACK_AND_DECODE(OUT_TYPE)                                         \
  template std::pair<const uint8_t*, int64_t>                                           \
  BitPacking::UnpackAndDecodeValues<OUT_TYPE>(int bit_width,                            \
      const uint8_t* __restrict__ in, int64_t in_bytes, OUT_TYPE* __restrict__ dict,    \
      int64_t dict_len, int64_t num_values, OUT_TYPE* __restrict__ out, int64_t stride, \
      bool* __restrict__ decode_error);

INSTANTIATE_UNPACK_AND_DECODE(bool);
INSTANTIATE_UNPACK_AND_DECODE(double);
INSTANTIATE_UNPACK_AND_DECODE(float);
INSTANTIATE_UNPACK_AND_DECODE(int8_t);
INSTANTIATE_UNPACK_AND_DECODE(int16_t);
INSTANTIATE_UNPACK_AND_DECODE(int32_t);
INSTANTIATE_UNPACK_AND_DECODE(int64_t);
INSTANTIATE_UNPACK_AND_DECODE(uint8_t);
INSTANTIATE_UNPACK_AND_DECODE(uint16_t);
INSTANTIATE_UNPACK_AND_DECODE(uint32_t);
INSTANTIATE_UNPACK_AND_DECODE(uint64_t);
INSTANTIATE_UNPACK_AND_DECODE(Decimal4Value);
INSTANTIATE_UNPACK_AND_DECODE(Decimal8Value);
INSTANTIATE_UNPACK_AND_DECODE(Decimal16Value);
INSTANTIATE_UNPACK_AND_DECODE(StringValue);
INSTANTIATE_UNPACK_AND_DECODE(TimestampValue);
INSTANTIATE_UNPACK_AND_DECODE(DateValue);

#define INSTANTIATE_UNPACK_AND_DELTA_DECODE(OUT_TYPE, PARQUET_TYPE)               \
  template std::pair<const uint8_t*, int64_t>                                     \
  BitPacking::UnpackAndDeltaDecodeValues<OUT_TYPE>(int bit_width,                 \
      const uint8_t* __restrict__ in, int64_t in_bytes, PARQUET_TYPE* base_value, \
      PARQUET_TYPE delta_offset, int64_t num_values, OUT_TYPE* __restrict__ out,  \
      int64_t stride, bool* __restrict__ decode_error);

INSTANTIATE_UNPACK_AND_DELTA_DECODE(int8_t, int32_t);
INSTANTIATE_UNPACK_AND_DELTA_DECODE(int16_t, int32_t);
INSTANTIATE_UNPACK_AND_DELTA_DECODE(int32_t, int32_t);
INSTANTIATE_UNPACK_AND_DELTA_DECODE(int64_t, int32_t);
INSTANTIATE_UNPACK_AND_DELTA_DECODE(int64_t, int64_t);

// Required for bit-packing-benchmark.cc.
template
const uint8_t* BitPacking::Unpack32Values(int bit_width, const uint8_t* __restrict__ in,
    int64_t in_bytes, uint32_t* __restrict__ out);
}
