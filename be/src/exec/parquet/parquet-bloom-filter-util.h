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

#include "exprs/literal.h"
#include "exec/parquet/parquet-common.h"

namespace impala {

class ParquetBloomFilter;

/// Returns whether Impala supports the pair of types in the Parquet Bloom filter.
/// 'parquet_type' is the type that is used in the Parquet file, 'impala_type' is the
/// type of the column in Impala. Note: in a WHERE clause with a literal, where we make
/// use of the Parquet Bloom filter, both the column's impala type and the literal's
/// impala type must be supported with the given Parquet type.
bool IsParquetBloomFilterSupported(parquet::Type::type parquet_type,
    const ColumnType& impala_type);

/// Returns OK if the Bloom filter header is valid and an error otherwise.
Status ValidateBloomFilterHeader(const parquet::BloomFilterHeader& bloom_filter_header);

/// Converts '*value', having type 'impala_type', to the given Parquet type and stores its
/// address in '*ptr' and byte length in '*len'.
///
/// If there is no need to allocate new memory to store the result of the conversion
/// because the result already exists in a buffer (this is the case for example for
/// 'StringValue's), '*ptr' will point to the already existing buffer. If memory
/// allocation is needed, the vector '*storage' will be used to allocate memory and '*ptr'
/// will point into its buffer. Any previous content in the vector will be lost.
///
/// There is no guarantee in which cases the vector will be used and in which cases it
/// won't, so it should not be relied on. The vector should be kept alive and unmodified
/// as long as the data at '*ptr' is used.
///
/// If the conversion fails, an error is returned.
Status BytesToParquetType(const void* value, const ColumnType& impala_type,
    const parquet::Type::type& parquet_type, vector<uint8_t>* storage,
    uint8_t** ptr, size_t* len) WARN_UNUSED_RESULT;

/// Like BytesToParquetType, but with a Literal that needs to be evaluated.
Status LiteralToParquetType(const Literal& literal, ScalarExprEvaluator* eval,
    const parquet::Type::type& parquet_type, vector<uint8_t>* storage,
    uint8_t** ptr, size_t* len) WARN_UNUSED_RESULT;

/// Creates a 'parquet::BloomFilterHeader' object based on 'bloom_filter'.
parquet::BloomFilterHeader CreateBloomFilterHeader(
    const ParquetBloomFilter& bloom_filter);

} // namespace impala
