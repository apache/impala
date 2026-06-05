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

#include "exec/parquet/parquet-struct-column-reader.h"

namespace impala {

/// Reads unshredded variant columns from Parquet files. An unshredded variant is stored
/// as a Parquet group with two BINARY children: "metadata" (field name dictionary) and
/// "value" (the encoded variant value).
///
/// This reader inherits from StructColumnReader since the physical Parquet layout is
/// identical to a struct with two binary fields. The only difference is the type check
/// and the slot size (24 bytes = two StringValues).
///
/// For shredded variants (future), this reader will be extended to handle additional
/// typed children beyond metadata+value, enabling projection pushdown.
class VariantColumnReader : public StructColumnReader {
 public:
  VariantColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc) : StructColumnReader(parent, node, slot_desc) {
    if (slot_desc != nullptr) DCHECK(slot_desc->type().IsVariantType());
  }

  virtual ~VariantColumnReader() {}

  virtual bool IsStructReader() const override { return false; }

  // A variant is physically a struct (StructColumnReader) and inherits its lack of
  // SkipRows() support (StructColumnReader::SkipRows() is a DCHECK(false) stub because
  // structs are excluded from late materialization). The scanner's late-materialization
  // guard skips any reader subtree for which HasStructReader() is true, so we must report
  // true here to be excluded as well; otherwise SkipRows() would be called on this reader
  // and crash. IsStructReader() stays false because a variant is not a user-visible
  // struct. TODO: implement SkipRows() to allow late materialization for variants.
  virtual bool HasStructReader() const override { return true; }
};

} // namespace impala
