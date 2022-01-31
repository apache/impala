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

namespace impala {

struct HdfsFileDesc;
class HdfsScanNodeBase;
class MemPool;
class RuntimeState;
class ScannerContext;
class SlotDescriptor;
class Tuple;
class TupleDescriptor;

/// Helper class for scanners dealing with different table/file formats.
class FileMetadataUtils {
public:
  FileMetadataUtils(HdfsScanNodeBase* scan_node, RuntimeState* state) :
      scan_node_(scan_node), state_(state) {}

  void Open(ScannerContext* context);

  /// Returns the template tuple corresponding to this scanner context. I.e. it sets
  /// partition columns and default values in the template tuple.
  Tuple* CreateTemplateTuple(MemPool* mem_pool);

  /// Returns true if 'slot_desc' refers to a value-based partition column. Returns false
  /// for transform-based partition columns and non-partition columns.
  bool IsValuePartitionCol(const SlotDescriptor* slot_desc);

private:
  HdfsScanNodeBase* scan_node_;
  RuntimeState* state_;

  // Members below are set in Open()
  const ScannerContext* context_ = nullptr;
  const HdfsFileDesc* file_desc_ = nullptr;
};

} // namespace impala
