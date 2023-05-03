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

#include "common/global-types.h"

#include <cstdint>
#include <map>
#include <memory>

namespace org::apache::impala::fb {
  struct FbIcebergMetadata;
}

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
  FileMetadataUtils(HdfsScanNodeBase* scan_node) : scan_node_(scan_node) {}

  /// Initialize the RuntimeState and HdfsFileDescriptor, this method can be called
  /// multiple times during the object's lifecycle.
  void SetFile(RuntimeState* state, const HdfsFileDesc* file_desc);

  /// Returns the template tuple corresponding to the partition_id and file_desc_.
  /// I.e. it sets partition columns and default values in the template tuple.
  /// Updates the slot_descs_written map with the SlotDescriptors that were written into
  /// the returned tuple.
  Tuple* CreateTemplateTuple(int64_t partition_id, MemPool* mem_pool,
      std::map<const SlotId, const SlotDescriptor*>* slot_descs_written);

  /// Returns true if 'slot_desc' refers to a value-based partition column. Returns false
  /// for transform-based partition columns and non-partition colusmns.
  bool IsValuePartitionCol(const SlotDescriptor* slot_desc);

  /// Returns true if the file should contain the column described by 'slot_desc'.
  /// Returns false when the data can be retrieved from other sources, e.g. value-based
  /// partition columns, virtual columns.
  bool NeedDataInFile(const SlotDescriptor* slot_desc);

private:
  void AddFileLevelVirtualColumns(MemPool* mem_pool, Tuple* template_tuple);

  /// Writes the Iceberg columns from FbIcebergMetadata into template_tuple. Updates the
  /// slot_descs_written map with the SlotDescriptors that were written into the
  /// template_tuple.
  void AddIcebergColumns(MemPool* mem_pool, Tuple** template_tuple,
      std::map<const SlotId, const SlotDescriptor*>* slot_descs_written);

  /// Writes Iceberg-related virtual column values to the template tuple.
  void AddVirtualIcebergColumn(MemPool* mem_pool, Tuple* template_tuple,
      const org::apache::impala::fb::FbIcebergMetadata& ice_metadata,
      const SlotDescriptor* slot_desc);

  HdfsScanNodeBase* const scan_node_;

  // Members below are set in Open()
  RuntimeState* state_ = nullptr;
  const HdfsFileDesc* file_desc_ = nullptr;
};

} // namespace impala
