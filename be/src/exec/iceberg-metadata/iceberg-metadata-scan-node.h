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

#include "exec/iceberg-metadata/iceberg-row-reader.h"
#include "exec/iceberg-metadata/iceberg-metadata-scanner.h"
#include "exec/scan-node.h"

#include <jni.h>

namespace impala {

class ExecNode;
class IcebergRowReader;
class RuntimeState;
class Status;

/// Scan node for an Iceberg metadata table.
/// Iceberg API provides predefined metadata tables, these tables can be scanned through
/// the Iceberg API just like any other regular Iceberg tables. Although, Impala utilizes
/// its Parquet scanner to scan Iceberg data, due to the virtual nature of the metadata
/// tables these should be scanned with the Iceberg API.
///
/// For scanning these metadata tables this scanner calls into the JVM and creates an
/// 'IcebergMetadataScanner' object that does the scanning. Once the Iceberg scan is done,
/// the scan node starts fetching the rows one by one and materializes the Iceberg rows
/// into Impala rowbatches.
///
/// The flow of scanning is:
/// 1. Backend:  gets the FeIcebergTable object from the frontend
/// 2. Backend:  creates an IcebergMetadataScanner object on the Java heap
/// 3. Backend:  triggers a metadata table creation and scan on the Frontend
/// 4. Frontend: creates the metadata table and executes the scan
/// 5. Backend:  calls GetNext that calls the IcebergMetadataScanner's GetNext
/// 6. Frontend: IcebergMetadataScanner's GetNext iterates over the result set and returns
///              a row in StructLike format
/// 7. Backend:  converts and materializes the returned StructLike object into RowBatch
///
/// Note:
///   This scan node should be executed on the coordinator, because it depends on the
///   frontend's table cache.
class IcebergMetadataScanPlanNode : public ScanPlanNode {
 public:
  Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;
  ~IcebergMetadataScanPlanNode() {}
};

class IcebergMetadataScanNode : public ScanNode {
 public:
  IcebergMetadataScanNode(ObjectPool* pool, const IcebergMetadataScanPlanNode& pnode,
      const DescriptorTbl& descs);

  /// Initializes counters, executes Iceberg table scan and initializes accessors.
  Status Prepare(RuntimeState* state) override;

  /// Creates the Iceberg row reader.
  Status Open(RuntimeState* state) override;

  /// Fills the next rowbatch with the results returned by the Iceberg scan.
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

  /// Finalize and close this operator.
  void Close(RuntimeState* state) override;

 private:
  /// Adapter that helps preparing the metadata table and executes an Iceberg table scan
  /// on Java side. Allows the ScanNode to fetch the metadata from the Java Heap.
  std::unique_ptr<IcebergMetadataScanner> metadata_scanner_;

  /// Helper class to transform Iceberg rows to Impala tuples.
  std::unique_ptr<IcebergRowReader> iceberg_row_reader_;

  // The TupleId and TupleDescriptor of the tuple that this scan node will populate.
  const TupleId tuple_id_;
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// Table and metadata table names.
  const TTableName table_name_;
  const string metadata_table_name_;

  /// Iceberg metadata scan specific counters.
  RuntimeProfile::Counter* scan_prepare_timer_;
  RuntimeProfile::Counter* iceberg_api_scan_timer_;

  /// Gets the FeIceberg table from the Frontend.
  Status GetCatalogTable(jobject* jtable);
};

}
