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

package org.apache.impala.util;

import com.google.common.base.Preconditions;

import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterator;

/**
 * The metadata table scanner class to scan Iceberg metadata tables through the Iceberg
 * API. This object is instantiated and governed by {IcebergMetadataScanNode} at the
 * backend during scanning.
 *
 * Iceberg generally throws RuntimeExceptions, these have to be taken care of by the
 * caller of {@code IcebergMetadataScanner}.
 */
public class IcebergMetadataScanner {
  // FeTable object is extracted by the backend and passed when this object is created
  private FeIcebergTable iceTbl_ = null;

  // Metadata table
  private Table metadataTable_ = null;

  // Name of the metadata table
  private String metadataTableName_;

  // Persist the file scan task iterator so we can continue after a RowBatch is full
  private CloseableIterator<FileScanTask> fileScanTaskIterator_;

  // Persist the data rows iterator, so we can continue after a batch is filled
  private CloseableIterator<StructLike> dataRowsIterator_;

  public IcebergMetadataScanner(FeIcebergTable iceTbl, String metadataTableName) {
    Preconditions.checkNotNull(iceTbl);
    this.iceTbl_ = (FeIcebergTable) iceTbl;
    this.metadataTableName_ = metadataTableName;
  }

  /**
   * Iterates over the {{fileScanTaskIterator_}} to find a {FileScanTask} that has rows.
   */
  public boolean FindFileScanTaskWithRows() {
    while (fileScanTaskIterator_.hasNext()) {
      DataTask dataTask = (DataTask)fileScanTaskIterator_.next();
      dataRowsIterator_ = dataTask.rows().iterator();
      if (dataRowsIterator_.hasNext()) return true;
    }
    return false;
  }

  /**
   * Creates the Metadata{Table} which is a predifined Iceberg {Table} object. This method
   * also starts an Iceberg {TableScan} to scan the {Table}. After the scan is ready it
   * initializes the iterators, so the {GetNext} call can start fetching the rows through
   * the Iceberg Api.
   */
  public void ScanMetadataTable() {
    // Create and scan the metadata table
    metadataTable_ = MetadataTableUtils.createMetadataTableInstance(
        iceTbl_.getIcebergApiTable(), MetadataTableType.valueOf(metadataTableName_));
    TableScan scan = metadataTable_.newScan();
    // Init the FileScanTask iterator and DataRowsIterator
    fileScanTaskIterator_ = scan.planFiles().iterator();
    FindFileScanTaskWithRows();
  }

  /**
   * Returns the field {Accessor} for the specified column position. This {Accessor} is
   * used to access a field in the {StructLike} object.
   */
  public Accessor GetAccessor(int slotColPos) {
    int fieldId = metadataTable_.schema().columns().get(slotColPos).fieldId();
    return metadataTable_.schema().accessorForField(fieldId);
  }

  /**
   * Returns the next available row of the scan result. The row is a {StructLike} object
   * and its fields can be accessed with {Accessor}s.
   */
  public StructLike GetNext() {
    // Return the next row in the DataRows iterator
    if (dataRowsIterator_.hasNext()) {
      return dataRowsIterator_.next();
    }
    // Otherwise this DataTask is empty, find a FileScanTask that has a non-empty DataTask
    if(FindFileScanTaskWithRows()) {
      return dataRowsIterator_.next();
    }
    return null;
  }
}
