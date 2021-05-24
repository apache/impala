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

package org.apache.impala.catalog;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;

/**
 * Simple Request builder class. Assumes all the metadata at higher granularity is
 * required if a specific level is requested. For example, if files are requested,
 * assumes that partitions names and partitions are also requested.
 */
public class GetPartialCatalogObjectRequestBuilder {
  private boolean wantFileMetadata_;
  private boolean wantHmsPartition_;
  private boolean wantPartitionNames_;
  private String tblName_, dbName_;
  private boolean wantHmsTable_;
  private boolean wantStatsForAllColumns_;
  private long tableId = CatalogServiceCatalog.TABLE_ID_UNAVAILABLE;
  private ValidWriteIdList writeIdList_;

  /**
   * Sets the database name for the request object.
   */
  public GetPartialCatalogObjectRequestBuilder db(String db) {
    this.dbName_ = db;
    return this;
  }

  /**
   * Sets the table name for the request object.
   */
  public GetPartialCatalogObjectRequestBuilder tbl(String tbl) {
    this.tblName_ = tbl;
    return this;
  }

  /**
   * Sets the request fields required for fetching partition names,
   * HMS Partition object and the file-metadata of the partitions.
   */
  public GetPartialCatalogObjectRequestBuilder wantFiles() {
    wantFileMetadata_ = true;
    wantHmsPartition_ = true;
    wantPartitionNames_ = true;
    return this;
  }

  /**
   * Sets the request fields required for fetching partition names,
   * HMS Partition objects. No file-metadata will be fetched.
   */
  public GetPartialCatalogObjectRequestBuilder wantPartitions() {
    wantPartitionNames_ = true;
    wantHmsPartition_ = true;
    return this;
  }

  /**
   * Sets the request fields required for fetching partition names. No
   * partition metadata or file-metadata will be fetched.
   */
  public GetPartialCatalogObjectRequestBuilder wantPartitionNames() {
    wantPartitionNames_ = true;
    return this;
  }

  /**
   * Sets the request fields for fetching column statistics for all columns.
   */
  public GetPartialCatalogObjectRequestBuilder wantStatsForAllColums() {
    wantStatsForAllColumns_ = true;
    return this;
  }

  GetPartialCatalogObjectRequestBuilder tableId(long id) {
    this.tableId = id;
    return this;
  }

  GetPartialCatalogObjectRequestBuilder writeId(String writeIdList) {
    Preconditions.checkNotNull(writeIdList);
    writeIdList_ = new ValidReaderWriteIdList();
    writeIdList_.readFromString(writeIdList);
    return this;
  }

  GetPartialCatalogObjectRequestBuilder writeId(
      ValidWriteIdList validWriteIdList) {
    writeIdList_ = Preconditions.checkNotNull(validWriteIdList);
    return this;
  }

  /**
   * Builds the {@link TGetPartialCatalogObjectRequest} object based on the fields
   * set in this Builder.
   */
  public TGetPartialCatalogObjectRequest build() {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable(dbName_, tblName_);
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.table_id = tableId;
    if (writeIdList_ != null) {
      req.table_info_selector.valid_write_ids = MetastoreShim
          .convertToTValidWriteIdList(writeIdList_);
    }
    if (wantPartitionNames_) {
      req.table_info_selector.want_partition_names = true;
    }
    if (wantHmsPartition_) {
      req.table_info_selector.want_hms_partition = true;
    }
    if (wantFileMetadata_) {
      req.table_info_selector.want_partition_files = true;
    }
    if (wantStatsForAllColumns_) {
      req.table_info_selector.want_stats_for_all_columns = true;
    }
    return req;
  }
}
