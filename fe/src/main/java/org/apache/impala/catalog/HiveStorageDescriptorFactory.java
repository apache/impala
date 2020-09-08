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

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.impala.thrift.THdfsFileFormat;

import com.google.common.base.Preconditions;

public class HiveStorageDescriptorFactory {
  /**
   * Creates and returns a Hive StoreDescriptor for the given FileFormat and RowFormat.
   * Currently supports creating StorageDescriptors for Parquet, Text, Sequence, Avro and
   * RC file.
   * TODO: Add support for HBase
   */
  public static StorageDescriptor createSd(THdfsFileFormat fileFormat,
      RowFormat rowFormat) {
    Preconditions.checkNotNull(fileFormat);
    Preconditions.checkNotNull(rowFormat);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setSerdeInfo(new org.apache.hadoop.hive.metastore.api.SerDeInfo());
    sd.getSerdeInfo().setParameters(new HashMap<>());
    // The compressed flag is not used to determine whether the table is compressed or
    // not. Instead, we use the input format or the filename.
    sd.setCompressed(false);
    HdfsFileFormat hdfsFileFormat = HdfsFileFormat.fromThrift(fileFormat);
    sd.setInputFormat(hdfsFileFormat.inputFormat());
    sd.setOutputFormat(hdfsFileFormat.outputFormat());
    sd.getSerdeInfo().setSerializationLib(hdfsFileFormat.serializationLib());
    sd.setBucketCols(new ArrayList<>(0));
    sd.setSortCols(new ArrayList<>(0));
    setSerdeInfo(rowFormat, sd.getSerdeInfo());
    return sd;
  }

  /**
   * Updates the serde info with the specified RowFormat. This method is used when
   * just updating the row format and not the entire storage descriptor.
   */
  public static void setSerdeInfo(RowFormat rowFormat, SerDeInfo serdeInfo) {
    if (rowFormat.getFieldDelimiter() != null) {
      serdeInfo.putToParameters(
          "serialization.format", rowFormat.getFieldDelimiter());
      serdeInfo.putToParameters("field.delim", rowFormat.getFieldDelimiter());
    }
    if (rowFormat.getEscapeChar() != null) {
      serdeInfo.putToParameters("escape.delim", rowFormat.getEscapeChar());
    }
    if (rowFormat.getLineDelimiter() != null) {
      serdeInfo.putToParameters("line.delim", rowFormat.getLineDelimiter());
    }

  }
}
