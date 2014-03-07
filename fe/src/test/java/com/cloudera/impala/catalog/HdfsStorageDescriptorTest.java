// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.Test;

import com.cloudera.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import com.google.common.collect.ImmutableList;

public class HdfsStorageDescriptorTest {
  @Test
  public void delimitersInCorrectOrder() {
    final List<String> DELIMITER_KEYS =
      ImmutableList.of(serdeConstants.LINE_DELIM, serdeConstants.FIELD_DELIM,
        serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
        serdeConstants.ESCAPE_CHAR, serdeConstants.QUOTE_CHAR);

    assertEquals(DELIMITER_KEYS, HdfsStorageDescriptor.DELIMITER_KEYS);
  }

  /**
   * Tests that Impala is able to create an HdfsStorageDescriptor using all
   * combinations of Parquet SerDe class name + input/output format class name.
   */
  @Test
  public void testParquetFileFormat() throws DatabaseNotFoundException,
      InvalidStorageDescriptorException {
    String[] parquetSerDe = new String[] {
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        "parquet.hive.serde.ParquetHiveSerDe"};
    String [] inputFormats = new String [] {
        "com.cloudera.impala.hive.serde.ParquetInputFormat",
        "parquet.hive.DeprecatedParquetInputFormat",
        "parquet.hive.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"};
    String [] outputFormats = new String [] {
        "com.cloudera.impala.hive.serde.ParquetOutputFormat",
        "parquet.hive.DeprecatedParquetOutputFormat",
        "parquet.hive.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"};

    for (String serDe: parquetSerDe) {
      SerDeInfo serDeInfo = new SerDeInfo();
      serDeInfo.setSerializationLib(serDe);
      serDeInfo.setParameters(new HashMap<String, String>());
      for (String inputFormat: inputFormats) {
        for (String outputFormat: outputFormats) {
          StorageDescriptor sd = new StorageDescriptor();
          sd.setSerdeInfo(serDeInfo);
          sd.setInputFormat(inputFormat);
          sd.setOutputFormat(outputFormat);
          assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTblName", sd));
        }
      }
    }
  }
}
