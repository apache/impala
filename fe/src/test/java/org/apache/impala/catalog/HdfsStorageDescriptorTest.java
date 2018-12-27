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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.catalog.HdfsStorageDescriptor.InvalidStorageDescriptorException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.THdfsFileFormat;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class HdfsStorageDescriptorTest {
  @BeforeClass
  public static void setup() {
    FeSupport.loadLibrary();
  }

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

  /**
   * Verifies Impala is able to properly parse delimiters in supported formats.
   * See HdfsStorageDescriptor.parseDelim() for details.
   */
  @Test
  public void testDelimiters() throws InvalidStorageDescriptorException {
    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(THdfsFileFormat.TEXT,
        RowFormat.DEFAULT_ROW_FORMAT);
    sd.setParameters(new HashMap<>());
    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "-2");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "-128");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "127");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.LINE_DELIM, "\001");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "|");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "\t");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "ab");
    try {
      HdfsStorageDescriptor.fromStorageDescriptor("fake", sd);
      fail();
    } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
      assertEquals("Invalid delimiter: 'ab'. Delimiter must be specified as a " +
          "single character or as a decimal value in the range [-128:127]",
          e.getMessage());
    }

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "128");
    try {
      HdfsStorageDescriptor.fromStorageDescriptor("fake", sd);
      fail();
    } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
      assertEquals("Invalid delimiter: '128'. Delimiter must be specified as a " +
          "single character or as a decimal value in the range [-128:127]",
          e.getMessage());
    }

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "\128");
    try {
      HdfsStorageDescriptor.fromStorageDescriptor("fake", sd);
      fail();
    } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
      assertEquals("Invalid delimiter: '\128'. Delimiter must be specified as a " +
          "single character or as a decimal value in the range [-128:127]",
          e.getMessage());
    }

    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.LINE_DELIM, "-129");
    try {
      HdfsStorageDescriptor.fromStorageDescriptor("fake", sd);
      fail();
    } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
      assertEquals("Invalid delimiter: '-129'. Delimiter must be specified as a " +
          "single character or as a decimal value in the range [-128:127]",
          e.getMessage());
    }

    // Test that a unicode character out of the valid range will not be accepted.
    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.LINE_DELIM, "\u1111");
    try {
      HdfsStorageDescriptor.fromStorageDescriptor("fake", sd);
      fail();
    } catch (HdfsStorageDescriptor.InvalidStorageDescriptorException e) {
      assertEquals("Invalid delimiter: '\u1111'. Delimiter must be specified as a " +
          "single character or as a decimal value in the range [-128:127]",
          e.getMessage());
    }

    // Validate that unicode character in the valid range will be accepted.
    sd.getSerdeInfo().setParameters(new HashMap<>());
    sd.getSerdeInfo().putToParameters(serdeConstants.FIELD_DELIM, "\u0001");
    assertNotNull(HdfsStorageDescriptor.fromStorageDescriptor("fakeTbl", sd));

  }
}
