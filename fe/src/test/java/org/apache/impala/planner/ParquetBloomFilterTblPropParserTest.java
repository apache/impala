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

package org.apache.impala.planner;

import org.apache.log4j.Logger;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

public class ParquetBloomFilterTblPropParserTest {
  private static final Logger LOG = Logger.getLogger(
      ParquetBloomFilterTblPropParserTest.class);

  @Test
  public void testParsingOnlyColNames() throws Exception {
    final String props = "col1,col2,col3";
    final Map<String, Long> exp = ImmutableMap.of(
      "col1", HdfsTableSink.PARQUET_BLOOM_FILTER_MAX_BYTES,
      "col2", HdfsTableSink.PARQUET_BLOOM_FILTER_MAX_BYTES,
      "col3", HdfsTableSink.PARQUET_BLOOM_FILTER_MAX_BYTES
      );

    parseAndCheck(props, exp);
  }

  @Test
  public void testParsingAllSizesGiven() {
    final String props = "col1:128,col2:256,col3:64";
    final Map<String, Long> exp = ImmutableMap.of(
      "col1", 128l,
      "col2", 256l,
      "col3", 64l
      );

    parseAndCheck(props, exp);
  }

  @Test
  public void testParsingSomeSizesGiven() {
    final String props = "col1:128,col2,col3:64";
    final Map<String, Long> exp = ImmutableMap.of(
      "col1", 128l,
      "col2", HdfsTableSink.PARQUET_BLOOM_FILTER_MAX_BYTES,
      "col3", 64l
    );

    parseAndCheck(props, exp);
  }

  @Test
  public void testParsingContainsWhitespace() {
    final String props = "col1 : 128, col2, \ncol3: 64 \t";
    final Map<String, Long> exp = ImmutableMap.of(
      "col1", 128l,
      "col2", HdfsTableSink.PARQUET_BLOOM_FILTER_MAX_BYTES,
      "col3", 64l
    );

    parseAndCheck(props, exp);
  }

  private void parseAndCheck(final String tbl_props, final Map<String, Long> exp_res) {
    final Map<String, Long> res = HdfsTableSink.parseParquetBloomFilterWritingTblProp(
        tbl_props);
    assertEquals(exp_res, res);
  }
}
