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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.compat.MetastoreShim;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AcidUtilsTest {
  private static final Logger LOG = LoggerFactory.getLogger(AcidUtilsTest.class);
  /** Fake base path to root all FileStatuses under. */
  private static final Path BASE_PATH = new Path("file:///foo/bar/");

  public AcidUtilsTest() {
    Assume.assumeTrue("Tests require Hive 3 to parse and use WriteIdList",
        MetastoreShim.getMajorVersion() == 3);
  }

  private static List<FileStatus> createMockStats(String... testStrings) {
    return Arrays.asList(testStrings).stream().map(s -> {
      return new FileStatus(
          /* length=*/0, /*isdir=*/s.endsWith("/"), /*block_replication=*/1,
          /*blocksize=*/0, /*mtime=*/1, /*path=*/new Path(BASE_PATH, s));
    }).collect(Collectors.toList());
  }

  private void assertFiltering(String[] relPaths, String validWriteIdListStr,
      String[] expectedRelPaths) {
    assertFiltering(relPaths, "", validWriteIdListStr, expectedRelPaths);
  }

  private void assertFiltering(String[] relPaths, String validTxnListStr,
      String validWriteIdListStr, String[] expectedRelPaths) {
    ValidWriteIdList writeIds = MetastoreShim.getValidWriteIdListFromString(
        validWriteIdListStr);
    List<FileStatus> stats = createMockStats(relPaths);
    List<FileStatus> expectedStats = createMockStats(expectedRelPaths);

    try {
      assertThat(AcidUtils.filterFilesForAcidState(stats, BASE_PATH,
          new ValidReadTxnList(validTxnListStr), writeIds, null),
          Matchers.containsInAnyOrder(expectedStats.toArray()));
    } catch (CatalogException me) {
      //TODO: Remove try-catch once IMPALA-9042 is resolved.
      assertTrue(false);
    }
  }

  public void filteringError(String[] relPaths, String validTxnListStr,
      String validWriteIdListStr, String expectedErrorString) {
    Preconditions.checkNotNull(expectedErrorString, "No expected error message given.");
    try {
      ValidWriteIdList writeIds = MetastoreShim.getValidWriteIdListFromString(
        validWriteIdListStr);
      List<FileStatus> stats = createMockStats(relPaths);
      AcidUtils.filterFilesForAcidState(
          stats, BASE_PATH, new ValidReadTxnList(validTxnListStr), writeIds, null);
    } catch (Exception e) {
      String errorString = e.getMessage();
      Preconditions.checkNotNull(errorString, "Stack trace lost during exception.");
      String msg = "got error:\n" + errorString + "\nexpected:\n" + expectedErrorString;
      assertTrue(msg, errorString.startsWith(expectedErrorString));
      return;
    }
    fail("Filtering didn't result in error");
  }

  @Test
  public void testParsingBaseWriteIds() {
    assertEquals(AcidUtils.SENTINEL_BASE_WRITE_ID,
        AcidUtils.getBaseWriteId("base_01.txt"));
    assertEquals(AcidUtils.SENTINEL_BASE_WRITE_ID,
        AcidUtils.getBaseWriteId("base_01_02"));
    assertEquals(AcidUtils.SENTINEL_BASE_WRITE_ID,
        AcidUtils.getBaseWriteId("abc/base_6"));
    assertEquals(2, AcidUtils.getBaseWriteId("base_00002"));
    assertEquals(2, AcidUtils.getBaseWriteId("base_00002/foo"));
    assertEquals(2, AcidUtils.getBaseWriteId("base_00002_v12345"));
    assertEquals(2, AcidUtils.getBaseWriteId("base_00002_v12345/foo"));
  }

  @Test
  public void testBasePredicate() {
    assertFiltering(new String[]{
          "base_01.txt",
          "post_upgrade.txt",
          "base_0000005/",
          "base_0000005/abc.txt",
          "base_0000005/0000/",
          "base_0000005/0000/abc.txt",
          "_tmp.base_0000005/000000_0.manifest",
          "abc/",
          "abc/base_0000006/", // Not at root, so shouldn't be handled.
          "base_00000100/def.txt"},
      // <tableName>:<highWatermark>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:1,2,3",
        new String[]{
          "base_0000005/abc.txt",
          "base_0000005/0000/abc.txt"});
  }

  @Test
  public void testOpenTransactions() {
    assertFiltering(new String[]{
          "base_01.txt",
          "post_upgrade.txt",
          "base_0000005/",
          "base_0000005/abc.txt",
          "base_0000005/0000/",
          "base_0000005/0000/abc.txt",
          "_tmp.base_0000005/000000_0.manifest",
          "delta_0000006_0000006_0000/",
          "delta_0000006_0000006_0000/000000_0",
          "delta_0000007_0000007_0000/",
          "delta_0000007_0000007_0000/000000_0",
          "delta_0000008_0000008_0000/",
          "delta_0000008_0000008_0000/000000_0",
          "delta_0000009_0000009_0000/",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"},
        "default.test:10:6:6,7,8:", // 6,7,8 are open write ids
        new String[]{
          "base_0000005/abc.txt",
          "base_0000005/0000/abc.txt",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"});
  }

  @Test
  public void testInProgressCompaction() {
    assertFiltering(new String[]{
          "base_01.txt",
          "post_upgrade.txt",
          "base_0000005_v900/",
          "base_0000005_v900/abc.txt",
          "base_0000005_v900/0000/",
          "base_0000005_v900/0000/abc.txt",
          "base_0000008_v1000/",
          "base_0000008_v1000/abc.txt",
          "delta_0000006_0000006_0000/",
          "delta_0000006_0000006_0000/000000_0",
          "delta_0000007_0000007_0000/",
          "delta_0000007_0000007_0000/000000_0",
          "delta_0000008_0000008_0000/",
          "delta_0000008_0000008_0000/000000_0",
          "delta_0000009_0000009_0000/",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"},
        "1100:1000:1000:", // txn 1000 is open
        "default.test:10:10::", // write ids are committed
        new String[]{
          "base_0000005_v900/abc.txt",
          "base_0000005_v900/0000/abc.txt",
          "delta_0000006_0000006_0000/000000_0",
          "delta_0000007_0000007_0000/000000_0",
          "delta_0000008_0000008_0000/000000_0",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"});
  }

  public void testStreamingIngest() {
    assertFiltering(new String[]{
      "delta_0000001_0000005/bucket_0000",
      "delta_0000001_0000005/bucket_0001",
      "delta_0000001_0000005/bucket_0002",
      "delta_0000001_0000005_v01000/bucket_0000",
      "delta_0000001_0000005_v01000/bucket_0001",
      "delta_0000001_0000005_v01000/bucket_0002"},
    "1100:1000:1000:", // txn 1000 is open
    "default.test:10:10::", // write ids are committed
    new String[]{
      "delta_0000001_0000005/bucket_0000",
      "delta_0000001_0000005/bucket_0001",
      "delta_0000001_0000005/bucket_0002"});

    assertFiltering(new String[]{
      "delta_0000001_0000005/bucket_0000",
      "delta_0000001_0000005/bucket_0001",
      "delta_0000001_0000005/bucket_0002",
      "delta_0000001_0000005_v01000/bucket_0000",
      "delta_0000001_0000005_v01000/bucket_0001",
      "delta_0000001_0000005_v01000/bucket_0002"},
    "1100:::", // txn 1000 is committed
    "default.test:10:10::", // write ids are committed
    new String[]{
      "delta_0000001_0000005_v01000/bucket_0000",
      "delta_0000001_0000005_v01000/bucket_0001",
      "delta_0000001_0000005_v01000/bucket_0002"});
  }

  @Test
  public void testAbortedCompaction() {
    assertFiltering(new String[]{
          "base_01.txt",
          "post_upgrade.txt",
          "base_0000005_v900/",
          "base_0000005_v900/abc.txt",
          "base_0000005_v900/0000/",
          "base_0000005_v900/0000/abc.txt",
          "base_0000007_v950/",
          "base_0000007_v950/abc.txt",
          "base_0000008_v1000/",
          "base_0000008_v1000/abc.txt",
          "delta_0000006_0000006_0000/",
          "delta_0000006_0000006_0000/000000_0",
          "delta_0000007_0000007_0000/",
          "delta_0000007_0000007_0000/000000_0",
          "delta_0000008_0000008_0000/",
          "delta_0000008_0000008_0000/000000_0",
          "delta_0000009_0000009_0000/",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"},
        "1100:1100::1000", // txn 1000 is aborted
        "default.test:10:10::", // write ids are committed
        new String[]{
          "base_0000007_v950/abc.txt",
          "delta_0000008_0000008_0000/000000_0",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"});
  }

  @Test
  public void testAbortedTransactions() {
    assertFiltering(new String[]{
          "base_01.txt",
          "post_upgrade.txt",
          "base_0000005/",
          "base_0000005/abc.txt",
          "base_0000005/0000/",
          "base_0000005/0000/abc.txt",
          "_tmp.base_0000005/000000_0.manifest",
          "delta_0000006_0000006_0000/",
          "delta_0000006_0000006_0000/000000_0",
          "delta_0000007_0000007_0000/",
          "delta_0000007_0000007_0000/000000_0",
          "delta_0000008_0000008_0000/",
          "delta_0000008_0000008_0000/000000_0",
          "delta_0000009_0000009_0000/",
          "delta_0000009_0000009_0000/000000_0",
          "delta_0000009_0000009_0000/0000/def.txt"},
        "default.test:10:1337::7,8,9", // 7,8,9 are aborted write ids
        new String[]{
          "base_0000005/abc.txt",
          "base_0000005/0000/abc.txt",
          "delta_0000006_0000006_0000/000000_0"});
  }

  @Test
  public void testPostCompactionBase() {
    assertFiltering(new String[]{
          "base_0000003_v0003217/",
          "base_0000003_v0003217/000000_0",
          "delta_0000001_0000001_0000/",
          "delta_0000001_0000001_0000/000000_0",
          "delta_0000002_0000002_0000/",
          "delta_0000002_0000002_0000/000000_0",
          "delta_0000003_0000003_0000/",
          "delta_0000003_0000003_0000/000000_0"},
      "test_acid_compaction_676f9a30.tt:3:9223372036854775807::",
      new String[]{
          "base_0000003_v0003217/000000_0"});
  }

  @Test
  public void testDeltaPredicate() {
    String[] paths = new String[]{
        "delta_000005_0000005/",
        "delta_000005_0000005/abc.txt",
        "delta_000005_0000005_0000/",
        "delta_000005_0000005_0000/abc.txt",
        "delta_000006_0000020/",
        "delta_000006_0000020/def.txt",
        "delta_000005.txt"};
    // Only committed up to write id 10, so we can select 6-20 delta.
    assertFiltering(paths,
      "default.test:10:1234:1,2,3",
      new String[]{
          "delta_000005_0000005/abc.txt",
          "delta_000006_0000020/def.txt",
          "delta_000005.txt",
          });
  }

  @Test
  public void testAcidStateFilter() {
    assertFiltering(new String[]{
          "base_0000009/",
          "base_0000009/abc.txt",
          "delta_000005_000005_0000/",
          "delta_000005_000005_0000/lmn.txt",
          "base_0000010/",
          "base_0000010/00000_0",
          "base_0000010/00001_0",
          "delta_0000012_0000012_0000/",
          "delta_0000012_0000012_0000/0000_0",
          "delta_0000012_0000012_0000/0000_1"},
      "", // Accept all,
      new String[]{
          "base_0000010/00000_0",
          "base_0000010/00001_0",
          "delta_0000012_0000012_0000/0000_0",
          "delta_0000012_0000012_0000/0000_1"});
  }

  @Test
  public void testAcidStateNoBase() {
    assertFiltering(new String[]{
            "base_01.txt",
            "post_upgrade.txt",
            "delta_000005_000005_0000/",
            "delta_000005_000005_0000/lmn.txt",
            "base_000010/",
            "delta_0000012_0000012_0000/",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"},
        "", // writeIdList that accepts all transactions as valid
        new String[]{
            // Post upgrade files are ignored if there is a valid base.
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"});

    // Same set of files, but no base directory.
    assertFiltering(new String[]{
            "base_01.txt",
            "post_upgrade.txt",
            "delta_000005_000005_0000/",
            "delta_000005_000005_0000/lmn.txt",
            "delta_0000012_0000012_0000/",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"},
        "", // writeIdList that accepts all transactions as valid
        new String[]{
            "base_01.txt", // Post upgrade files are considered valid if there is no base.
            "post_upgrade.txt",
            "delta_000005_000005_0000/lmn.txt",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"});
  }

  @Test
  public void testMinorCompactionAllTxnsValid() {
    assertFiltering(new String[]{
            "base_0000005/",
            "base_0000005/abc.txt",
            "delta_0000006_0000006_v01000/00000",
            "delta_0000007_0000007_v01000/00000",
            "delta_0000006_0000007_v01000/00000"},
        "", // all txns are valid
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:1,2,3",
        new String[]{
          "base_0000005/abc.txt",
          "delta_0000006_0000007_v01000/00000"});
    // Minor compact multi-statement transaction
    assertFiltering(new String[]{
          "base_0000005/",
          "base_0000005/abc.txt",
          "delta_0000006_0000006_00001/00000",  // statement id 1
          "delta_0000006_0000006_00002/00000",  // statement id 2
          "delta_0000006_0000006_00003/00000",  // statement id 3
          "delta_0000006_0000006_v01000/00000", // compacted
          "delta_0000006_0000006_v01000/00001"},
        "", // all txns are valid
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:",
        new String[]{
          "base_0000005/abc.txt",
          "delta_0000006_0000006_v01000/00000",
          "delta_0000006_0000006_v01000/00001"});
    // Disjunct minor compacted delta dirs
    assertFiltering(new String[]{
          "delta_0000001_0000001/00000",
          "delta_0000002_0000002/00000",
          "delta_0000003_0000003/00000",
          "delta_0000004_0000004/00000",
          "delta_0000005_0000005/00000",
          "delta_0000006_0000006/00000",
          "delta_0000007_0000007/00000",
          "delta_0000001_0000003_v00100/00000",
          "delta_0000004_0000005_v00101/00000",
          "delta_0000001_0000005_v00102/00000",
          "delta_0000006_0000007_v00123/00000",
          "delta_0000006_0000007_v00123/00001"},
        "", // all txns are valid
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:",
        new String[]{
          "delta_0000001_0000005_v00102/00000",
          "delta_0000006_0000007_v00123/00000",
          "delta_0000006_0000007_v00123/00001"});
    // Compacted delta range contains aborted write id
    assertFiltering(new String[]{
      "delta_0000001_0000001/00000",
      "delta_0000002_0000002/00000",
      "delta_0000003_0000003/00000",
      "delta_0000001_0000003_v01000/00000"},
    "", // all txns are valid
    // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
    "default.test:10:5::2",
    new String[]{"delta_0000001_0000003_v01000/00000"});
  }

  @Test
  public void testInProgressMinorCompactions() {
    assertFiltering(new String[]{
            "base_0000005/",
            "base_0000005/abc.txt",
            "delta_0000006_0000006/00000",
            "delta_0000007_0000007/00000",
            "delta_0000006_0000007_v100/00000"},
        // Txns valid up to id 90, so 100 is invalid
        "90:90::",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:1,2,3",
        new String[]{
          "base_0000005/abc.txt",
          "delta_0000006_0000006/00000",
          "delta_0000007_0000007/00000"});
    // Minor compact multi-statement transaction
    assertFiltering(new String[]{
          "base_0000005/",
          "base_0000005/abc.txt",
          "delta_0000006_0000006_00001/00000", // statement id 1
          "delta_0000006_0000006_00002/00000", // statement id 2
          "delta_0000006_0000006_00003/00000", // statement id 3
          "delta_0000006_0000006_v100/00000",  // no statement id => compacted
          "delta_0000006_0000006_v100/00001"},
        // Txn 100 is invalid
        "110:100:100:",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:",
        new String[]{
          "base_0000005/abc.txt",
          "delta_0000006_0000006_00001/00000",
          "delta_0000006_0000006_00002/00000",
          "delta_0000006_0000006_00003/00000"});
    // Disjunct minor compacted delta dirs
    assertFiltering(new String[]{
          "delta_0000001_0000001/00000",
          "delta_0000002_0000002/00000",
          "delta_0000003_0000003/00000",
          "delta_0000004_0000004/00000",
          "delta_0000005_0000005/00000",
          "delta_0000006_0000006/00000",
          "delta_0000007_0000007/00000",
          "delta_0000001_0000003_v00100/00000",
          "delta_0000004_0000005_v00101/00000",
          "delta_0000001_0000005_v00102/00000",
          "delta_0000006_0000007_v00123/00000",
          "delta_0000006_0000007_v00123/00001"},
        // Txn 102 is invalid (minor compaction 1-5)
        "130:102:102:",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:",
        new String[]{
          "delta_0000001_0000003_v00100/00000",
          "delta_0000004_0000005_v00101/00000",
          "delta_0000006_0000007_v00123/00000",
          "delta_0000006_0000007_v00123/00001"});
  }

  @Test
  public void testDeleteDelta() {
    assertFiltering(new String[]{
            "base_0000005/",
            "base_0000005/abc.txt",
            "delete_delta_0000006_0000006/",
            "delete_delta_0000006_0000006/00000"},
        // all txns are valid
        "",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:1,2,3",
        new String[]{
          "base_0000005/abc.txt",
          "delete_delta_0000006_0000006/00000"});
    assertFiltering(new String[]{
            "base_0000005/",
            "base_0000005/abc.txt",
            "delete_delta_0000006_0000006/",
            "delete_delta_0000006_0000006/00000",
            "delete_delta_0000007_0000007/00000",
            "delete_delta_0000007_0000007/00001",
            "delete_delta_0000008_0000008/00000",
            "delete_delta_0000009_0000009/00000",
            "delete_delta_0000010_0000010/00000",
            "delete_delta_0000011_0000011/00000"},
        // all txns are valid
        "",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:10:1234:6:9",
        new String[]{
          "base_0000005/abc.txt",
          "delete_delta_0000007_0000007/00000",
          "delete_delta_0000007_0000007/00001",
          "delete_delta_0000008_0000008/00000",
          "delete_delta_0000010_0000010/00000"});
  }

  public void testHiveStreamingFail() {
    filteringError(new String[]{
            "base_0000005/",
            "base_0000005/abc.txt",
            "delta_0000006_0000016/",
            "delta_0000006_0000016/00000_0",
            "delta_0000006_0000016/00000_0_flush_length"},
        // all txns are valid
        "",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:22:1234:1,2,3",
        "Found Hive Streaming side-file"
        );
    assertFiltering(new String[]{
            "base_0000005/",
            "base_0000005/abc.txt",
            "delta_0000006_0000016/",
            "delta_0000006_0000016/00000_0",
            "delta_0000006_0000016/00000_0_flush_length",
            "base_0000017_v123/0000_0"},
        // all txns are valid
        "",
        // <tbl>:<hwm>:<minOpenWriteId>:<openWriteIds>:<abortedWriteIds>
        "default.test:22:1234:1,2,3",
        new String[]{"base_0000017_v123/0000_0"});
  }

  @Test
  public void testMinorCompactionBeforeBase() {
    assertFiltering(new String[]{
            "delta_000005_000008_0000/",
            "delta_000005_000008_0000/abc.txt",
            "base_000010/",
            "base_000010/0000_0",
            "delta_0000012_0000012_0000/",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"},
        // <table>:<highWaterMark>:<minOpenWriteId>
        "default.test:20:15::",
        new String[]{
            "base_000010/0000_0",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"});
  }

  @Test
  public void testDeletesBeforeBase() {
    assertFiltering(new String[]{
            "delta_000004_000004_0000/",
            "delta_000004_000004_0000/0000",
            "delete_delta_000005_000005_0000/",
            "delete_delta_000005_000005_0000/0000",
            "base_000010/",
            "base_000010/0000_0",
            "delta_0000012_0000012_0000/",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"},
        // <table>:<highWaterMark>:<minOpenWriteId>
        "default.test:20:15::",
        new String[]{
            // No deletes after base directory so it should succeed.
            "base_000010/0000_0",
            "delta_0000012_0000012_0000/0000_0",
            "delta_0000012_0000012_0000/0000_1"});
  }

  @Test
  public void testWriteIdListCompare() {
    ValidWriteIdList a =
            new ValidReaderWriteIdList("default.test:1:1:1:");
    ValidWriteIdList b =
            new ValidReaderWriteIdList("default.test:1:9223372036854775807::");

    // should return -1 since b is more recent
    assert(AcidUtils.compare(a, b) == -1);

    //Should return 1 since b is more recent
    assert(AcidUtils.compare(b,a) == 1);
  }
}
