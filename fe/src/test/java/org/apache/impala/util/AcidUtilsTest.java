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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.impala.compat.MetastoreShim;
import org.hamcrest.Matchers;
import org.junit.Assume;
import org.junit.Test;

public class AcidUtilsTest {

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

    ValidWriteIdList writeIds = MetastoreShim.getValidWriteIdListFromString(
        validWriteIdListStr);
    List<FileStatus> stats = createMockStats(relPaths);
    List<FileStatus> expectedStats = createMockStats(expectedRelPaths);

    assertThat(AcidUtils.filterFilesForAcidState(stats, BASE_PATH, writeIds, null),
      Matchers.containsInAnyOrder(expectedStats.toArray()));
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
  public void testAbortedTransactions() {
    assertFiltering(new String[]{
          "base_01.txt",
          "post_upgrade.txt",
          "base_0000005/",
          "base_0000005/abc.txt",
          "base_0000005/0000/",
          "base_0000005/0000/abc.txt",
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

    // Only committed up to transaction 10, so skip the 6-20 delta.
    assertFiltering(paths,
      "default.test:10:1234:1,2,3",
      new String[]{
          "delta_000005_0000005/abc.txt",
          "delta_000005_0000005_0000/abc.txt",
          "delta_000005.txt"});
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
}
