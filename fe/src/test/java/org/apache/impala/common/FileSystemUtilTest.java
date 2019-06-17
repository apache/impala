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

package org.apache.impala.common;

import static org.apache.impala.common.FileSystemUtil.HIVE_TEMP_FILE_PREFIX;
import static org.apache.impala.common.FileSystemUtil.isIgnoredDir;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the various util methods in FileSystemUtil class
 */
public class FileSystemUtilTest {

  private static final Path TEST_TABLE_PATH = new Path("/test-warehouse/foo"
      + ".db/filesystem-util-test");

  @Test
  public void testIsInIgnoredDirectory() {
    // test positive cases
    assertTrue("Files in hive staging directory should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH, "/part=1/"
            + ".hive-staging/tempfile")));

    assertTrue("Files in hidden directory ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH, ".hidden/000000_0")));

    assertTrue("Files in the hive temporary directories should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
            HIVE_TEMP_FILE_PREFIX + "base_0000000_1/000000_1.manifest")));

    assertTrue("Files in hive temporary directories should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
            HIVE_TEMP_FILE_PREFIX + "delta_000000_2/test.manifest")));

    //multiple nested levels
    assertTrue(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
        ".hive-staging/nested-1/nested-2/nested-3/tempfile")));

    // test negative cases
    // table path should not ignored
    assertFalse(testIsInIgnoredDirectory(TEST_TABLE_PATH));
    assertFalse(
        testIsInIgnoredDirectory(new Path("hdfs://localhost:20500" + TEST_TABLE_PATH)));
    // partition path
    assertFalse(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH + "/part=1/000000")));
    assertFalse(testIsInIgnoredDirectory(
        new Path("hdfs://localhost:20500" + TEST_TABLE_PATH + "/part=1/00000")));
    // nested directories for ACID tables should not be ignored
    assertFalse(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH, "/part=100"
        + "/base_0000005/datafile")));
    assertFalse(testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
        "/delta_0000001_0000002/deltafile")));

  }

  @Test
  public void testIsIgnoredDir() {
    assertTrue("Directory should be ignored if it starts with _tmp.",
        isIgnoredDir(new Path(TEST_TABLE_PATH, HIVE_TEMP_FILE_PREFIX + "dummy")));
    assertTrue("Directory should be ignored if its hidden",
        isIgnoredDir(new Path(TEST_TABLE_PATH, ".hidden-dir")));
    assertFalse(isIgnoredDir(TEST_TABLE_PATH));
    assertFalse(isIgnoredDir(new Path(TEST_TABLE_PATH + "/part=100/datafile")));
  }

  private boolean testIsInIgnoredDirectory(Path input) {
    return testIsInIgnoredDirectory(input, true);
  }

  private boolean testIsInIgnoredDirectory(Path input, boolean isDir) {
    FileStatus mockFileStatus = Mockito.mock(FileStatus.class);
    Mockito.when(mockFileStatus.getPath()).thenReturn(input);
    Mockito.when(mockFileStatus.isDirectory()).thenReturn(isDir);
    return FileSystemUtil.isInIgnoredDirectory(TEST_TABLE_PATH, mockFileStatus);
  }
}
