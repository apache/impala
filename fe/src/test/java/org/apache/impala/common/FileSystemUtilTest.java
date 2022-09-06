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

import org.apache.impala.common.Pair;
import static org.apache.impala.common.FileSystemUtil.HIVE_TEMP_FILE_PREFIX;
import static org.apache.impala.common.FileSystemUtil.SPARK_TEMP_FILE_PREFIX;
import static org.apache.impala.common.FileSystemUtil.isIgnoredDir;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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

    assertTrue("Files in spark temporary directories should be ignored",
        testIsInIgnoredDirectory(new Path(TEST_TABLE_PATH,
            SPARK_TEMP_FILE_PREFIX + "/0")));

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

  @Test
  public void testFsType() throws IOException {
    testFsType(mockLocation(FileSystemUtil.SCHEME_ABFS), FileSystemUtil.FsType.ADLS);
    testFsType(mockLocation(FileSystemUtil.SCHEME_ABFSS), FileSystemUtil.FsType.ADLS);
    testFsType(mockLocation(FileSystemUtil.SCHEME_ADL), FileSystemUtil.FsType.ADLS);
    testFsType(mockLocation(FileSystemUtil.SCHEME_FILE), FileSystemUtil.FsType.LOCAL);
    testFsType(mockLocation(FileSystemUtil.SCHEME_HDFS), FileSystemUtil.FsType.HDFS);
    testFsType(mockLocation(FileSystemUtil.SCHEME_S3A), FileSystemUtil.FsType.S3);
    testFsType(mockLocation(FileSystemUtil.SCHEME_O3FS), FileSystemUtil.FsType.OZONE);
    testFsType(mockLocation(FileSystemUtil.SCHEME_OFS), FileSystemUtil.FsType.OZONE);
    testFsType(
        mockLocation(FileSystemUtil.SCHEME_ALLUXIO), FileSystemUtil.FsType.ALLUXIO);
  }

  @Test
  public void testSupportStorageIds() throws IOException {
    testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_ABFS), false);
    testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_ABFSS), false);
    testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_ADL), false);
    testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_FILE), false);
    testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_S3A), false);

    testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_HDFS), true);

    // The following tests are disabled because the underlying systems is not included
    // in impala mini cluster.
    // TODO: enable following tests if we add them into impala mini cluster.
    // testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_O3FS), true);
    // testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_OFS), true);
    // testIsSupportStorageIds(mockLocation(FileSystemUtil.SCHEME_ALLUXIO), true);
  }

  @Test
  public void testWriteableByImpala() throws IOException {
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_ALLUXIO), false);

    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_ABFS), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_ABFSS), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_ADL), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_FILE), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_HDFS), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_S3A), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_O3FS), true);
    testIsWritableByImpala(mockLocation(FileSystemUtil.SCHEME_OFS), true);
  }

  @Test
  public void testSupportedDefaultFs() throws IOException {
    testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_ABFS), true);
    testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_ABFSS), true);
    testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_ADL), true);
    testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_HDFS), true);
    testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_S3A), true);

    testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_FILE), false);

    // The following tests are disabled because the underlying systems is not included
    // in impala mini cluster.
    // TODO: enable following tests if we add them into impala mini cluster.
    // testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_O3FS), false);
    // testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_OFS), false);
    // testIsSupportedDefaultFs(mockLocation(FileSystemUtil.SCHEME_ALLUXIO), false);
  }

  @Test
  public void testValidLoadDataInpath() throws IOException {
    testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_ABFS), true);
    testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_ABFSS), true);
    testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_ADL), true);
    testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_HDFS), true);
    testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_S3A), true);

    testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_FILE), false);

    // The following tests are disabled because the underlying systems is not included
    // in impala mini cluster.
    // TODO: enable following tests if we add them into impala mini cluster.
    // testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_O3FS), true);
    // testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_OFS), true);
    // testValidLoadDataInpath(mockLocation(FileSystemUtil.SCHEME_ALLUXIO), false);
    // Also extend testIsPathOnFileSystem().
  }

  @Test
  public void testIsPathOnFileSystem() throws IOException {
    List<String> schemes = Arrays.asList(
        FileSystemUtil.SCHEME_ABFS,
        FileSystemUtil.SCHEME_ABFSS,
        FileSystemUtil.SCHEME_ADL,
        FileSystemUtil.SCHEME_HDFS,
        FileSystemUtil.SCHEME_S3A,
        FileSystemUtil.SCHEME_FILE);
    List<Path> mockFiles = new ArrayList<>();
    for (String scheme : schemes) {
      mockFiles.add(new Path(mockLocation(scheme)));
    }
    List<FileSystem> fileSystems = new ArrayList<>();
    for (Path mockFile : mockFiles) {
      fileSystems.add(FileSystemUtil.getFileSystemForPath(mockFile));
    }
    for (int i = 0; i < fileSystems.size(); ++i) {
      FileSystem fs = fileSystems.get(i);
      for (int j = 0; j < mockFiles.size(); ++j) {
        Path mockFile = mockFiles.get(j);
        if (i == j) {
          assertTrue(String.format(
                          "Path '%s' should be on file system '%s'", mockFile, fs),
                     FileSystemUtil.isPathOnFileSystem(mockFile, fs));
        } else {
          assertFalse(String.format(
                          "Path '%s' should not be on file system '%s'", mockFile, fs),
                      FileSystemUtil.isPathOnFileSystem(mockFile, fs));
        }
      }
    }
  }

  @Test
  public void testVolumeBucketPair() throws IOException {
    List<Pair<String, Pair<String, String>>> cases = Arrays.asList(
      Pair.create(mockLocation(FileSystemUtil.SCHEME_OFS),
          Pair.create("volume1", "bucket2")),
      Pair.create("ofs://svc1:9876/volume/bucket/file", Pair.create("volume", "bucket")),
      Pair.create("ofs://svc1:9876/volume/bucket/", Pair.create("volume", "bucket")),
      Pair.create("ofs://svc1:9876/volume/bucket", Pair.create("volume", "bucket")),
      Pair.create("ofs://svc1:9876/volume/", Pair.create("volume", "")),
      Pair.create("ofs://svc1:9876/volume", Pair.create("volume", "")),
      Pair.create("ofs://svc1:9876/", Pair.create("", ""))
    );
    for (Pair<String, Pair<String, String>> c : cases) {
      Path p = new Path(c.first);
      assertEquals(c.second, FileSystemUtil.volumeBucketPair(p));
    }
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

  private String mockLocation(String scheme) throws IOException {
    switch (scheme) {
      case FileSystemUtil.SCHEME_ABFS:
        return "abfs://dummy-fs@dummy-account.dfs.core.windows.net/dummy-part-1";
      case FileSystemUtil.SCHEME_ABFSS:
        return "abfss://dummy-fs@dummy-account.dfs.core.windows.net/dummy-part-2";
      case FileSystemUtil.SCHEME_ADL:
        return "adl://dummy-account.azuredatalakestore.net/dummy-part-3";
      case FileSystemUtil.SCHEME_FILE:
        return "file:///tmp/dummy-part-4";
      case FileSystemUtil.SCHEME_HDFS:
        return "hdfs://localhost:20500/dummy-part-5";
      case FileSystemUtil.SCHEME_S3A:
        return "s3a://dummy-bucket/dummy-part-6";
      case FileSystemUtil.SCHEME_O3FS:
        return "o3fs://bucket.volume/key";
      case FileSystemUtil.SCHEME_OFS:
        return "ofs://svc1:9876/volume1/bucket2/dir3/";
      case FileSystemUtil.SCHEME_ALLUXIO:
        return "alluxio://zk@zk-1:2181,zk-2:2181,zk-3:2181/path/";
      default:
        throw new IOException("FileSystem scheme is not supported!");
    }
  }

  private void testFsType(String location, FileSystemUtil.FsType expected) {
    Path path = new Path(location);
    FileSystemUtil.FsType type =
        FileSystemUtil.FsType.getFsType(path.toUri().getScheme());
    assertEquals(type, expected);
  }

  private void testIsSupportStorageIds(String location, boolean expected)
      throws IOException {
    Path path = new Path(location);
    FileSystem fs = FileSystemUtil.getFileSystemForPath(path);
    assertEquals(FileSystemUtil.supportsStorageIds(fs), expected);
  }

  private void testIsWritableByImpala(String location, boolean expected)
      throws IOException {
    assertEquals(FileSystemUtil.isImpalaWritableFilesystem(location), expected);
  }

  private void testIsSupportedDefaultFs(String location, boolean expected)
      throws IOException {
    Path path = new Path(location);
    FileSystem fs = FileSystemUtil.getFileSystemForPath(path);
    assertEquals(FileSystemUtil.isValidDefaultFileSystem(fs), expected);
  }

  private void testValidLoadDataInpath(String location, boolean expected)
      throws IOException {
    Path path = new Path(location);
    FileSystem fs = FileSystemUtil.getFileSystemForPath(path);
    assertEquals(FileSystemUtil.isValidLoadDataInpath(fs), expected);
  }
}
