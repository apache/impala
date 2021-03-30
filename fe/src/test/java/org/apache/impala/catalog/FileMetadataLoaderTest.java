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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;

public class FileMetadataLoaderTest {

  @Test
  public void testRecursiveLoading() throws IOException, MetaException {
    //TODO(IMPALA-9042): Remove "throws MetaException"
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/alltypes/");
    FileMetadataLoader fml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */Collections.emptyList(), hostIndex, null, null);
    fml.load();
    assertEquals(24, fml.getStats().loadedFiles);
    assertEquals(24, fml.getLoadedFds().size());

    // Test that relative paths are constructed properly.
    ArrayList<String> relPaths = new ArrayList<>(Collections2.transform(
        fml.getLoadedFds(), FileDescriptor::getRelativePath));
    Collections.sort(relPaths);
    assertEquals("year=2009/month=1/090101.txt", relPaths.get(0));
    assertEquals("year=2010/month=9/100901.txt", relPaths.get(23));

    // Test that refreshing is properly incremental if no files changed.
    FileMetadataLoader refreshFml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */fml.getLoadedFds(), hostIndex, null, null);
    refreshFml.load();
    assertEquals(24, refreshFml.getStats().skippedFiles);
    assertEquals(0, refreshFml.getStats().loadedFiles);
    assertEquals(fml.getLoadedFds(), refreshFml.getLoadedFds());

    // Touch a file and make sure that we reload locations for that file.
    FileSystem fs = tablePath.getFileSystem(new Configuration());
    FileDescriptor fd = fml.getLoadedFds().get(0);
    Path filePath = new Path(tablePath, fd.getRelativePath());
    fs.setTimes(filePath, fd.getModificationTime() + 1, /* atime= */-1);

    refreshFml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */fml.getLoadedFds(), hostIndex, null, null);
    refreshFml.load();
    assertEquals(1, refreshFml.getStats().loadedFiles);
  }

  @Test
  public void testHudiParquetLoading() throws IOException, MetaException {
    //TODO(IMPALA-9042): Remove "throws MetaException"
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/hudi_parquet/");
    FileMetadataLoader fml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */ Collections.emptyList(), hostIndex, null, null,
        HdfsFileFormat.HUDI_PARQUET);
    fml.load();
    // 6 files in total, 3 files with 2 versions and we should only load the latest
    // version of each
    assertEquals(3, fml.getStats().loadedFiles);
    assertEquals(3, fml.getLoadedFds().size());

    // Test the latest version was loaded.
    ArrayList<String> relPaths = new ArrayList<>(
        Collections2.transform(fml.getLoadedFds(), FileDescriptor::getRelativePath));
    Collections.sort(relPaths);
    assertEquals(
        "year=2015/month=03/day=16/5f541af5-ca07-4329-ad8c-40fa9b353f35-0_2-103-391"
            + "_20200210090618.parquet",
        relPaths.get(0));
    assertEquals(
        "year=2015/month=03/day=17/675e035d-c146-4658-9404-fe590e296d80-0_0-103-389"
            + "_20200210090618.parquet",
        relPaths.get(1));
    assertEquals(
        "year=2016/month=03/day=15/940359ee-cc79-4974-8a2a-5d133a81a3fd-0_1-103-390"
            + "_20200210090618.parquet",
        relPaths.get(2));
  }

  @Test
  public void testAcidMinorCompactionLoading() throws IOException, MetaException {
    //TODO(IMPALA-9042): Remove "throws MetaException"
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    ValidWriteIdList writeIds = MetastoreShim.getValidWriteIdListFromString(
        "functional_orc_def.complextypestbl_minor_compacted:10:10::");
    Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/managed/" +
                              "complextypestbl_minor_compacted_orc_def/");
    FileMetadataLoader fml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */ Collections.emptyList(), hostIndex, new ValidReadTxnList(""),
        writeIds, HdfsFileFormat.ORC);
    fml.load();
    // Only load the compacted file.
    assertEquals(1, fml.getStats().loadedFiles);
    // 2 * 8 files since the hidden '_orc_acid_version' is filtered out later.
    assertEquals(16, fml.getStats().filesSupersededByAcidState);
  }

  @Test
  public void testLoadMissingDirectory() throws IOException, MetaException {
    //TODO(IMPALA-9042): Remove "throws MetaException"
    for (boolean recursive : ImmutableList.of(false, true)) {
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/does-not-exist/");
      FileMetadataLoader fml = new FileMetadataLoader(tablePath, recursive,
          /* oldFds = */Collections.emptyList(), hostIndex, null, null);
      fml.load();
      assertEquals(0, fml.getLoadedFds().size());
    }
  }

  //TODO(IMPALA-9042): Remove 'throws MetaException'
  @Test
  public void testSkipHiddenDirectories() throws IOException, MetaException {
    Path sourcePath = new Path("hdfs://localhost:20500/test-warehouse/alltypes/");
    Path tmpTestPath = new Path("hdfs://localhost:20500/tmp/test-filemetadata-loader");
    Configuration conf = new Configuration();
    FileSystem dstFs = tmpTestPath.getFileSystem(conf);
    FileSystem srcFs = sourcePath.getFileSystem(conf);
    //copy the file-structure of a valid table
    FileUtil.copy(srcFs, sourcePath, dstFs, tmpTestPath, false, true, conf);
    dstFs.deleteOnExit(tmpTestPath);
    // create a hidden directory similar to what hive does
    Path hiveStaging = new Path(tmpTestPath, ".hive-staging_hive_2019-06-13_1234");
    dstFs.mkdirs(hiveStaging);
    Path manifestDir = new Path(tmpTestPath, "_tmp.base_0000007");
    dstFs.mkdirs(manifestDir);
    dstFs.createNewFile(new Path(manifestDir, "000000_0.manifest"));
    dstFs.createNewFile(new Path(hiveStaging, "tmp-stats"));
    dstFs.createNewFile(new Path(hiveStaging, ".hidden-tmp-stats"));

    FileMetadataLoader fml = new FileMetadataLoader(tmpTestPath, true,
        Collections.emptyList(), new ListMap <>(), null, null);
    fml.load();
    assertEquals(24, fml.getStats().loadedFiles);
    assertEquals(24, fml.getLoadedFds().size());
  }

  // TODO(todd) add unit tests for loading ACID tables once we have some ACID
  // tables with data loaded in the functional test DBs.
}
