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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.catalog.iceberg.GroupedContentFiles;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.IcebergUtil;
import org.apache.impala.util.ListMap;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;

public class FileMetadataLoaderTest {

  @Test
  public void testRecursiveLoading() throws IOException, CatalogException {
    //TODO(IMPALA-9042): Remove "throws CatalogException"
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
  public void testHudiParquetLoading() throws IOException, CatalogException {
    //TODO(IMPALA-9042): Remove "throws CatalogException"
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
  public void testIcebergLoading() throws IOException, CatalogException {
    CatalogServiceCatalog catalog = CatalogServiceTestCatalog.create();
    IcebergFileMetadataLoader fml1 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml1.load();
    assertEquals(20, fml1.getStats().loadedFiles);
    assertEquals(0, fml1.getStats().skippedFiles);
    assertEquals(20, fml1.getLoadedFds().size());

    ArrayList<String> relPaths = new ArrayList<>(
        Collections2.transform(fml1.getLoadedFds(), FileDescriptor::getRelativePath));
    Collections.sort(relPaths);
    assertEquals("data/event_time_hour=2020-01-01-08/action=view/" +
        "00001-1-b975a171-0911-47c2-90c8-300f23c28772-00000.parquet", relPaths.get(0));

    IcebergFileMetadataLoader fml2 =  getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml2.load();
    assertEquals(20, fml2.getStats().loadedFiles);
    assertEquals(0, fml2.getStats().skippedFiles);
    assertEquals(20, fml2.getLoadedFds().size());

    relPaths = new ArrayList<>(
        Collections2.transform(fml2.getLoadedFds(), FileDescriptor::getRelativePath));
    Collections.sort(relPaths);
    assertEquals("data/00001-1-5dbd44ad-18bc-40f2-9dd6-aeb2cc23457c-00000.parquet",
        relPaths.get(0));
  }

  @Test
  public void testIcebergRefresh() throws IOException, CatalogException {
    CatalogServiceCatalog catalog = CatalogServiceTestCatalog.create();
    IcebergFileMetadataLoader fml1 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml1.load();

    IcebergFileMetadataLoader fml1Refresh = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ fml1.getLoadedFds(),
        /* canDataBeOutsideOfTableLocation = */ false);
    assertTrue(fml1Refresh.shouldReuseOldFds());
    fml1Refresh.load();
    assertEquals(0, fml1Refresh.getStats().loadedFiles);
    assertEquals(20, fml1Refresh.getStats().skippedFiles);
    assertEquals(20, fml1Refresh.getLoadedFds().size());

    List<String> relPaths = new ArrayList<>(
        Collections2.transform(fml1Refresh.getLoadedFds(),
        FileDescriptor::getRelativePath));
    Collections.sort(relPaths);
    assertEquals("data/event_time_hour=2020-01-01-08/action=view/" +
        "00001-1-b975a171-0911-47c2-90c8-300f23c28772-00000.parquet", relPaths.get(0));

    IcebergFileMetadataLoader fml2 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml2.load();

    IcebergFileMetadataLoader fml2Refresh = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ fml2.getLoadedFds(),
        /* canDataBeOutsideOfTableLocation = */ false);
    assertTrue(fml2Refresh.shouldReuseOldFds());
    fml2Refresh.load();
    assertEquals(0, fml2Refresh.getStats().loadedFiles);
    assertEquals(20, fml2Refresh.getStats().skippedFiles);
    assertEquals(20, fml2Refresh.getLoadedFds().size());

    relPaths = new ArrayList<>(
        Collections2.transform(fml2Refresh.getLoadedFds(),
            FileDescriptor::getRelativePath));
    Collections.sort(relPaths);
    assertEquals("data/00001-1-5dbd44ad-18bc-40f2-9dd6-aeb2cc23457c-00000.parquet",
        relPaths.get(0));
  }

  @Test
  public void testIcebergPartialRefresh() throws IOException, CatalogException {
    CatalogServiceCatalog catalog = CatalogServiceTestCatalog.create();
    IcebergFileMetadataLoader fml1 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml1.load();

    IcebergFileMetadataLoader fml1Refresh = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ fml1.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false);
    assertTrue(fml1Refresh.shouldReuseOldFds());
    fml1Refresh.load();
    assertEquals(10, fml1Refresh.getStats().loadedFiles);
    assertEquals(10, fml1Refresh.getStats().skippedFiles);
    assertEquals(20, fml1Refresh.getLoadedFds().size());

    IcebergFileMetadataLoader fml2 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml2.load();

    IcebergFileMetadataLoader fml2Refresh = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ fml2.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false);
    assertTrue(fml2Refresh.shouldReuseOldFds());
    fml2Refresh.load();
    assertEquals(10, fml2Refresh.getStats().loadedFiles);
    assertEquals(10, fml2Refresh.getStats().skippedFiles);
    assertEquals(20, fml2Refresh.getLoadedFds().size());
  }

  @Test
  public void testIcebergNewFilesThreshold() throws IOException, CatalogException {
    CatalogServiceCatalog catalog = CatalogServiceTestCatalog.create();
    IcebergFileMetadataLoader fml1 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml1.load();

    IcebergFileMetadataLoader fml1Refresh = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ fml1.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false);
    assertTrue(fml1Refresh.shouldReuseOldFds());
    fml1Refresh.setForceRefreshBlockLocations(true);
    assertFalse(fml1Refresh.shouldReuseOldFds());
    fml1Refresh.setForceRefreshBlockLocations(false);
    assertTrue(fml1Refresh.shouldReuseOldFds());

    IcebergFileMetadataLoader fml1Refresh10 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ fml1.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false, 10);
    assertTrue(fml1Refresh10.shouldReuseOldFds());
    IcebergFileMetadataLoader fml1Refresh9 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_partitioned",
        /* oldFds = */ fml1.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false, 9);
    assertFalse(fml1Refresh9.shouldReuseOldFds());


    IcebergFileMetadataLoader fml2 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ false);
    fml2.load();

    IcebergFileMetadataLoader fml2Refresh = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ fml2.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false);
    IcebergFileMetadataLoader fml2Refresh10 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ fml2.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false, 10);
    assertTrue(fml2Refresh10.shouldReuseOldFds());
    IcebergFileMetadataLoader fml2Refresh9 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_non_partitioned",
        /* oldFds = */ fml2.getLoadedFds().subList(0, 10),
        /* canDataBeOutsideOfTableLocation = */ false, 9);
    assertFalse(fml2Refresh9.shouldReuseOldFds());
  }

  @Test
  public void testIcebergMultipleStorageLocations() throws IOException, CatalogException {
    CatalogServiceCatalog catalog = CatalogServiceTestCatalog.create();
    BackendConfig.INSTANCE.setIcebergAllowDatafileInTableLocationOnly(false);
    IcebergFileMetadataLoader fml1 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_multiple_storage_locations",
        /* oldFds = */ Collections.emptyList(),
        /* canDataBeOutsideOfTableLocation = */ true);
    fml1.load();

    IcebergFileMetadataLoader fml1Refresh1 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_multiple_storage_locations",
        /* oldFds = */ fml1.getLoadedFds().subList(0, 1),
        /* canDataBeOutsideOfTableLocation = */ true);
    assertTrue(fml1Refresh1.shouldReuseOldFds());
    fml1Refresh1.load();
    assertEquals(5, fml1Refresh1.getStats().loadedFiles);
    assertEquals(1, fml1Refresh1.getStats().skippedFiles);
    assertEquals(6, fml1Refresh1.getLoadedFds().size());

    IcebergFileMetadataLoader fml1Refresh5 = getLoaderForIcebergTable(catalog,
        "functional_parquet", "iceberg_multiple_storage_locations",
        /* oldFds = */ fml1.getLoadedFds().subList(0, 5),
        /* canDataBeOutsideOfTableLocation = */ true);
    assertTrue(fml1Refresh5.shouldReuseOldFds());
    fml1Refresh5.load();
    assertEquals(1, fml1Refresh5.getStats().loadedFiles);
    assertEquals(5, fml1Refresh5.getStats().skippedFiles);
    assertEquals(6, fml1Refresh5.getLoadedFds().size());
  }

  private IcebergFileMetadataLoader getLoaderForIcebergTable(
      CatalogServiceCatalog catalog, String dbName, String tblName,
      List<FileDescriptor> oldFds, boolean canDataBeOutsideOfTableLocation)
      throws CatalogException {
    return getLoaderForIcebergTable(catalog, dbName, tblName, oldFds,
        canDataBeOutsideOfTableLocation, -1);
  }

  private IcebergFileMetadataLoader getLoaderForIcebergTable(
      CatalogServiceCatalog catalog, String dbName, String tblName,
      List<FileDescriptor> oldFds, boolean canDataBeOutsideOfTableLocation,
      int newFilesThreshold)
      throws CatalogException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    FeIcebergTable iceT = (FeIcebergTable)catalog.getOrLoadTable(
        dbName, tblName, "test", null);
    Path location = new Path(iceT.getLocation());
    GroupedContentFiles iceFiles = IcebergUtil.getIcebergFiles(iceT,
        /*predicates=*/Collections.emptyList(), /*timeTravelSpec=*/null);
    return new IcebergFileMetadataLoader(location, /* recursive=*/true,
        oldFds, hostIndex, null, null, iceFiles, canDataBeOutsideOfTableLocation,
        newFilesThreshold);
  }

  private FileMetadataLoader getLoaderForAcidTable(
      String validWriteIdString, String path, HdfsFileFormat format)
      throws IOException, CatalogException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    ValidWriteIdList writeIds =
        MetastoreShim.getValidWriteIdListFromString(validWriteIdString);
    Path tablePath = new Path(path);
    FileMetadataLoader fml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */ Collections.emptyList(), hostIndex, new ValidReadTxnList(""),
        writeIds, format);
    fml.load();
    return fml;
  }

  @Test
  public void testAcidMinorCompactionLoading() throws IOException, CatalogException {
    //TODO(IMPALA-9042): Remove "throws CatalogException"
    FileMetadataLoader fml = getLoaderForAcidTable(
        "functional_orc_def.complextypestbl_minor_compacted:10:10::",
        "hdfs://localhost:20500/test-warehouse/managed/functional_orc_def.db/" +
            "complextypestbl_minor_compacted_orc_def/",
        HdfsFileFormat.ORC);
    // Only load the compacted file.
    assertEquals(1, fml.getStats().loadedFiles);
    assertEquals(8, fml.getStats().filesSupersededByAcidState);

    fml = getLoaderForAcidTable(
        "functional_parquet.insert_only_minor_compacted:6:6::",
        "hdfs://localhost:20500/test-warehouse/managed/functional_parquet.db/" +
            "insert_only_minor_compacted_parquet/",
        HdfsFileFormat.PARQUET);
    // Only load files after compaction.
    assertEquals(3, fml.getStats().loadedFiles);
    assertEquals(2, fml.getStats().filesSupersededByAcidState);
  }

  @Test
  public void testLoadMissingDirectory() throws IOException, CatalogException {
    //TODO(IMPALA-9042): Remove "throws CatalogException"
    for (boolean recursive : ImmutableList.of(false, true)) {
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/does-not-exist/");
      FileMetadataLoader fml = new FileMetadataLoader(tablePath, recursive,
          /* oldFds = */Collections.emptyList(), hostIndex, null, null);
      fml.load();
      assertEquals(0, fml.getLoadedFds().size());
    }
  }

  //TODO(IMPALA-9042): Remove 'throws CatalogException'
  @Test
  public void testSkipHiddenDirectories() throws IOException, CatalogException {
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
