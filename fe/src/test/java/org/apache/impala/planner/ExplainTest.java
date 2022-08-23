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

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.Path;
import org.apache.impala.analysis.TableRef;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsPartition;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TClientRequest;
import org.apache.impala.thrift.TExplainLevel;

import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TQueryOptions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for validating explain plans. This class relies on mocking
 * {@link ScanNode} objects (and other associated classes) to validate explain plan
 * output.
 */
public class ExplainTest extends FrontendTestBase {

  /**
   * IMPALA-6050: Tests that explains plans for queries that read data from multiple
   * filesystems (e.g. S3, ADLS, HDFS) accurately report the number of partitions and
   * file read from each filesystem.
   */
  @Test
  public void testScanNodeFsScheme() throws ImpalaException {
    List<HdfsPartition> partitions = new ArrayList<>();

    String dummyDbName = "dummy-db";
    String dummyTblName = "dummy-tbl";
    String dummyTblPath = "hdfs://localhost/" + dummyDbName + "." + dummyTblName;

    FeDb mockDb = mock(FeDb.class);
    when(mockDb.getName()).thenReturn(dummyDbName);
    FeFsTable mockFeFsTable = createMockFeFsTable(partitions, dummyTblName, mockDb);
    TupleDescriptor tupleDescriptor = createMockTupleDescriptor(mockFeFsTable);
    TableRef mockTableRef = mock(TableRef.class);
    when(mockTableRef.getTable()).thenReturn(mockFeFsTable);

    partitions.add(createMockHdfsPartition("abfs://dummy-fs@dummy-account.dfs.core"
            + ".windows.net/dummy-part-1",
        FileSystemUtil.FsType.ADLS));
    partitions.add(createMockHdfsPartition("abfs://dummy-fs@dummy-account.dfs.core"
            + ".windows.net/dummy-part-2",
        FileSystemUtil.FsType.ADLS));
    partitions.add(createMockHdfsPartition("abfss://dummy-fs@dummy-account.dfs.core"
            + ".windows.net/dummy-part-3",
        FileSystemUtil.FsType.ADLS));
    partitions.add(createMockHdfsPartition("abfss://dummy-fs@dummy-account.dfs.core"
            + ".windows.net/dummy-part-4",
        FileSystemUtil.FsType.ADLS));
    partitions.add(createMockHdfsPartition("adl://dummy-account.azuredatalakestore"
            + ".net/dummy-part-5",
        FileSystemUtil.FsType.ADLS));
    partitions.add(createMockHdfsPartition("adl://dummy-account.azuredatalakestore"
            + ".net/dummy-part-6",
        FileSystemUtil.FsType.ADLS));
    partitions.add(createMockHdfsPartition(
        "s3a://dummy-bucket/dummy-part-7", FileSystemUtil.FsType.S3));
    partitions.add(createMockHdfsPartition(
        "s3a://dummy-bucket/dummy-part-8", FileSystemUtil.FsType.S3));
    partitions.add(createMockHdfsPartition(
        dummyTblPath + "/dummy-part-9", FileSystemUtil.FsType.HDFS));
    partitions.add(createMockHdfsPartition(
        dummyTblPath + "/dummy-part-10", FileSystemUtil.FsType.HDFS));

    HdfsScanNode hdfsScanNode =
        new HdfsScanNode(PlanNodeId.createGenerator().getNextId(), tupleDescriptor,
            new ArrayList<>(), partitions, mockTableRef, null, new ArrayList<>(),
            false);

    Analyzer mockAnalyer = createMockAnalyzer();

    hdfsScanNode.init(mockAnalyer);

    List<String> explainString =
        Lists.newArrayList(Splitter.on('\n').omitEmptyStrings().trimResults().split(
            hdfsScanNode.getNodeExplainString("", "", TExplainLevel.STANDARD)));

    Assert.assertEquals(
        "Scan node explain string not of expected size", 4, explainString.size());
    Assert.assertTrue("Scan node explain string does not contain correct base table "
            + "scheme",
        explainString.get(0).contains("SCAN HDFS"));
    Assert.assertTrue("Scan node explain string does not correct ADLS metadata",
        explainString.get(1).contains("ADLS partitions=6/10 files=6 size=6B"));
    Assert.assertTrue("Scan node explain string does not correct HDFS metadata",
        explainString.get(2).contains("HDFS partitions=2/10 files=2 size=2B"));
    Assert.assertTrue("Scan node explain string does not correct S3 metadata",
        explainString.get(3).contains("S3 partitions=2/10 files=2 size=2B"));
  }

  private TupleDescriptor createMockTupleDescriptor(FeFsTable mockFeFsTable) {
    TupleDescriptor tupleDescriptor = mock(TupleDescriptor.class);
    when(tupleDescriptor.getTable()).thenReturn(mockFeFsTable);
    when(tupleDescriptor.getId()).thenReturn(TupleId.createGenerator().getNextId());
    when(tupleDescriptor.getPath()).thenReturn(mock(Path.class));
    return tupleDescriptor;
  }

  private FeFsTable createMockFeFsTable(
      List<HdfsPartition> partitions, String dummyTblName, FeDb mockDb) {
    FeFsTable mockFeFsTable = mock(FeFsTable.class);
    when(mockFeFsTable.getFsType()).thenReturn(FileSystemUtil.FsType.HDFS);
    when(mockFeFsTable.getMetaStoreTable()).thenReturn(mock(Table.class));
    doReturn(partitions).when(mockFeFsTable).getPartitions();
    when(mockFeFsTable.getDb()).thenReturn(mockDb);
    when(mockFeFsTable.getName()).thenReturn(dummyTblName);
    return mockFeFsTable;
  }

  private HdfsPartition createMockHdfsPartition(
      String path, FileSystemUtil.FsType fsType) {
    HdfsPartition mockHdfsPartition = mock(HdfsPartition.class);

    List<HdfsPartition.FileDescriptor> mockFilesDescs = new ArrayList<>();
    HdfsPartition.FileDescriptor mockFileDesc = mock(HdfsPartition.FileDescriptor.class);
    when(mockFileDesc.getFileLength()).thenReturn(1L);
    when(mockFileDesc.getRelativePath()).thenReturn("");
    when(mockFileDesc.getPath()).thenReturn("");
    mockFilesDescs.add(mockFileDesc);

    when(mockHdfsPartition.getLocationPath())
        .thenReturn(new org.apache.hadoop.fs.Path(path));
    when(mockHdfsPartition.getLocation()).thenReturn(path);
    when(mockHdfsPartition.getFileDescriptors()).thenReturn(mockFilesDescs);
    when(mockHdfsPartition.getFileFormat()).thenReturn(HdfsFileFormat.PARQUET);
    when(mockHdfsPartition.getFsType()).thenReturn(fsType);
    return mockHdfsPartition;
  }

  private Analyzer createMockAnalyzer() {
    Analyzer mockAnalyer = mock(Analyzer.class);

    TQueryCtx mockQueryCtx = mock(TQueryCtx.class);
    TClientRequest tClientRequest = mock(TClientRequest.class);
    when(tClientRequest.getQuery_options()).thenReturn(mock(TQueryOptions.class));
    mockQueryCtx.client_request = tClientRequest;

    DescriptorTable mockDescriptorTable = mock(DescriptorTable.class);
    when(mockDescriptorTable.getTupleDesc(any())).thenReturn(mock(TupleDescriptor.class));

    when(mockAnalyer.getQueryCtx()).thenReturn(mockQueryCtx);
    when(mockAnalyer.getDescTbl()).thenReturn(mock(DescriptorTable.class));
    when(mockAnalyer.getQueryOptions()).thenReturn(mock(TQueryOptions.class));
    when(mockAnalyer.getDescTbl()).thenReturn(mockDescriptorTable);
    when(mockAnalyer.getTupleDesc(any())).thenReturn(mock(TupleDescriptor.class));
    return mockAnalyer;
  }
}
