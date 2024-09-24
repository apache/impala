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
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;

public class IcebergContentFileStoreTest {
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private CatalogServiceCatalog catalog_;

  @Before
  public void init() {
    catalog_ = CatalogServiceTestCatalog.create();
  }

  @After
  public void cleanUp() { catalog_.close(); }

  @Test
  public void testDecodeWithoutIcebergMetadata() {
    expectedException.expect(NullPointerException.class);

    FileDescriptor fileDesc = new FileDescriptor(createFbFileDesc(),null);

    IcebergContentFileStore.decode(IcebergContentFileStore.encode(fileDesc));
  }

  private FbFileDesc createFbFileDesc() {
    FlatBufferBuilder fbb = new FlatBufferBuilder(1);
    fbb.finish(
        FbFileDesc.createFbFileDesc(fbb, 0, 10L, (byte) 0, 10000L, 0, false, 0, false));
    ByteBuffer bb = fbb.dataBuffer().slice();
    ByteBuffer compressedBb = ByteBuffer.allocate(bb.capacity());
    compressedBb.put(bb);
    return FbFileDesc.getRootAsFbFileDesc((ByteBuffer) compressedBb.flip());
  }

  @Test
  public void testEncodeDecode() throws Exception {
    IcebergTable iceTbl = loadIcebergTable(
        "functional_parquet",
        "iceberg_v2_partitioned_position_deletes_orc");
    assertTrue(iceTbl.getContentFileStore().getNumFiles() > 0);

    for (FileDescriptor fileDesc : iceTbl.getContentFileStore().getAllFiles()) {
      FileDescriptor serdeFileDesc =
          IcebergContentFileStore.decode(IcebergContentFileStore.encode(fileDesc));
      assertTrue(fileDesc != serdeFileDesc);
      assertEquals(serdeFileDesc, fileDesc);
    }
  }

  // Verify that the underlying representation of the file descriptors are shared between
  // the Iceberg table and the HdfsTable within the Iceberg table. In other words this
  // checks that storing the file descriptors also in both places doesn't result in
  // redundant JVM memory usage.
  @Test
  public void testFileDescriptorsAreShared() throws Exception {
    IcebergTable iceTbl = loadIcebergTable(
        "functional_parquet",
        "iceberg_v2_partitioned_position_deletes");
    assertTrue(iceTbl.getContentFileStore().getNumFiles() > 0);

    Map<String, FileDescriptor> fileDescsByPath = new HashMap<>();
    for (FileDescriptor fileDesc : iceTbl.getContentFileStore().getAllFiles()) {
      fileDescsByPath.put(fileDesc.getRelativePath(), fileDesc);
    }

    assertTrue(iceTbl.getFeFsTable() instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) iceTbl.getFeFsTable();
    for (PrunablePartition partition : hdfsTable.getPartitions()) {
      assertTrue(partition instanceof HdfsPartition);
      HdfsPartition hdfsPartition = (HdfsPartition) partition;
      for (FileDescriptor hdfsFileDesc : hdfsPartition.getFileDescriptors()) {
        FileDescriptor iceFileDesc = fileDescsByPath.get(hdfsFileDesc.getRelativePath());
        assertNotNull(iceFileDesc);
        assertNotSame(iceFileDesc, hdfsFileDesc);
        assertEquals(iceFileDesc.getFbFileDescriptor().getByteBuffer().array(),
            hdfsFileDesc.getFbFileDescriptor().getByteBuffer().array());
      }
    }
  }

  private IcebergTable loadIcebergTable(String dbName, String tblName) throws Exception {
    Table tbl = catalog_.getOrLoadTable(dbName, tblName,"test",null);
    assertTrue(tbl instanceof IcebergTable);
    IcebergTable iceTbl = (IcebergTable) tbl;
    assertNotNull(iceTbl.getContentFileStore());
    return iceTbl;
  }
}
