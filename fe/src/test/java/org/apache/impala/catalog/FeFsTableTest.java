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

import com.google.flatbuffers.FlatBufferBuilder;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.fb.FbFileDesc;
import org.junit.Test;
import org.mockito.Mockito;

public class FeFsTableTest {

  private static FileDescriptor createFd(boolean isEc, byte ecPolicyId) {
    FlatBufferBuilder fbb = new FlatBufferBuilder(1);
    fbb.finish(FbFileDesc.createFbFileDesc(
        fbb, 0, 10L, (byte) 0, 10000L, 0, isEc, 0, false, ecPolicyId));
    ByteBuffer bb = fbb.dataBuffer().slice();
    ByteBuffer copy = ByteBuffer.allocate(bb.capacity());
    copy.put(bb);
    return new FileDescriptor(FbFileDesc.getRootAsFbFileDesc((ByteBuffer) copy.flip()));
  }

  private static FeFsPartition mockPartition(FileDescriptor... fds) {
    FeFsPartition p = Mockito.mock(FeFsPartition.class);
    Mockito.when(p.getFileDescriptors()).thenReturn(Arrays.asList(fds));
    Mockito.when(p.getLocationPath())
        .thenReturn(new Path("hdfs://localhost:20500/dummy"));
    return p;
  }

  /**
   * Tests the cases of FeFsTable.getErasureCodingPolicy() that are decided from the
   * file descriptors alone, without falling back to a filesystem lookup.
   */
  @Test
  public void testGetErasureCodingPolicy() {
    byte rs63 = SystemErasureCodingPolicies.RS_6_3_POLICY_ID;
    byte xor21 = SystemErasureCodingPolicies.XOR_2_1_POLICY_ID;
    // No erasure-coded files.
    assertEquals(FileSystemUtil.NO_ERASURE_CODE_LABEL, FeFsTable.getErasureCodingPolicy(
        mockPartition(createFd(false, (byte) 0), createFd(false, (byte) 0))));
    // All files share one policy, resolved locally by its id.
    assertEquals("RS-6-3-1024k", FeFsTable.getErasureCodingPolicy(
        mockPartition(createFd(true, rs63), createFd(true, rs63))));
    // Files with two different policies.
    assertEquals(FeFsTable.MIXED_ERASURE_CODE_LABEL, FeFsTable.getErasureCodingPolicy(
        mockPartition(createFd(true, rs63), createFd(true, xor21))));
    // A mix of erasure-coded and plain files.
    assertEquals(FeFsTable.MIXED_ERASURE_CODE_LABEL, FeFsTable.getErasureCodingPolicy(
        mockPartition(createFd(true, rs63), createFd(false, (byte) 0))));
    // A mix of plain files and an erasure-coded file whose policy id is unknown is
    // MIXED regardless of the unknown policy, in either file order.
    assertEquals(FeFsTable.MIXED_ERASURE_CODE_LABEL, FeFsTable.getErasureCodingPolicy(
        mockPartition(createFd(false, (byte) 0), createFd(true, (byte) 0))));
    assertEquals(FeFsTable.MIXED_ERASURE_CODE_LABEL, FeFsTable.getErasureCodingPolicy(
        mockPartition(createFd(true, (byte) 0), createFd(false, (byte) 0))));
  }
}
