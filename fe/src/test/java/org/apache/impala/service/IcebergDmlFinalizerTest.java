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

package org.apache.impala.service;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.flatbuffers.FlatBufferBuilder;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.fb.FbIcebergDataFile;
import org.apache.impala.thrift.TIcebergOperation;
import org.apache.impala.thrift.TIcebergOperationParam;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.InOrder;

public class IcebergDmlFinalizerTest {
  @Rule
  public TemporaryFolder temporaryFolder_ = new TemporaryFolder();

  private FeIcebergTable table_;
  private Transaction transaction_;
  private AppendFiles append_;
  private TIcebergOperationParam operation_;

  @Before
  public void setUp() {
    table_ = mock(FeIcebergTable.class);
    transaction_ = mock(Transaction.class);
    append_ = mock(AppendFiles.class);
    when(transaction_.newAppend()).thenReturn(append_);

    operation_ = new TIcebergOperationParam();
    operation_.setOperation(TIcebergOperation.INSERT);
    operation_.setIs_overwrite(false);
    operation_.setIceberg_data_files_fb(Collections.emptyList());
  }

  @Test
  public void testOperationHookAndTransactionCommitOrder() throws Exception {
    IcebergDmlFinalizer.PostOperationHook hook =
        mock(IcebergDmlFinalizer.PostOperationHook.class);

    IcebergDmlFinalizer.finalizeDml(
        table_, transaction_, operation_, null, hook);

    InOrder order = inOrder(append_, hook, transaction_);
    order.verify(append_).commit();
    order.verify(hook).run();
    order.verify(transaction_).commitTransaction();
  }

  @Test
  public void testKnownFailureCleansUpFiles() throws Exception {
    File uncommittedFile = addUncommittedFile();
    RuntimeException failure = new RuntimeException("known failure");
    doThrow(failure).when(transaction_).commitTransaction();

    assertWrappedFailure(failure);
    assertFalse(uncommittedFile.exists());
  }

  @Test
  public void testUnknownCommitStateDoesNotCleanUpFiles() throws Exception {
    File uncommittedFile = addUncommittedFile();
    CommitStateUnknownException failure = new CommitStateUnknownException(
        "unknown state", new RuntimeException("unknown state cause"));
    doThrow(failure).when(transaction_).commitTransaction();

    assertWrappedFailure(failure);
    verify(transaction_).commitTransaction();
    assertTrue(uncommittedFile.exists());
  }

  @Test
  public void testHookFailureCleansUpFilesWithoutCommitting() throws Exception {
    File uncommittedFile = addUncommittedFile();
    RuntimeException failure = new RuntimeException("hook failure");
    IcebergDmlFinalizer.PostOperationHook hook =
        mock(IcebergDmlFinalizer.PostOperationHook.class);
    doThrow(failure).when(hook).run();

    try {
      IcebergDmlFinalizer.finalizeDml(
          table_, transaction_, operation_, null, hook);
      fail("Expected Iceberg DML finalization to fail");
    } catch (ImpalaRuntimeException e) {
      assertSame(failure, e.getCause());
    }
    verify(transaction_, never()).commitTransaction();
    assertFalse(uncommittedFile.exists());
  }

  private void assertWrappedFailure(RuntimeException failure) {
    try {
      IcebergDmlFinalizer.finalizeDml(table_, transaction_, operation_, null);
      fail("Expected Iceberg DML finalization to fail");
    } catch (ImpalaRuntimeException e) {
      assertSame(failure, e.getCause());
    }
  }

  private File addUncommittedFile() throws Exception {
    File file = temporaryFolder_.newFile();
    FlatBufferBuilder fbb = new FlatBufferBuilder(128);
    int path = fbb.createString(file.toURI().toString());
    FbIcebergDataFile.startFbIcebergDataFile(fbb);
    FbIcebergDataFile.addPath(fbb, path);
    int dataFile = FbIcebergDataFile.endFbIcebergDataFile(fbb);
    fbb.finish(dataFile);
    ByteBuffer buffer = fbb.dataBuffer().slice();
    operation_.setIceberg_delete_files_fb(Collections.singletonList(buffer));
    return file;
  }
}
