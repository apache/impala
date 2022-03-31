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


import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class FsPermissionCheckerTest {

  private final FsPermissionChecker checker = FsPermissionChecker.getInstance();

  @Test
  public void testCheckAccessWhenAuthorizationEnableAndFsPermissionDenied()
      throws IOException {
    Path path = new Path("hdfs://nameservice/user/impala/work");
    FileSystem fs = Mockito.mock(FileSystem.class);
    FsAction read = FsAction.READ;

    Mockito.doThrow(new AccessControlException()).when(fs).access(path, read);

    boolean access = checker.checkAccess(path, fs, read, true);
    Assert.assertFalse(access);
  }

  @Test
  public void testCheckAccessWhenAuthorizationEnableAndPermissionConfirmed()
      throws IOException {
    Path path = new Path("hdfs://nameservice/user/impala/work");
    FileSystem fs = Mockito.mock(FileSystem.class);
    FsAction read = FsAction.READ;

    Mockito.doNothing().when(fs).access(path, read);

    boolean access = checker.checkAccess(path, fs, read, true);
    Assert.assertTrue(access);
  }

  @Test
  public void testCheckAccessWhenAuthorizationDisableAndPermissionDenied()
      throws IOException {
    Path path = new Path("hdfs://nameservice/user/impala/work");
    FileSystem fs = Mockito.mock(FileSystem.class);
    FsAction read = FsAction.READ;

    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    Mockito.doReturn(fileStatus).when(fs).getFileStatus(path);
    FsPermission permission = FsPermission.valueOf("-r--r-----");
    Mockito.doReturn(permission).when(fileStatus).getPermission();

    boolean access = checker.checkAccess(path, fs, read, false);
    Assert.assertFalse(access);
  }

  @Test
  public void testCheckAccessWhenAuthorizationDisableAndPermissionConfirmed()
      throws IOException {
    Path path = new Path("hdfs://nameservice/user/impala/work");
    FileSystem fs = Mockito.mock(FileSystem.class);
    FsAction read = FsAction.READ;

    FileStatus fileStatus = Mockito.mock(FileStatus.class);
    Mockito.doReturn(fileStatus).when(fs).getFileStatus(path);
    FsPermission permission = FsPermission.valueOf("-r--r--r--");
    Mockito.doReturn(permission).when(fileStatus).getPermission();

    boolean access = checker.checkAccess(path, fs, read, false);
    Assert.assertTrue(access);
  }
}