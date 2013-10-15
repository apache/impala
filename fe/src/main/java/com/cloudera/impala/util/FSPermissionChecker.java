// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.util;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.base.Preconditions;

/**
 * Singleton class that checks whether the current user has permission
 * to access FileSystem Paths.
 * This file is a modified version of class:
 * org.apache.hadoop.hdfs.server.namenode.FSPermissionChecker
 * that was simplified for Impala's use case and modified to meet our
 * coding standards.
 */
public class FSPermissionChecker {
  private final static FSPermissionChecker instance_;
  private final String user_;
  private final Set<String> groups_ = new HashSet<String>();

  static {
    try {
      instance_ = new FSPermissionChecker();
    } catch (IOException e) {
      throw new RuntimeException(
          "Error initializing the FSPermissionChecker: " + e.getMessage(), e);
    }
  }

  private FSPermissionChecker() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    groups_.addAll(Arrays.asList(ugi.getGroupNames()));
    user_ = ugi.getShortUserName();
  }

  /**
   * Checks whether the current user has permission to access the given Path at the
   * specified access level.
   */
  public boolean hasAccess(FileSystem fs, Path path, FsAction access)
      throws IOException {
    Preconditions.checkNotNull(path);
    Preconditions.checkNotNull(access);
    FileStatus fileStatus = fs.getFileStatus(path);
    FsPermission mode = fileStatus.getPermission();
    if (user_.equals(fileStatus.getOwner())) { //user class
      if (mode.getUserAction().implies(access)) { return true; }
    } else if (groups_.contains(fileStatus.getGroup())) { //group class
      if (mode.getGroupAction().implies(access)) { return true; }
    } else { //other class
      if (mode.getOtherAction().implies(access)) { return true; }
    }
    return false;
  }

  /*
   * Returns an instance of the FSPermissionChecker.
   */
  public static FSPermissionChecker getInstance() {
    return instance_;
  }
}