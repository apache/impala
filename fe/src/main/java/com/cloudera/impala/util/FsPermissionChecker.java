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
 * Singleton class that can check whether the current user has permission to access paths
 * in a FileSystem.
 */
public class FsPermissionChecker {
  private final static FsPermissionChecker instance_;
  protected final String user_;
  private final Set<String> groups_ = new HashSet<String>();

  static {
    try {
      instance_ = new FsPermissionChecker();
    } catch (IOException e) {
      throw new RuntimeException(
          "Error initializing FsPermissionChecker: " + e.getMessage(), e);
    }
  }

  private FsPermissionChecker() throws IOException {
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    groups_.addAll(Arrays.asList(ugi.getGroupNames()));
    user_ = ugi.getShortUserName();
  }

  /**
   * Allows checking different access permissions of a file without repeatedly accessing
   * the underlying filesystem by caching the results of a status call at construction.
   */
  public class Permissions {
    private final FileStatus fileStatus_;
    private final FsPermission permissions_;

    protected Permissions(FileStatus fileStatus) {
      Preconditions.checkNotNull(fileStatus);
      fileStatus_ = fileStatus;
      permissions_ = fileStatus.getPermission();
    }

    /**
     * Returns true if the current user can perform the given action given these
     * permissions.
     */
    public boolean checkPermissions(FsAction action) {
      // Check user, group and then 'other' permissions in turn.
      if (FsPermissionChecker.this.user_.equals(fileStatus_.getOwner())) {
        // If the user matches, we must return their access rights whether or not the user
        // is allowed to access without checking the group. This is counter-intuitive if
        // the user cannot access the file, but the group permissions would allow it, but
        // is consistent with UNIX behaviour.
        return permissions_.getUserAction().implies(action);
      }

      if (FsPermissionChecker.this.groups_.contains(fileStatus_.getGroup())) {
        return permissions_.getGroupAction().implies(action);
      }
      return permissions_.getOtherAction().implies(action);
    }

    public boolean canRead() { return checkPermissions(FsAction.READ); }
    public boolean canWrite() { return checkPermissions(FsAction.WRITE); }
    public boolean canReadAndWrite() { return canRead() && canWrite(); }
  }

  /**
   * Returns a Permissions object that can answer all access permission queries for the
   * given path.
   */
  public Permissions getPermissions(FileSystem fs, Path path) throws IOException {
    Preconditions.checkNotNull(fs);
    Preconditions.checkNotNull(path);
    return new Permissions(fs.getFileStatus(path));
  }

  /**
   * Returns the FsPermissionChecker singleton.
   */
  public static FsPermissionChecker getInstance() { return instance_; }
}
