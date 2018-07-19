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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.impala.util.FsPermissionChecker.Permissions;

/**
 * Simple non-thread-safe cache for resolved file permissions. This allows
 * pre-caching permissions by listing the status of all files within a directory,
 * and then using that cache to avoid round trips to the FileSystem for later
 * queries of those paths.
 */
public class FsPermissionCache {
  private static Configuration CONF = new Configuration();
  private Map<Path, Permissions> cache_ = new HashMap<>();

  public Permissions getPermissions(Path location) throws IOException {
    Permissions perms = cache_.get(location);
    if (perms != null) return perms;
    FsPermissionChecker checker = FsPermissionChecker.getInstance();
    FileSystem fs = location.getFileSystem(CONF);
    perms = checker.getPermissions(fs, location);
    cache_.put(location, perms);
    return perms;
  }

  public void precacheChildrenOf(FileSystem fs, Path p)
      throws FileNotFoundException, IOException {
    FsPermissionChecker checker = FsPermissionChecker.getInstance();
    RemoteIterator<FileStatus> iter = fs.listStatusIterator(p);
    while (iter.hasNext()) {
      FileStatus status = iter.next();
      Permissions perms = checker.getPermissions(fs, status);
      cache_.put(status.getPath(), perms);
    }
  }
}
