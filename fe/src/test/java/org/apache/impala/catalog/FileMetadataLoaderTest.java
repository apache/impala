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
import org.apache.hadoop.fs.Path;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.junit.Test;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;


public class FileMetadataLoaderTest {

  @Test
  public void testRecursiveLoading() throws IOException {
    ListMap<TNetworkAddress> hostIndex = new ListMap<>();
    Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/alltypes/");
    FileMetadataLoader fml = new FileMetadataLoader(tablePath, /* recursive=*/true,
        /* oldFds = */Collections.emptyList(), hostIndex);
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
        /* oldFds = */fml.getLoadedFds(), hostIndex);
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
        /* oldFds = */fml.getLoadedFds(), hostIndex);
    refreshFml.load();
    assertEquals(1, refreshFml.getStats().loadedFiles);
  }

  @Test
  public void testLoadMissingDirectory() throws IOException {
    for (boolean recursive : ImmutableList.of(false, true)) {
      ListMap<TNetworkAddress> hostIndex = new ListMap<>();
      Path tablePath = new Path("hdfs://localhost:20500/test-warehouse/does-not-exist/");
      FileMetadataLoader fml = new FileMetadataLoader(tablePath, recursive,
          /* oldFds = */Collections.emptyList(), hostIndex);
      fml.load();
      assertEquals(0, fml.getLoadedFds().size());
    }
  }
}
