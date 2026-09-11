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

package org.apache.impala.catalog.iceberg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for VendedCredentialsFileIO that need no object store: the operations it offers
 * and its behaviour for the paths that no vended credential covers.
 */
public class VendedCredentialsFileIOTest {

  @Rule
  public TemporaryFolder tmpDir_ = new TemporaryFolder();

  private static VendedCredentialsFileIO createFileIO(
      List<StorageCredential> credentials) {
    VendedCredentialsFileIO io = new VendedCredentialsFileIO();
    io.setConf(new Configuration());
    io.initialize(Collections.emptyMap());
    io.setCredentials(credentials);
    return io;
  }

  private static List<StorageCredential> s3Credentials() {
    return Collections.singletonList(StorageCredential.create(
        "s3a://bucket/warehouse/db/tbl",
        ImmutableMap.of("s3.access-key-id", "key", "s3.secret-access-key", "secret")));
  }

  private String writeFile(VendedCredentialsFileIO io, String name, String content)
      throws IOException {
    String path = new File(tmpDir_.getRoot(), name).toURI().toString();
    try (PositionOutputStream out = io.newOutputFile(path).create()) {
      out.write(content.getBytes(StandardCharsets.UTF_8));
    }
    return path;
  }

  /**
   * A bulk or prefix operation that is passed on to the plain HadoopFileIO would not use
   * the vended credentials, so the class must not offer them: Iceberg then uses the
   * single-file operations, which do.
   */
  @Test
  public void testNoBulkAndPrefixOperations() {
    VendedCredentialsFileIO io = createFileIO(s3Credentials());
    assertFalse(io instanceof SupportsBulkOperations);
    assertFalse(io instanceof SupportsPrefixOperations);
    assertEquals(1, io.credentials().size());
  }

  /** Paths that no credential covers are read and written like with HadoopFileIO. */
  @Test
  public void testPathsWithoutCredential() throws IOException {
    for (List<StorageCredential> credentials :
        ImmutableList.of(Collections.<StorageCredential>emptyList(), s3Credentials())) {
      VendedCredentialsFileIO io = createFileIO(credentials);
      String path = writeFile(io, "f-" + credentials.size(), "content");

      InputFile in = io.newInputFile(path);
      assertTrue(in.exists());
      assertEquals(7, in.getLength());
      byte[] buf = new byte[7];
      try (SeekableInputStream stream = io.newInputFile(path, 7).newStream()) {
        assertEquals(7, stream.read(buf, 0, 7));
      }
      assertEquals("content", new String(buf, StandardCharsets.UTF_8));

      io.deleteFile(path);
      assertFalse(io.newInputFile(path).exists());
    }
    // No FileSystem is created for a credential that covers none of the paths.
    assertEquals(0, VendedCredentialsFileIO.cachedFileSystemCount());
  }

  /**
   * Iceberg's cleanup of the files of a failed commit (CatalogUtil.deleteFiles(), used
   * by BaseTransaction) deletes the files one by one through deleteFile().
   */
  @Test
  public void testIcebergFileCleanup() throws IOException {
    VendedCredentialsFileIO io = createFileIO(s3Credentials());
    List<String> paths = new ArrayList<>();
    for (int i = 0; i < 3; ++i) paths.add(writeFile(io, "manifest-" + i, "m"));

    CatalogUtil.deleteFiles(io, paths, "manifest", /*concurrent*/ false);
    for (String path : paths) assertFalse(io.newInputFile(path).exists());
  }
}
