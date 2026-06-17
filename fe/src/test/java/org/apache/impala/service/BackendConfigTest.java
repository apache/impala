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

import org.apache.impala.thrift.TBackendGflags;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BackendConfigTest {
  private BackendConfig configWith(String trustedJarPaths) {
    TBackendGflags gflags = new TBackendGflags();
    gflags.trusted_jar_paths = trustedJarPaths;
    return new BackendConfig(gflags);
  }

  // --- isJarPathAllowed ---

  @Test
  public void testIsJarPathAllowed_emptyAllowlist_returnsFalse() {
    BackendConfig cfg = configWith("");
    assertFalse(cfg.isJarPathAllowed("/trusted/path/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_nullAllowlist_returnsFalse() {
    BackendConfig cfg = configWith(null);
    assertFalse(cfg.isJarPathAllowed("/trusted/path/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_nullPath_returnsFalse() {
    BackendConfig cfg = configWith("/trusted/");
    assertFalse(cfg.isJarPathAllowed(null));
  }

  @Test
  public void testIsJarPathAllowed_emptyPath_returnsFalse() {
    BackendConfig cfg = configWith("/trusted/");
    assertFalse(cfg.isJarPathAllowed(""));
  }

  @Test
  public void testIsJarPathAllowed_exactPrefixMatch_returnsTrue() {
    BackendConfig cfg = configWith("/trusted/");
    assertTrue(cfg.isJarPathAllowed("/trusted/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_pathEqualsPrefix_returnsTrue() {
    BackendConfig cfg = configWith("/trusted/path");
    assertTrue(cfg.isJarPathAllowed("/trusted/path"));
  }

  @Test
  public void testIsJarPathAllowed_noMatchingPrefix_returnsFalse() {
    BackendConfig cfg = configWith("/trusted/");
    assertFalse(cfg.isJarPathAllowed("/untrusted/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_multipleAllowlistEntries_firstMatches_returnsTrue() {
    BackendConfig cfg = configWith("/trusted/,/other/path/");
    assertTrue(cfg.isJarPathAllowed("/trusted/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_multipleAllowlistEntries_secondMatches_returnsTrue() {
    BackendConfig cfg = configWith("/trusted/,/other/path/");
    assertTrue(cfg.isJarPathAllowed("/other/path/bar.jar"));
  }

  @Test
  public void testIsJarPathAllowed_multipleAllowlistEntries_noneMatch_returnsFalse() {
    BackendConfig cfg = configWith("/trusted/,/other/path/");
    assertFalse(cfg.isJarPathAllowed("/somewhere/else/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_allowlistWithWhitespace_trimsAndMatches() {
    BackendConfig cfg = configWith("  /trusted/  ,  /other/path/  ");
    assertTrue(cfg.isJarPathAllowed("/trusted/foo.jar"));
    assertTrue(cfg.isJarPathAllowed("/other/path/bar.jar"));
  }

  @Test
  public void testIsJarPathAllowed_pathContainsPrefixDoesNotStartWithIt_returnsFalse() {
    BackendConfig cfg = configWith("/trusted/");
    assertFalse(cfg.isJarPathAllowed("/not/trusted/foo.jar"));
  }

  @Test
  public void testIsJarPathAllowed_allowlistWithBlankEntry_ignoresBlank() {
    // A comma-only allowlist produces one blank entry, which should not match anything.
    BackendConfig cfg = configWith(",");
    assertFalse(cfg.isJarPathAllowed("/trusted/foo.jar"));
  }
}
