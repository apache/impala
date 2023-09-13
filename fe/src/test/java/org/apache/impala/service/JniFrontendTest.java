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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TStringLiteral;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class JniFrontendTest {
  private static TBackendGflags origFlags;

  @BeforeClass
  public static void setup() {
    // The original BackendConfig need to be mocked, we are saving the values here, so
    // they can be restored and not break other tests
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  @Test
  public void testCheckGroupsMappingProvider() throws ImpalaException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        JniBasedUnixGroupsMappingWithFallback.class.getName());
    assertTrue(JniFrontend.checkGroupsMappingProvider(conf).isEmpty());

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsMapping.class.getName());
    assertEquals(JniFrontend.checkGroupsMappingProvider(conf),
        String.format("Hadoop groups mapping provider: %s is known to be problematic. " +
            "Consider using: %s instead.",
            ShellBasedUnixGroupsMapping.class.getName(),
            JniBasedUnixGroupsMappingWithFallback.class.getName()));

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsNetgroupMapping.class.getName());
    assertEquals(JniFrontend.checkGroupsMappingProvider(conf),
        String.format("Hadoop groups mapping provider: %s is known to be problematic. " +
            "Consider using: %s instead.",
            ShellBasedUnixGroupsNetgroupMapping.class.getName(),
            JniBasedUnixGroupsNetgroupMappingWithFallback.class.getName()));
  }

  /**
   * This test verifies whether short-circuit socket path parent directory configurations
   * are skipped for coordinator-only mode instances and checked for executors. A socket
   * directory is created in order to mock the file access.
   */
  @Test
  public void testCheckShortCircuitConfigs() {
    String tmpDir = System.getProperty("java.io.tmpdir", "/tmp");
    File socketDir = new File(tmpDir + "/socketTest",
        "socks." + (System.currentTimeMillis() + "." + (new Random().nextInt())));
    socketDir.mkdirs();
    socketDir.getParentFile().setExecutable(false);

    Configuration conf = mock(Configuration.class);
    when(conf.getBoolean(HdfsClientConfigKeys.Read.ShortCircuit.KEY,
        HdfsClientConfigKeys.Read.ShortCircuit.DEFAULT)).thenReturn(true);
    when(conf.getTrimmed(HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY,
        HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_DEFAULT))
        .thenReturn(socketDir.getAbsolutePath());
    when(conf.getBoolean(HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL,
             HdfsClientConfigKeys.DFS_CLIENT_USE_LEGACY_BLOCKREADERLOCAL_DEFAULT))
        .thenReturn(false);
    BackendConfig.INSTANCE = mock(BackendConfig.class);

    when(BackendConfig.INSTANCE.isDedicatedCoordinator()).thenReturn(true);
    String actualErrorMessage = JniFrontend.checkShortCircuitRead(conf);
    assertEquals("", actualErrorMessage);

    when(BackendConfig.INSTANCE.isDedicatedCoordinator()).thenReturn(false);
    actualErrorMessage = JniFrontend.checkShortCircuitRead(conf);
    assertEquals("Invalid short-circuit reads configuration:\n"
        + "  - Impala cannot read or execute the parent directory of "
        + HdfsClientConfigKeys.DFS_DOMAIN_SOCKET_PATH_KEY + "\n",
        actualErrorMessage);

    if (socketDir != null) {
      socketDir.getParentFile().setExecutable(true);
      socketDir.delete();
      socketDir.getParentFile().delete();
    }
  }

  /**
   * This test validates that JniFrontend::getSecretFromKeyStore function can return
   * the secret from the configured Jceks KeyStore
   */
  @Test
  public void testGetSecretFromKeyStore() throws ImpalaException {
    // valid secret-key returns the correct secret
    TStringLiteral secretKey = new TStringLiteral("openai-api-key-secret");
    byte[] secretKeyBytes = JniUtil.serializeToThrift(secretKey);
    String secret = JniFrontend.getSecretFromKeyStore(secretKeyBytes);
    assertEquals(secret, "secret");
    // invalid secret-key returns error
    secretKey = new TStringLiteral("dummy-secret");
    secretKeyBytes = JniUtil.serializeToThrift(secretKey);
    try {
      secret = JniFrontend.getSecretFromKeyStore(secretKeyBytes);
    } catch (InternalException e) {
      assertEquals(e.getMessage(),
          String.format(JniFrontend.KEYSTORE_ERROR_MSG, secretKey.getValue()));
    }
  }
}
