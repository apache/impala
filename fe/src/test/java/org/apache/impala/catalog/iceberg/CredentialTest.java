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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.impala.common.Credential;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Tests for Credential: the extraction of the credentials vended into a table's FileIO
 * (used by IcebergMetaProvider after loading a table from the REST catalog), the
 * translation of the Iceberg credential properties to Hadoop config keys, the expiration
 * time of a credential and its identity. */
public class CredentialTest {

  interface CredentialFileIO extends org.apache.iceberg.io.FileIO,
      org.apache.iceberg.io.SupportsStorageCredentials {}

  private static CredentialFileIO credentialIOReturning(
      List<org.apache.iceberg.io.StorageCredential> creds) {
    CredentialFileIO io = mock(CredentialFileIO.class);
    when(io.credentials()).thenReturn(creds);
    return io;
  }

  private static org.apache.iceberg.io.StorageCredential storageCredential(
      String prefix, Map<String, String> config) {
    return org.apache.iceberg.io.StorageCredential.create(prefix, config);
  }

  private static Credential credential(String prefix, Map<String, String> config) {
    return new Credential(prefix, config);
  }

  @Test
  public void testExtractCredentialsWithNullListReturnsEmpty() {
    CredentialFileIO io = credentialIOReturning(null);
    List<Credential> creds = Credential.extract(io);
    assertTrue(creds.isEmpty());
  }

  @Test
  public void testExtractCredentialsWithSingleCredential() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.secret-access-key", "SECRET");
    config.put("s3.session-token", "TOKEN");
    CredentialFileIO io = credentialIOReturning(
        Collections.singletonList(storageCredential("s3://bucket/warehouse/", config)));

    List<Credential> creds = Credential.extract(io);

    assertEquals(1, creds.size());
    assertEquals("s3://bucket/warehouse/", creds.get(0).getPrefix());
    assertEquals("AKID", creds.get(0).getConfig().get("s3.access-key-id"));
    assertEquals("SECRET", creds.get(0).getConfig().get("s3.secret-access-key"));
    assertEquals("TOKEN", creds.get(0).getConfig().get("s3.session-token"));
  }

  @Test
  public void testExtractCredentialsWithMultipleCredentialsPreservesOrder() {
    CredentialFileIO io = credentialIOReturning(Arrays.asList(
        storageCredential("s3://bucket-a/",
            Collections.singletonMap("s3.access-key-id", "AKID1")),
        storageCredential("s3://bucket-b/",
            Collections.singletonMap("s3.access-key-id", "AKID2"))));

    List<Credential> creds = Credential.extract(io);

    assertEquals(2, creds.size());
    assertEquals("s3://bucket-a/", creds.get(0).getPrefix());
    assertEquals("AKID1", creds.get(0).getConfig().get("s3.access-key-id"));
    assertEquals("s3://bucket-b/", creds.get(1).getPrefix());
    assertEquals("AKID2", creds.get(1).getConfig().get("s3.access-key-id"));
  }

  @Test
  public void testToHadoopConfigTranslatesS3SessionCredentials() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.secret-access-key", "SECRET");
    config.put("s3.session-token", "TOKEN");

    Map<String, String> hadoopConfig =
        credential("s3://bucket/warehouse/", config).toHadoopConfig();

    assertEquals("AKID", hadoopConfig.get("fs.s3a.access.key"));
    assertEquals("SECRET", hadoopConfig.get("fs.s3a.secret.key"));
    assertEquals("TOKEN", hadoopConfig.get("fs.s3a.session.token"));
    assertEquals("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        hadoopConfig.get("fs.s3a.aws.credentials.provider"));
  }

  @Test
  public void testToHadoopConfigWithoutSessionTokenKeepsDefaultProvider() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.secret-access-key", "SECRET");

    // TemporaryAWSCredentialsProvider requires a session token, it must not be selected
    // for plain access/secret keys.
    Map<String, String> hadoopConfig =
        credential("s3a://bucket/warehouse/", config).toHadoopConfig();

    assertEquals("AKID", hadoopConfig.get("fs.s3a.access.key"));
    assertEquals("SECRET", hadoopConfig.get("fs.s3a.secret.key"));
    assertNull(hadoopConfig.get("fs.s3a.aws.credentials.provider"));
  }

  @Test
  public void testToHadoopConfigDropsUntranslatedKeys() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    // Neither the expiry nor an unknown property is meant for the Hadoop config.
    config.put("s3.session-token-expires-at-ms", "1700000000000");
    config.put("s3.some-unknown-property", "value");

    Map<String, String> hadoopConfig =
        credential("s3://bucket/warehouse/", config).toHadoopConfig();

    assertEquals(Collections.singletonMap("fs.s3a.access.key", "AKID"), hadoopConfig);
  }

  @Test
  public void testToHadoopConfigIsEmptyForUnsupportedScheme() {
    Map<String, String> config =
        Collections.singletonMap("s3.access-key-id", "AKID");

    assertTrue(credential("hdfs://ns/warehouse/", config).toHadoopConfig().isEmpty());
    assertTrue(credential("no-scheme", config).toHadoopConfig().isEmpty());
  }

  @Test
  public void testGetExpiryMs() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.session-token", "TOKEN");
    config.put("s3.session-token-expires-at-ms", " 1700000000000 ");

    assertEquals(1700000000000L,
        credential("s3://bucket/warehouse/", config).getExpiryMs());
  }

  @Test
  public void testGetExpiryMsIsZeroWhenUnavailable() {
    // Credential without an expiration time, e.g. static keys.
    assertEquals(0L, credential("s3://bucket/warehouse/",
        Collections.singletonMap("s3.access-key-id", "AKID")).getExpiryMs());
    // Unparsable expiration time.
    assertEquals(0L, credential("s3://bucket/warehouse/",
        Collections.singletonMap("s3.session-token-expires-at-ms", "not-a-number"))
            .getExpiryMs());
    // Unsupported scheme.
    assertEquals(0L, credential("hdfs://ns/warehouse/",
        Collections.singletonMap("s3.session-token-expires-at-ms", "1700000000000"))
            .getExpiryMs());
  }

  @Test
  public void testIdentityIsStableForTheSameCredential() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.session-token", "TOKEN");
    Credential cred = credential("s3://bucket/warehouse/", config);

    assertEquals(cred.identity(), cred.identity());
    assertEquals(cred.identity(),
        credential("s3://bucket/warehouse/", new HashMap<>(config)).identity());
  }

  @Test
  public void testIdentityChangesWhenTheCredentialIsRotated() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.session-token", "TOKEN");
    Map<String, String> rotatedConfig = new HashMap<>(config);
    rotatedConfig.put("s3.session-token", "NEW_TOKEN");

    assertNotEquals(credential("s3://bucket/warehouse/", config).identity(),
        credential("s3://bucket/warehouse/", rotatedConfig).identity());
  }

  @Test
  public void testIdentityDoesNotExposeCredentialValues() {
    Map<String, String> config = new HashMap<>();
    config.put("s3.access-key-id", "AKID");
    config.put("s3.secret-access-key", "SECRET");

    String identity = credential("s3://bucket/warehouse/", config).identity();

    assertFalse(identity.contains("AKID"));
    assertFalse(identity.contains("SECRET"));
  }
}
