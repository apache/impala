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

package org.apache.impala.common;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.io.SupportsStorageCredentials;
import org.apache.impala.catalog.iceberg.translate.CredentialScheme;
import org.apache.impala.thrift.TCredential;

/**
 * A storage credential: a storage prefix plus a config map.
 * The Impala backend reads storage through the Hadoop FileSystem layer and therefore
 * understands only Hadoop-native config keys. toHadoopConfig()/toThrift() translate any
 * key to their Hadoop equivalents. The per-scheme translation config lives in
 * {@link CredentialScheme}, selected from the credential prefix, so supporting a new
 * storage system is a matter of adding a scheme there rather than new branching logic.
 *
 * Sensitive values must not be logged. toString() omits config values.
 */
public class Credential {
  private final String prefix_;
  private final ImmutableMap<String, String> config_;
  // The scheme selected from prefix_, or null if the scheme is unsupported.
  private final CredentialScheme scheme_;

  // Derived, rebuildable state, computed once on first use.
  private ImmutableMap<String, String> hadoopConfig_;
  private String identity_;

  public Credential(String prefix, Map<String, String> config) {
    Preconditions.checkNotNull(prefix, "prefix must not be null");
    Preconditions.checkNotNull(config, "config must not be null");
    prefix_ = prefix;
    config_ = ImmutableMap.copyOf(config);
    scheme_ = CredentialScheme.forPrefix(prefix);
  }

  /**
   * Extracts the storage credentials vended into a table's FileIO. Returns an empty list
   * when the FileIO exposes no vended credentials.
   */
  public static List<Credential> extract(FileIO io) {
    if (!(io instanceof SupportsStorageCredentials)) {
      return Collections.emptyList();
    }
    List<StorageCredential> storageCreds =
        ((SupportsStorageCredentials) io).credentials();
    if (storageCreds == null || storageCreds.isEmpty()) {
      return Collections.emptyList();
    }
    List<Credential> result = new ArrayList<>(storageCreds.size());
    for (StorageCredential cred : storageCreds) {
      result.add(new Credential(cred.prefix(), cred.config()));
    }
    return result;
  }

  /** Storage location prefix this credential applies to (e.g. "s3://bucket/"). */
  public String getPrefix() { return prefix_; }

  /**
   * Raw credential configuration map in Iceberg key convention.
   * Use toHadoopConfig() to obtain the Hadoop-translated map for backend consumption.
   */
  public ImmutableMap<String, String> getConfig() { return config_; }

  /**
   * Absolute expiry time of this credential as epoch milliseconds, taken from the
   * scheme's Iceberg expiry property (e.g. 's3.session-token-expires-at-ms'). Returns 0
   * when the scheme has no expiry, the credential carries no expiry (e.g. static keys),
   * or the value cannot be parsed.
   */
  public long getExpiryMs() {
    return scheme_ == null ? 0L : scheme_.expiryMs(config_);
  }

  /**
   * Translates the Iceberg credential keys to their Hadoop equivalents for the backend
   * IO layer, using the CredentialScheme selected for the credential's prefix. Returns
   * an empty map for an unsupported scheme or a credential that yields no Hadoop keys.
   */
  public Map<String, String> toHadoopConfig() {
    if (hadoopConfig_ != null) return hadoopConfig_;
    Map<String, String> result =
        scheme_ != null ? scheme_.translate(config_) : ImmutableMap.of();
    hadoopConfig_ = ImmutableMap.copyOf(result);
    return hadoopConfig_;
  }

  /**
   * A stable discriminator for the Hadoop-translated configuration, so a rotated
   * credential yields a distinct value without exposing secrets. Computed as a SHA-256
   * digest over the config entries in sorted key order, so the value is deterministic
   * across processes and does not leak the underlying credential material.
   */
  public String identity() {
    if (identity_ != null) return identity_;
    Hasher hasher = Hashing.sha256().newHasher();
    // Sort by key so the digest is independent of map iteration order.
    for (Map.Entry<String, String> kv : new TreeMap<>(toHadoopConfig()).entrySet()) {
      hasher.putString(kv.getKey(), StandardCharsets.UTF_8)
          .putByte((byte) 0)
          .putString(kv.getValue(), StandardCharsets.UTF_8)
          .putByte((byte) 0);
    }
    identity_ = hasher.hash().toString();
    return identity_;
  }

  /** Serializes to the Thrift wire form for the backend (Hadoop-native config keys). */
  public TCredential toThrift() {
    TCredential tCred = new TCredential(prefix_, toHadoopConfig());
    long expiryMs = getExpiryMs();
    if (expiryMs > 0) tCred.setExpires_at_ms(expiryMs);
    return tCred;
  }

  @Override
  public String toString() {
    // Deliberately omit config values to avoid leaking credentials in logs.
    return String.format("Credential{prefix=%s, config.keys=%s}",
        prefix_, config_.keySet());
  }
}
