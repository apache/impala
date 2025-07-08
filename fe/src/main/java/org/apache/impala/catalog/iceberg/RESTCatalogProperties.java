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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class to extract native Iceberg catalog properties from a Properties object
 * that is possibly created from a Trino connector file. The goal is that users can
 * just simply reuse their already existing Trino configurations with Impala.
 *
 * Iceberg REST Catalog and Trino can use different property names for the same
 * functionality.E.g.:
 * +-------------------+----------------------------------------+
 * |      Iceberg      |                 Trino                  |
 * +-------------------+----------------------------------------+
 * | uri               | iceberg.rest-catalog.uri               |
 * | warehouse         | iceberg.rest-catalog.warehouse         |
 * | oauth2-server-uri | iceberg.rest-catalog.oauth2.server-uri |
 * +-------------------+----------------------------------------+
 *
 * For a complete list check the followings:
 * Iceberg: CatalogProperties, OAuth2Properties
 * Trino: IcebergRestCatalogConfig, OAuth2SecurityConfig
 *
 * With this class Impala can recognize the alternative configuration names and translate
 * them to the Iceberg native ones. It also handles required properties like "uri" that
 * must be set.
 *
 * If we only support a single setting for a configuration option, we also verify their
 * values. E.g. 'vended-credentials-enabled' must be false, as Impala doesn't support
 * vended credentials yet.
 *
 * And some properties are simply ignored as they are specific to another query engine
 * (e.g. 'case-insensitive-name-matching.cache-ttl'), or they have different purposes than
 * configuring the REST catalog (e.g. 'connector.name').
 *
 * The remaining properties (that are not translated, verified, or ignored) don't
 * need special treatment and are simply returned as they are.
 */
public class RESTCatalogProperties {
  /**
   * Utility class for properties that can have alternative names.
   */
  private static class Config {
    protected String catalogKey;
    protected ImmutableList<String> alternativeKeys;

    public Config(String key) {
      this(key, ImmutableList.of());
    }

    public Config(String key, ImmutableList<String> alternativeKeys) {
      this.catalogKey = key;
      this.alternativeKeys = alternativeKeys;
    }

    public boolean applyConfig(
        Map<String, String> sourceMap, Map<String, String> outputMap) {
      verifyOutputMap(outputMap);

      boolean applied = false;
      String value = sourceMap.get(catalogKey);
      if (value != null) {
        applied = true;
        sourceMap.remove(catalogKey);
        outputMap.put(catalogKey, value);
      }
      // Even if already applied, check alternative keys for ambiguity.
      for (String alternativeKey : alternativeKeys) {
        value = sourceMap.get(alternativeKey);
        if (value != null) {
          if (applied) {
            throw new IllegalStateException(
                String.format("Alternative key '%s' sets the same configuration as " +
                    "'%s' which is already defined with value '%s'",
                    alternativeKey, catalogKey, value));
          }
          applied = true;
          sourceMap.remove(alternativeKey);
          // We still need to use 'catalogKey' for alternative keys.
          outputMap.put(catalogKey, value);
        }
      }
      return applied;
    }

    protected void verifyOutputMap(Map<String, String> outputMap) {
      String value = outputMap.get(catalogKey);
      if (value != null) {
        throw new IllegalStateException(
            String.format("REST Catalog property is defined multiple times: %s\n" +
                "Current value: %s", catalogKey, value));
      }
    }
  }

  /**
   * Config that must be present. Currently only 'URI'.
   */
  private static class RequiredConfig extends Config {
    public RequiredConfig(String key) {
      super(key, ImmutableList.of());
    }

    public RequiredConfig(String key, ImmutableList<String> alternativeKeys) {
      super(key, alternativeKeys);
    }

    @Override
    public boolean applyConfig(
        Map<String, String> sourceMap, Map<String, String> outputMap) {
      boolean success = super.applyConfig(sourceMap, outputMap);
      if (success) return true;
      throw new IllegalStateException(
          String.format("Missing property of IcebergRESTCatalog: %s", catalogKey));
    }
  }

  /**
   * Configuration that is only meaningful for other query engines, and cannot be
   * translated to Iceberg config.
   */
  private static class IgnoredConfig extends Config {
    public IgnoredConfig(String key) {
      super(key, ImmutableList.of());
    }

    @Override
    public boolean applyConfig(
        Map<String, String> sourceMap, Map<String, String> outputMap) {
      if (sourceMap.containsKey(catalogKey)) {
        sourceMap.remove(catalogKey);
        return true;
      } else {
        return false;
      }
    }
  }

  /**
   * Config for which we only support a single value.
   */
  private static class VerifiedConfig extends Config {
    private String expectedValue;
    public VerifiedConfig(String key, String expectedValue) {
      super(key, ImmutableList.of());
      Preconditions.checkState(expectedValue != null);
      this.expectedValue = expectedValue;
    }

    @Override
    public boolean applyConfig(
        Map<String, String> sourceMap, Map<String, String> outputMap) {
      String value = sourceMap.get(catalogKey);
      if (value != null) {
        // Config keys are case sensitive, but the values are typically not, especially
        // the config values that are verified (false/FALSE, none/NONE).
        if (!expectedValue.equalsIgnoreCase(value)) {
          throw new IllegalStateException(
              String.format(
                  "The only allowed value for REST Catalog property '%s' is '%s'.\n" +
                  "Value in configuration is '%s'",
                  catalogKey, expectedValue, value));
        }
        return true;
      }
      return false;
    }
  }

  private static final String NAME = "iceberg.rest-catalog.name";

  private static final ImmutableList<Config> CATALOG_CONFIGS = ImmutableList.of(
      new RequiredConfig(CatalogProperties.URI,
          ImmutableList.of("iceberg.rest-catalog.uri")),
      new Config("prefix",
          ImmutableList.of("iceberg.rest-catalog.prefix")),
      new Config(CatalogProperties.WAREHOUSE_LOCATION,
          ImmutableList.of("iceberg.rest-catalog.warehouse")),
      new Config(CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
          ImmutableList.of("iceberg.rest-catalog.session-timeout")),
      // USER sessions are not supported
      new VerifiedConfig("iceberg.rest-catalog.session", "NONE"),
      new VerifiedConfig("iceberg.rest-catalog.vended-credentials-enabled", "false"),
      new VerifiedConfig("iceberg.rest-catalog.nested-namespace-enabled", "false"),
      new VerifiedConfig("iceberg.rest-catalog.case-insensitive-name-matching", "true"),
      new IgnoredConfig("iceberg.rest-catalog.case-insensitive-name-matching.cache-ttl"),
      new IgnoredConfig("iceberg.catalog.type"),
      new IgnoredConfig("connector.name"),
      new IgnoredConfig(NAME)
  );

  private static final ImmutableList<Config> OAUTH2_CONFIGS = ImmutableList.of(
      // Since currently only OAUTH2 is possible we ignore this config. It also
      // doesn't map to any Iceberg catalog property.
      new VerifiedConfig("iceberg.rest-catalog.security", "OAUTH2"),
      // TODO: switch to OAuth2Properties.OAUTH2_SERVER_URI with Iceberg upgrade.
      new Config("oauth2-server-uri",
          ImmutableList.of("iceberg.rest-catalog.oauth2.server-uri")),
      new Config(OAuth2Properties.CREDENTIAL,
          ImmutableList.of("iceberg.rest-catalog.oauth2.credential")),
      new Config(OAuth2Properties.TOKEN,
          ImmutableList.of("iceberg.rest-catalog.oauth2.token")),
      new Config(OAuth2Properties.TOKEN_REFRESH_ENABLED,
          ImmutableList.of("iceberg.rest-catalog.oauth2.token-refresh-enabled")),
      new Config(OAuth2Properties.SCOPE,
          ImmutableList.of("iceberg.rest-catalog.oauth2.scope"))
  );

  private Map<String, String> sourceMap_;
  private Map<String, String> finalMap_;
  private String uri_;
  private String name_ = "";

  public RESTCatalogProperties(Properties properties) {
    sourceMap_ = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      sourceMap_.put(key, properties.getProperty(key));
    }

    // 'NAME' is used in RESTCatalog.initialize(), not in the properties.
    if (sourceMap_.containsKey(NAME)) {
      name_ = sourceMap_.get(NAME);
    }

    finalMap_ = new HashMap<>();
    applyConfigs(CATALOG_CONFIGS, sourceMap_, finalMap_);
    applyConfigs(OAUTH2_CONFIGS, sourceMap_, finalMap_);
    // Copy over remaining configuration that do not need special handling.
    for (Map.Entry<String, String> entry : sourceMap_.entrySet()) {
      Preconditions.checkState(!finalMap_.containsKey(entry.getKey()));
      finalMap_.put(entry.getKey(), entry.getValue());
    }
    uri_ = finalMap_.get(CatalogProperties.URI);
    Preconditions.checkState(uri_ != null);
  }

  private void applyConfigs(ImmutableList<Config> configs, Map<String, String> sourceMap,
      Map<String, String> outputMap) {
    for (Config config : configs) {
      config.applyConfig(sourceMap, outputMap);
    }
  }

  public String getName() { return name_; }
  public String getUri() { return uri_; }
  public Map<String, String> getCatalogProperties() { return finalMap_; }
}
