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

import static org.apache.impala.catalog.iceberg.translate.TranslationRule.IgnoreRule;
import static org.apache.impala.catalog.iceberg.translate.TranslationRule.RenameRule;
import static org.apache.impala.catalog.iceberg.translate.TranslationRule.RequiredRule;
import static org.apache.impala.catalog.iceberg.translate.TranslationRule.VerifyRule;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.impala.catalog.iceberg.translate.ConfigTranslator;
import org.apache.impala.catalog.iceberg.translate.TranslationRule;

import java.util.HashMap;
import java.util.List;
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
 * values. E.g. 'iceberg.rest-catalog.session' must be 'NONE', as USER sessions are
 * not supported yet.
 *
 * And some properties are simply ignored as they are specific to another query engine
 * (e.g. 'case-insensitive-name-matching.cache-ttl'), or they have different purposes than
 * configuring the REST catalog (e.g. 'connector.name').
 *
 * The remaining properties (that are not translated, verified, or ignored) don't
 * need special treatment and are simply returned as they are.
 *
 * The translation is expressed as a list of {@link TranslationRule}s applied by a
 * {@link ConfigTranslator}, the same declarative engine used by
 * {@code Credential} for credential-to-Hadoop translation.
 */
public class RESTCatalogProperties {
  private static final String NAME = "iceberg.rest-catalog.name";

  // Trino-style flag that enables Iceberg REST credential vending.
  private static final String VENDED_CREDENTIALS_ENABLED =
      "iceberg.rest-catalog.vended-credentials-enabled";

  // Header that makes the RESTCatalog request vended credentials on loadTable.
  private static final String ACCESS_DELEGATION_HEADER =
      "header.X-Iceberg-Access-Delegation";
  private static final String ACCESS_DELEGATION_VENDED = "vended-credentials";

  // Translation rules that turn Trino/Iceberg catalog properties into Iceberg-native
  // catalog properties. Unknown keys are passed through unchanged (see ConfigTranslator
  // constructed with passThroughRemaining=true). The rules apply in order: catalog
  // properties first, then OAuth2 properties.
  private static final ImmutableList<TranslationRule> RULES =
      ImmutableList.<TranslationRule>builder()
          // Catalog properties.
          .add(new RequiredRule(CatalogProperties.URI,
              List.of("iceberg.rest-catalog.uri")))
          .add(new RenameRule("prefix",
              List.of("iceberg.rest-catalog.prefix")))
          .add(new RenameRule(CatalogProperties.WAREHOUSE_LOCATION,
              List.of("iceberg.rest-catalog.warehouse")))
          .add(new RenameRule(CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
              List.of("iceberg.rest-catalog.session-timeout")))
          // USER sessions are not supported.
          .add(new VerifyRule("iceberg.rest-catalog.session", "NONE"))
          .add(new RenameRule(VENDED_CREDENTIALS_ENABLED))
          .add(new VerifyRule(
              "iceberg.rest-catalog.nested-namespace-enabled", "false"))
          .add(new VerifyRule(
              "iceberg.rest-catalog.case-insensitive-name-matching", "true"))
          .add(new IgnoreRule(
              "iceberg.rest-catalog.case-insensitive-name-matching.cache-ttl"))
          .add(new IgnoreRule("iceberg.catalog.type"))
          .add(new IgnoreRule("connector.name"))
          .add(new IgnoreRule(NAME))
          // OAuth2 properties. Since currently only OAUTH2 is possible we ignore the
          // 'security' config; it also doesn't map to any Iceberg catalog property.
          .add(new VerifyRule("iceberg.rest-catalog.security", "OAUTH2"))
          // TODO: switch to OAuth2Properties.OAUTH2_SERVER_URI with Iceberg upgrade.
          .add(new RenameRule("oauth2-server-uri",
              List.of("iceberg.rest-catalog.oauth2.server-uri")))
          .add(new RenameRule(OAuth2Properties.CREDENTIAL,
              List.of("iceberg.rest-catalog.oauth2.credential")))
          .add(new RenameRule(OAuth2Properties.TOKEN,
              List.of("iceberg.rest-catalog.oauth2.token")))
          .add(new RenameRule(OAuth2Properties.TOKEN_REFRESH_ENABLED,
              List.of("iceberg.rest-catalog.oauth2.token-refresh-enabled")))
          .add(new RenameRule(OAuth2Properties.SCOPE,
              List.of("iceberg.rest-catalog.oauth2.scope")))
          .build();

  private static final ConfigTranslator TRANSLATOR =
      new ConfigTranslator(RULES, /*passThroughRemaining*/ true);

  private final Map<String, String> finalMap_;
  private final String uri_;
  private String name_ = "";

  public RESTCatalogProperties(Properties properties) {
    Map<String, String> sourceMap = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      sourceMap.put(key, properties.getProperty(key));
    }

    // 'NAME' is used in RESTCatalog.initialize(), not in the properties.
    if (sourceMap.containsKey(NAME)) {
      name_ = sourceMap.get(NAME);
    }

    finalMap_ = new HashMap<>(TRANSLATOR.translate(sourceMap));
    uri_ = finalMap_.get(CatalogProperties.URI);
    Preconditions.checkState(uri_ != null);

    // When vending is enabled, ask the REST catalog to return storage credentials on
    // loadTable by sending the access-delegation header. The vended credentials are
    // then extracted from the loaded table's FileIO (see Credential.extract).
    boolean credentialsEnabled = Boolean.parseBoolean(
        finalMap_.get(VENDED_CREDENTIALS_ENABLED));
    if (credentialsEnabled) {
      finalMap_.put(ACCESS_DELEGATION_HEADER, ACCESS_DELEGATION_VENDED);
    }
  }

  public String getName() { return name_; }
  public String getUri() { return uri_; }
  public Map<String, String> getCatalogProperties() { return finalMap_; }
}
