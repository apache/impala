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
import static org.junit.Assert.fail;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;

public class TestRESTCatalogProperties {

  @Test
  public void testEmptyConfig() {
    try {
      Properties props = new Properties();
      RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    } catch (Exception e) {
      // RESTCatalogProperties throws an exception if required properties are not defined.
      return;
    }
    fail();
  }

  @Test
  public void testUriOnlyConfig() {
    Properties props = new Properties();
    props.setProperty(CatalogProperties.URI, "test-uri");

    RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    assertEquals("test-uri", restProps.getUri());
    assertEquals("", restProps.getName());
    assertEquals(1, restProps.getCatalogProperties().size());
    assertTrue(restProps.getCatalogProperties().containsKey(CatalogProperties.URI));
  }

  @Test
  public void testIcebergNativeConfig() {
    Properties props = new Properties();
    props.setProperty(CatalogProperties.URI, "test-uri");
    props.setProperty("iceberg.rest-catalog.name", "catalog-name");
    props.setProperty(CatalogProperties.WAREHOUSE_LOCATION, "warehouse-loc");
    props.setProperty(CatalogProperties.AUTH_SESSION_TIMEOUT_MS, "5000");
    //TODO: Switch to OAuth2Properties.OAUTH2_SERVER_URI with Iceberg upgrade.
    props.setProperty("oauth2-server-uri", "oauth-uri");
    props.setProperty(OAuth2Properties.TOKEN, "oauth-token");
    props.setProperty(OAuth2Properties.SCOPE, "oauth-scope");

    RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    assertEquals("test-uri", restProps.getUri());
    assertEquals("catalog-name", restProps.getName());
    assertEquals(6, restProps.getCatalogProperties().size());
    Map<String, String> catProps = restProps.getCatalogProperties();
    assertEquals("test-uri", catProps.get(CatalogProperties.URI));
    assertEquals("warehouse-loc", catProps.get(CatalogProperties.WAREHOUSE_LOCATION));
    assertEquals("5000", catProps.get(CatalogProperties.AUTH_SESSION_TIMEOUT_MS));
    assertEquals("oauth-uri", catProps.get("oauth2-server-uri"));
    assertEquals("oauth-token", catProps.get(OAuth2Properties.TOKEN));
    assertEquals("oauth-scope", catProps.get(OAuth2Properties.SCOPE));
  }

  @Test
  public void testTrinoConfig() {
    Properties props = new Properties();
    props.setProperty("iceberg.rest-catalog.uri", "test-uri");
    props.setProperty("iceberg.rest-catalog.name", "catalog-name");
    props.setProperty("iceberg.rest-catalog.warehouse", "warehouse-loc");
    props.setProperty("iceberg.rest-catalog.session-timeout", "5000");
    //TODO: Switch to OAuth2Properties.OAUTH2_SERVER_URI with Iceberg upgrade.
    props.setProperty("iceberg.rest-catalog.oauth2.server-uri", "oauth-uri");
    props.setProperty("iceberg.rest-catalog.oauth2.credential", "oauth-cred");

    RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    assertEquals("test-uri", restProps.getUri());
    assertEquals("catalog-name", restProps.getName());
    assertEquals(5, restProps.getCatalogProperties().size());
    Map<String, String> catProps = restProps.getCatalogProperties();
    assertEquals("test-uri", catProps.get(CatalogProperties.URI));
    assertEquals("warehouse-loc", catProps.get(CatalogProperties.WAREHOUSE_LOCATION));
    assertEquals("5000", catProps.get(CatalogProperties.AUTH_SESSION_TIMEOUT_MS));
    assertEquals("oauth-uri", catProps.get("oauth2-server-uri"));
    assertEquals("oauth-cred", catProps.get(OAuth2Properties.CREDENTIAL));
  }

  @Test
  public void testAmbiguousKeys() {
    try {
      Properties props = new Properties();
      props.setProperty("iceberg.rest-catalog.uri", "test-uri");
      props.setProperty("uri", "test-uri2");
      props.setProperty(CatalogProperties.WAREHOUSE_LOCATION, "warehouse-loc");

      RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    } catch (Exception e) {
      // RESTCatalogProperties throws an exception when the same property is defined
      // multiple times.
      return;
    }
    fail();
  }

  @Test
  public void testVerifiedConfigsSucceed() {
    Properties props = new Properties();
    props.setProperty("iceberg.rest-catalog.uri", "test-uri");
    props.setProperty("iceberg.rest-catalog.session", "none");
    props.setProperty("iceberg.rest-catalog.vended-credentials-enabled", "false");

    RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    Map<String, String> catProps = restProps.getCatalogProperties();
    assertEquals(3, catProps.size());
    assertEquals("test-uri", catProps.get(CatalogProperties.URI));
    assertEquals("none", catProps.get("iceberg.rest-catalog.session"));
    assertEquals("false", catProps.get(
        "iceberg.rest-catalog.vended-credentials-enabled"));
  }

  @Test
  public void testVerifiedConfigsFail() {
    try {
      Properties props = new Properties();
      props.setProperty("iceberg.rest-catalog.uri", "test-uri");
      props.setProperty("iceberg.rest-catalog.session", "user");

      RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    } catch (Exception e) {
      // RESTCatalogProperties throws an exception when a verified config doesn't
      // have the expected value.
      return;
    }
    fail();
  }

  @Test
  public void testIgnoredConfigs() {
    Properties props = new Properties();
    props.setProperty(CatalogProperties.URI, "test-uri");
    props.setProperty("iceberg.rest-catalog.name", "catalog-name");
    props.setProperty(CatalogProperties.WAREHOUSE_LOCATION, "warehouse-loc");
    props.setProperty("iceberg.rest-catalog.session-timeout", "5000");
    //TODO: Switch to OAuth2Properties.OAUTH2_SERVER_URI with Iceberg upgrade.
    props.setProperty("iceberg.rest-catalog.oauth2.server-uri", "oauth-uri");
    props.setProperty(OAuth2Properties.CREDENTIAL, "oauth-cred");
    props.setProperty("connector.name", "iceberg");
    props.setProperty("iceberg.catalog.type", "rest");

    RESTCatalogProperties restProps = new RESTCatalogProperties(props);
    assertEquals("test-uri", restProps.getUri());
    assertEquals("catalog-name", restProps.getName());
    Map<String, String> catProps = restProps.getCatalogProperties();
    assertEquals(5, catProps.size());
    assertFalse(catProps.containsKey("connector.name"));
    assertFalse(catProps.containsKey("iceberg.catalog.type"));
    assertEquals("test-uri", catProps.get(CatalogProperties.URI));
    assertEquals("warehouse-loc", catProps.get(CatalogProperties.WAREHOUSE_LOCATION));
    assertEquals("5000", catProps.get(CatalogProperties.AUTH_SESSION_TIMEOUT_MS));
    assertEquals("oauth-uri", catProps.get("oauth2-server-uri"));
    assertEquals("oauth-cred", catProps.get(OAuth2Properties.CREDENTIAL));
  }
}
