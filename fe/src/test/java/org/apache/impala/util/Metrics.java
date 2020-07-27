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

import java.io.IOException;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.HttpHost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Utility class for retrieving metrics from the Impala webserver.
 */
public class Metrics {
  private final static String WEBSERVER_HOST = "localhost";
  private final static int DEFAULT_WEBSERVER_PORT = 25000;
  private final static String JSON_METRICS = "/jsonmetrics?json";

  private CloseableHttpClient httpClient_;
  private int port_;
  private String username_;
  private String password_;

  public Metrics() { this("", "", DEFAULT_WEBSERVER_PORT); }

  public Metrics(int port) { this("", "", port); }

  public Metrics(String username, String password) {
    this(username, password, DEFAULT_WEBSERVER_PORT);
  }

  public Metrics(String username, String password, int port) {
    this.username_ = username;
    this.password_ = password;
    this.port_ = port;
    httpClient_ = HttpClients.createDefault();
  }

  public void Close() throws IOException { httpClient_.close(); }

  /**
   * Returns the metric for the given metric id from the Impala web server.
   * @param metricId identifier of the metric we want to retrieve
   * @return A Java Object that holds the value for the given metric id.
   *         The caller needs to cast it to the appropriate type, e.g. Long, String, etc.
   */
  public Object getMetric(String metricId) throws Exception {
    String content = readContent(JSON_METRICS);
    if (content == null) return null;

    JSONObject json = toJson(content);
    if (json == null) return null;

    return json.get(metricId);
  }

  /**
   * Retrieves the page at 'path' and returns its contents.
   */
  public String readContent(String path) throws IOException {
    HttpHost targetHost = new HttpHost(WEBSERVER_HOST, port_, "http");
    HttpClientContext context = HttpClientContext.create();
    if (!username_.equals("")) {
      CredentialsProvider credsProvider = new BasicCredentialsProvider();
      credsProvider.setCredentials(new AuthScope(WEBSERVER_HOST, port_),
          new UsernamePasswordCredentials(username_, password_));
      AuthCache authCache = new BasicAuthCache();
      authCache.put(targetHost, new BasicScheme());
      context.setCredentialsProvider(credsProvider);
      context.setAuthCache(authCache);
    }

    String ret = "";
    HttpGet httpGet = new HttpGet(path);
    CloseableHttpResponse response = httpClient_.execute(targetHost, httpGet, context);
    try {
      ret = EntityUtils.toString(response.getEntity());
    } finally {
      response.close();
    }

    return ret;
  }

  private static JSONObject toJson(String text) throws Exception {
    JSONParser parser = new JSONParser();
    Object obj = parser.parse(text);

    if (!(obj instanceof JSONObject)) {
      return null;
    }

    return (JSONObject)obj;
  }
}
