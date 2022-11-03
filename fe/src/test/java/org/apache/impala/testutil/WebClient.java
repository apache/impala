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

package org.apache.impala.testutil;

import java.io.IOException;
import java.util.List;

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.AuthCache;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.cookie.Cookie;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.NameValuePair;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicAuthCache;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Utility class for interacting with the Impala webserver.
 */
public class WebClient {
  private final static String WEBSERVER_HOST = "localhost";
  private final static int DEFAULT_WEBSERVER_PORT = 25000;
  private final static String JSON_METRICS = "/jsonmetrics?json";

  private CloseableHttpClient httpClient_;
  private BasicCookieStore cookieStore_;
  private int port_;
  private String username_;
  private String password_;

  public WebClient() { this("", "", DEFAULT_WEBSERVER_PORT); }

  public WebClient(int port) { this("", "", port); }

  public WebClient(String username, String password) {
    this(username, password, DEFAULT_WEBSERVER_PORT);
  }

  public WebClient(String username, String password, int port) {
    this.username_ = username;
    this.password_ = password;
    this.port_ = port;
    httpClient_ = HttpClients.createDefault();
    cookieStore_ = new BasicCookieStore();
  }

  public void Close() throws IOException { httpClient_.close(); }

  public List<Cookie> getCookies() { return cookieStore_.getCookies(); }

  /**
   * Returns the metric for the given metric id from the Impala web server.
   * @param metricId identifier of the metric we want to retrieve
   * @return A Java Object that holds the value for the given metric id.
   *         The caller needs to cast it to the appropriate type, e.g. Long, String, etc.
   */
  public Object getMetric(String metricId) throws Exception {
    JSONObject json = jsonGet(JSON_METRICS);
    if (json == null) return null;

    return json.get(metricId);
  }

  /**
   * Does a GET request at path of the Impala web server and returns response.
   * @param path URI path to query
   * @return A JSON object, or null if not parseable as JSON
   * @throws Exception if the request fails or JSON is not an object
   */
  public JSONObject jsonGet(String path) throws Exception {
    return toJson(readContent(path));
  }

  /**
   * Does a POST request at path of the Impala web server and returns response.
   * @param path URI path to query
   * @param headers Headers to include in the request
   * @param params Parameters to include in the POST
   * @return A JSON object, or null if not parseable as JSON
   * @throws Exception if the request fails or JSON is not an object
   */
  public JSONObject jsonPost(String path, Header[] headers, List<NameValuePair> params)
      throws Exception {
    return toJson(post(path, headers, params, 200));
  }

  /**
   * Retrieves the page at 'path' and returns its contents.
   */
  public String readContent(String path) throws IOException {
    HttpHost target = new HttpHost(WEBSERVER_HOST, port_, "http");
    HttpClientContext context = getContext(target);

    HttpGet get = new HttpGet(path);
    try (CloseableHttpResponse response = httpClient_.execute(target, get, context)) {
      return EntityUtils.toString(response.getEntity());
    }
  }

  /**
   * Does a POST request at path of the Impala web server and returns response.
   * If the response does not include the expected status code, returns null.
   * @param path URI path to query
   * @param headers Headers to include in the POST; can be null
   * @param params Parameters to include in the POST; can be null
   * @param code Response status code expected
   * @return Response string
   * @throws IOException if the request fails
   */
  public String post(String path, Header[] headers, List<NameValuePair> params, int code)
      throws IOException {
    HttpHost target = new HttpHost(WEBSERVER_HOST, port_, "http");
    HttpClientContext context = getContext(target);

    HttpPost post = new HttpPost(path);
    if (headers != null) {
      post.setHeaders(headers);
    }
    if (params != null) {
      post.setEntity(new UrlEncodedFormEntity(params));
    }
    try (CloseableHttpResponse response = httpClient_.execute(target, post, context)) {
      if (response.getStatusLine().getStatusCode() != code) {
        return null;
      }
      return EntityUtils.toString(response.getEntity());
    }
  }

  private HttpClientContext getContext(HttpHost targetHost) {
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
    context.setCookieStore(cookieStore_);
    return context;
  }

  private static JSONObject toJson(String text) throws ParseException {
    JSONParser parser = new JSONParser();
    Object obj = parser.parse(text);

    if (!(obj instanceof JSONObject)) {
      return null;
    }

    return (JSONObject)obj;
  }
}
