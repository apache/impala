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

import java.net.URL;
import java.util.Scanner;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Utility class for retrieving metrics from the Impala webserver.
 */
public class Metrics {
  private final static String DEFAULT_WEBSERVER_BASE_URL = "http://localhost:25000/";
  private final static String JSON_METRICS = "jsonmetrics?json";

  private String webserverBaseUrl;

  public Metrics() {
    this.webserverBaseUrl = DEFAULT_WEBSERVER_BASE_URL;
  }

  public Metrics(String webserverUrl) {
    this.webserverBaseUrl = webserverUrl;
  }

  /**
   * Returns the metric for the given metric id from the Impala web server.
   * @param metricId identifier of the metric we want to retrieve
   * @return A Java Object that holds the value for the given metric id.
   *         The caller needs to cast it to the appropriate type, e.g. Long, String, etc.
   */
  public Object getMetric(String metricId) throws Exception {
    String content = readContent(new URL(this.webserverBaseUrl + JSON_METRICS));
    if (content == null) return null;

    JSONObject json = toJson(content);
    if (json == null) return null;

    return json.get(metricId);
  }

  private static String readContent(URL url) throws Exception {
    String ret = null;

    try (Scanner scanner = new Scanner(url.openStream(), "UTF-8")) {
      scanner.useDelimiter("\\A");
      if (scanner.hasNext()) {
        ret = scanner.next();
      }
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
