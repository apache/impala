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

package org.apache.impala.authentication.saml;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.http.impl.EnglishReasonPhraseCatalog;
import org.apache.impala.thrift.TWrappedHttpRequest;
import org.apache.impala.thrift.TWrappedHttpResponse;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.pac4j.core.context.Cookie;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;

// Wraps TWrappedHttpRequest + TWrappedHttpResponse as a Pac4j WebContext.
public class WrappedWebContext implements WebContext {

  private final TWrappedHttpRequest request;
  private final TWrappedHttpResponse response;
  private final Map<String, Object> attributes = new HashMap<>();
  private final List<Cookie> requestCookies;
  private final Map<String, String[]> requestParameters;
  // TODO: if real session management seems necessary then this should be replaced,
  //       probably with a singleton
  private final SessionStore sessionStore = new NullSessionStore();

  public WrappedWebContext(TWrappedHttpRequest request, TWrappedHttpResponse response) {
    this.request = request;
    this.response = response;
    requestCookies = request.cookies.entrySet().stream()
       .map(e -> new Cookie(e.getKey(), e.getValue()))
       .collect(Collectors.toList());
    // This doesn't support the same parameter multiple times in the query string.
    requestParameters = request.cookies.entrySet().stream()
       .collect(Collectors.toMap(e -> e.getKey(), e -> new String[]{e.getValue()}));
    response.headers = new HashMap<>();
    response.cookies = new HashMap<>();
  }

  @Override
  public void addResponseCookie(Cookie cookie) {
    response.cookies.put(cookie.getName(), cookie.getValue());
  }

  @Override
  public String getFullRequestURL() {
  String url = request.getServer_name() + ":" + request.server_port + request.getPath();
    // TODO recreate params or store original URL?
    return url;
  }

  @Override
  public String getPath() {
    return request.getPath();
  }

  @Override
  public String getRemoteAddr() {
    return request.getRemote_ip();
  }

  @Override
  public Optional getRequestAttribute(String name) {
    return Optional.ofNullable(attributes.get(name));
  }


  @Override
  public Collection<Cookie> getRequestCookies() {
    return requestCookies;
  }

  @Override
  public Optional<String> getRequestHeader(String name) {
    return Optional.ofNullable(request.getHeaders().get(name));
  }

  @Override
  public String getRequestMethod() {
    return request.getMethod();
  }

  @Override
  public Optional<String> getRequestParameter(String name) {
    return Optional.ofNullable(request.getParams().get(name));
  }

  @Override
  public Map<String, String[]> getRequestParameters() {
    return requestParameters;
  }

  @Override
  public String getScheme() {
    // TODO Do we need to support http too?
    return "https";
  }

  @Override
  public String getServerName() {
    return request.getServer_name();
  }

  @Override
  public int getServerPort() {
    return request.getServer_port();
  }

  @Override
  public SessionStore getSessionStore() {
    return sessionStore;
  }

  @Override
  public boolean isSecure() {
    return request.isSecure();
  }

  @Override
  public String getRequestContent() {
    return request.content;
  }


  @Override
  public void setRequestAttribute(String name, Object value) {
    attributes.put(name, value);
  }

  @Override
  public void setResponseContentType(String contentType) {
    response.setContent_type(contentType);
  }

  @Override
  public void setResponseHeader(String name, String value) {
    response.getHeaders().put(name, value);
  }

  public void setResponseStatusCode(int status) {
    response.setStatus_code((short)status);
    response.setStatus_text(EnglishReasonPhraseCatalog.INSTANCE.getReason(status, null));
  }

  public void setResponseContent(String contentType, String content) {
    response.setContent_type(contentType);
    response.setContent(content);
  }

  public String getRequestAsJsonString() {
    try {
      TSerializer jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      return jsonSerializer.toString(request);
    } catch (TException ex) {
      return "Couldn't deserialize TWrappedHttpRequest: " + ex.getMessage();
    }
  }

  public String getResponseAsJsonString() {
    try {
      TSerializer jsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
      return jsonSerializer.toString(response);
    } catch (TException ex) {
      return "Couldn't deserialize TWrappedHttpResponse: " + ex.getMessage();
    }
  }

}
