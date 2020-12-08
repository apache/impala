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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.impala.service.BackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// slightly modified (config replaced) version of
// https://github.com/vihangk1/hive/blob/master_saml/service/src/java/org/apache/hive/service/auth/saml/HiveSamlAuthTokenGenerator.java

/**
 * A token is generated when the SAML assertion is successfully validated. This Token
 * is passed back to the client (Jdbc/ODBC Driver) via the browser. This token is
 * presented by the subsequent http request as a bearer token.
 */
public class HiveSamlAuthTokenGenerator implements AuthTokenGenerator {

  private final long ttlMs;
  private final SecureRandom rand = new SecureRandom();
  private final byte[] signatureSecret = Long.toString(rand.nextLong()).getBytes();
  private static final String USER = "u";
  private static final String SEPARATOR = "=";
  private static final String ATTR_SEPARATOR = ";";
  private static final String ID = "id";
  private static final String CREATE_TIME = "time";
  public static final String RELAY_STATE = "rs";
  private static final String SIGN = "sg";
  private static HiveSamlAuthTokenGenerator INSTANCE;
  private static final Logger LOG = LoggerFactory.getLogger(HiveSamlAuthTokenGenerator.class);

  public static synchronized AuthTokenGenerator get() {
    if (INSTANCE != null) {
      return INSTANCE;
    }
    INSTANCE = new HiveSamlAuthTokenGenerator();
    return INSTANCE;
  }

  private HiveSamlAuthTokenGenerator() {
    BackendConfig conf = BackendConfig.INSTANCE;
    ttlMs = conf.getSaml2CallbackTokenTtl();
  }

  @Override
  public String get(String username, String relayStateKey) {
    String id = String.valueOf(rand.nextLong());
    String time = String.valueOf(System.currentTimeMillis());
    LOG.debug("Generating token for user {} with id {} and time {}", username, id, time);
    String tokenStr = getTokenStr(username, id, time, relayStateKey);
    return sign(tokenStr);
  }

  private String getTokenStr(String username, String id, String timestamp,
      String relayStateKey) {
    StringBuilder sb = new StringBuilder();
    sb.append(USER).append(SEPARATOR).append(username)
        .append(ATTR_SEPARATOR);
    sb.append(ID).append(SEPARATOR).append(id)
        .append(ATTR_SEPARATOR);
    sb.append(CREATE_TIME).append(SEPARATOR).append(timestamp).append(ATTR_SEPARATOR);
    sb.append(RELAY_STATE).append(SEPARATOR).append(relayStateKey);
    return sb.toString();
  }

  private String getSign(String input) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(input.getBytes());
      md.update(signatureSecret);
      byte[] digest = md.digest();
      return Base64.getEncoder().encodeToString(digest);
    } catch (NoSuchAlgorithmException ex) {
      throw new RuntimeException(ex);
    }
  }

  private String sign(String input) {
    return input + ATTR_SEPARATOR + SIGN + SEPARATOR + getSign(input);
  }

  @Override
  public String validate(String token) throws HttpSamlAuthenticationException {
    Map<String, String> keyValue = new HashMap<>();
    if (!parse(token, keyValue)) {
      throw new HttpSamlAuthenticationException("Invalid token");
    }
    String tokenStr = getTokenStr(keyValue.get(USER), keyValue.get(ID),
        keyValue.get(CREATE_TIME), keyValue.get(RELAY_STATE));
    String signature = getSign(tokenStr);
    if (!signatureMatches(keyValue.get(SIGN), signature)) {
      throw new HttpSamlAuthenticationException("Token could not be verified");
    }
    if (isExpired(System.currentTimeMillis(),
        Long.parseLong(keyValue.get(CREATE_TIME)))) {
      throw new HttpSamlAuthenticationException("Token is expired");
    }
    return keyValue.get(USER);
  }

  private boolean isExpired(long currentTime, long tokenTime) {
    if (currentTime >= tokenTime) {
      return (currentTime - tokenTime) > ttlMs;
    }
    return false;
  }

  private boolean signatureMatches(String origSign, String derivedSign) {
    return !MessageDigest.isEqual(origSign.getBytes(), derivedSign.getBytes());
  }

  public static boolean parse(String token, Map<String, String> kv) {
    String[] splits = token.split(ATTR_SEPARATOR);
    if (splits.length != 5) {
      return false;
    }
    for (String split : splits) {
      String[] pair = split.split(SEPARATOR);
      if (pair.length != 2) {
        return false;
      }
      kv.put(pair[0], pair[1]);
    }
    return kv.containsKey(USER) && kv.containsKey(CREATE_TIME) && kv.containsKey(ID) && kv
        .containsKey(SIGN) && kv.containsKey(RELAY_STATE);
  }
}
