/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.util;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;

import org.apache.impala.common.ImpalaRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class that converts between JSON string and property map.
 */
public class JsonUtil {

  private static final Logger LOG = LoggerFactory.getLogger(JsonUtil.class);

  /**
   * Convert string in JSON format to property map.
   */
  public static Map<String, String> convertJSONToPropertyMap(String propertyString)
      throws ImpalaRuntimeException {
    Map<String, String> propertyMap = null;
    if (!Strings.isNullOrEmpty(propertyString)) {
      try {
        TypeReference<HashMap<String, String>> typeRef =
            new TypeReference<HashMap<String, String>>() {};
        propertyMap = new ObjectMapper().readValue(propertyString, typeRef);
      } catch (JsonProcessingException e) {
        String errorMessage = String.format(
            "Invalid JSON string for property: '%s'", propertyString);
        LOG.error(errorMessage, e);
        throw new ImpalaRuntimeException(errorMessage);
      }
    }
    return propertyMap;
  }

  /**
   * Convert property map to string in JSON format.
   */
  public static String convertPropertyMapToJSON(Map<String, String> propertyMap)
      throws ImpalaRuntimeException {
    if (propertyMap != null && propertyMap.size() > 0) {
      try {
        return new ObjectMapper().writeValueAsString(propertyMap);
      } catch (JsonProcessingException e) {
        String errorMessage = String.format(
            "Failed to convert property map to JSON string: %s", e.getMessage());
        LOG.error(errorMessage);
        throw new ImpalaRuntimeException(errorMessage);
      }
    }
    return new String("");
  }
}

