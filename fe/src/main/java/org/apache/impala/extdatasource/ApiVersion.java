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

package org.apache.impala.extdatasource;

import org.apache.impala.extdatasource.v1.ExternalDataSource;
import com.google.common.base.Strings;

/**
 * Enumerates the valid versions of the {@link ExternalDataSource} API.
 */
public enum ApiVersion {
  V1(org.apache.impala.extdatasource.v1.ExternalDataSource.class);

  private final Class<?> apiInterface_;

  ApiVersion(Class<?> interfaceClass) {
    apiInterface_ = interfaceClass;
  }

  /**
   * Gets the {@link Class} for the interface this API version represents.
   */
  public Class<?> getApiInterface() { return apiInterface_; }

  /**
   * Parses the API version from the string. Is case-insensitive.
   * @return The value of the ApiVersion enum represented by the string or null
   *         if the string is not a valid ApiVersion.
   */
  public static ApiVersion parseApiVersion(String apiVersionString) {
    if (Strings.isNullOrEmpty(apiVersionString)) return null;
    try {
      return valueOf(apiVersionString.toUpperCase());
    } catch (IllegalArgumentException ex) {
      return null;
    }
  }
}