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


package org.apache.impala.authorization;

import org.apache.impala.authorization.ranger.RangerAuthorizationFactory;

/**
 * This enum contains the list of authorization providers supported in Impala.
 *
 * Associated with each enum value is an {@link AuthorizationFactory} implementation
 * used to create the correct authorization classes.
 */
public enum AuthorizationProvider {
  RANGER(RangerAuthorizationFactory.class.getCanonicalName()),
  NOOP(NoopAuthorizationFactory.class.getCanonicalName());

  private final String factoryClassName_;

  AuthorizationProvider(String factoryClassName) {
    this.factoryClassName_ = factoryClassName;
  }

  /**
   * Returns the canonical name of the {@link AuthorizationFactory} implementation
   * associated with this provider.
   *
   * @return the canonical name of the {@link AuthorizationFactory} impl for `this`
   */
  public String getAuthorizationFactoryClassName() {
    return factoryClassName_;
  }
}
