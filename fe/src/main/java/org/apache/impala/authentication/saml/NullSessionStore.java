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

import java.util.Optional;

import org.pac4j.core.context.WebContext;
import org.pac4j.core.context.session.SessionStore;

// Very primitive implementation of SessionStore that doesn't save/or load any
// session attributes. As Impala needs no session management, the only function
// this has is returning the remote address as session Id for logging.
public class NullSessionStore implements SessionStore {

  @Override
  public String getOrCreateSessionId(WebContext context) {
   WrappedWebContext wrappedContext = (WrappedWebContext) context;
    return wrappedContext.getRemoteAddr();
  }

  @Override
  public Optional get(WebContext context, String key) {
    return Optional.empty();
  }

  @Override
  public void set(WebContext context, String key, Object value) {
  }

  @Override
  public boolean destroySession(WebContext context) {
    return false;
  }

  @Override
  public Optional getTrackableSession(WebContext context) {
    return Optional.empty();
  }

  @Override
  public Optional buildFromTrackableSession(WebContext context, Object trackableSession) {
    return Optional.empty();
  }

  @Override
  public boolean renewSession(WebContext context) {
    return false;
  }

}
