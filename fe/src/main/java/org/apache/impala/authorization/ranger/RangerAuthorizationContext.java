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

package org.apache.impala.authorization.ranger;

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.AuthorizationContext;
import org.apache.impala.thrift.TSessionState;

import javax.annotation.Nullable;

/**
 * Ranger specific {@link AuthorizationContext}.
 */
public class RangerAuthorizationContext extends AuthorizationContext {
  private final TSessionState sessionState_;
  // Audit handler can be null meaning we don't want to do an audit log.
  private @Nullable RangerBufferAuditHandler auditHandler_;

  public RangerAuthorizationContext(TSessionState sessionState) {
    sessionState_ = sessionState;
  }

  public void setAuditHandler(RangerBufferAuditHandler auditHandler) {
    auditHandler_ = Preconditions.checkNotNull(auditHandler);
  }

  public RangerBufferAuditHandler getAuditHandler() { return auditHandler_; }

  public TSessionState getSessionState() { return sessionState_; }
}
