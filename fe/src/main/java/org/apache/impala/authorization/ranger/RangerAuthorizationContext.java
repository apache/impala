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
import org.apache.impala.util.EventSequence;
import org.apache.ranger.audit.model.AuthzAuditEvent;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Ranger specific {@link AuthorizationContext}.
 */
public class RangerAuthorizationContext extends AuthorizationContext {
  private final TSessionState sessionState_;
  // Audit handler can be null meaning we don't want to do an audit log.
  private @Nullable RangerBufferAuditHandler auditHandler_;
  private Map<String, AuthzAuditEvent> deduplicatedAuditEvents_;

  public RangerAuthorizationContext(TSessionState sessionState,
      Optional<EventSequence> timeline) {
    super(timeline);
    sessionState_ = sessionState;
    deduplicatedAuditEvents_ = new LinkedHashMap<>();
  }

  public void setAuditHandler(RangerBufferAuditHandler auditHandler) {
    auditHandler_ = Preconditions.checkNotNull(auditHandler);
  }

  public RangerBufferAuditHandler getAuditHandler() { return auditHandler_; }

  /**
   * We stash the List of AuthzAuditEvent's in a Map after the analysis of the query and
   * thus the AuthzAuditEvent's in the Map are deduplicated. At this point, there are
   * only column masking-related events on the List. We will add back the deduplicated
   * events by applyDeduplicatedAuthzEvents() only if the authorization of the query is
   * successful. Thus, the relative order between the column masking-related events and
   * other events on the List of auditHandler_.getAuthzEvents() is changed afterwards and
   * only those column masking-related events are deduplicated.
   */
  public void stashAuditEvents(RangerImpalaPlugin plugin) {
    Set<String> unfilteredMaskNames = plugin.getUnfilteredMaskNames(
        Arrays.asList("MASK_NONE"));
    for (AuthzAuditEvent event : auditHandler_.getAuthzEvents()) {
      // We assume that all the logged events until now are column masking-related. Since
      // we remove those AuthzAuditEvent's corresponding to the "Unmasked" policy of type
      // "MASK_NONE", we exclude this type of mask.
      Preconditions.checkState(unfilteredMaskNames
          .contains(event.getAccessType().toUpperCase()));

      // event.getEventKey() is the concatenation of the following fields in an
      // AuthzAuditEvent: 'user', 'accessType', 'resourcePath', 'resourceType', 'action',
      // 'accessResult', 'sessionId', and 'clientIP'. Recall that 'resourcePath' is the
      // concatenation of 'dbName', 'tableName', and 'columnName' that were used to
      // instantiate a RangerAccessResourceImpl in order to create a RangerAccessRequest
      // to call RangerImpalaPlugin#evalDataMaskPolicies(). Refer to
      // RangerAuthorizationChecker#evalColumnMask() for further details.
      deduplicatedAuditEvents_.put(event.getEventKey(), event);
    }
    auditHandler_.getAuthzEvents().clear();
  }

  public void applyDeduplicatedAuthzEvents() {
    auditHandler_.getAuthzEvents().addAll(deduplicatedAuditEvents_.values());
  }

  public TSessionState getSessionState() { return sessionState_; }
}
