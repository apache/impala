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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Ranger specific {@link AuthorizationContext}.
 */
public class RangerAuthorizationContext extends AuthorizationContext {
  private static final Logger LOG = LoggerFactory.getLogger(
      RangerAuthorizationContext.class);
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
   * Stash and deduplicate the audit events produced by table masking (Column-masking /
   * Row-filtering) which are performed during the analyze phase. Called at the end of
   * analyzing. These stashed events will be added back after the query pass the
   * authorization phase. Note that normal events (select, insert, drop, etc.) are
   * produced in the authorization phase. Stashing table masking events avoids exposing
   * them when the query fails authorization. Refer to IMPALA-9597 for further details.
   */
  public void stashTableMaskingAuditEvents(RangerImpalaPlugin plugin) {
    // Collect all the column masking types except "MASK_NONE", because MASK_NONE events
    // have been removed in RangerAuthorizationChecker#removeStaleAudits().
    Set<String> legalEventTypes = plugin.getUnfilteredMaskNames(
        Arrays.asList("MASK_NONE"));
    // Row filter policies produce ROW_FILTER events.
    legalEventTypes.add(RangerBufferAuditHandler.ACCESS_TYPE_ROWFILTER.toUpperCase());
    for (AuthzAuditEvent event : auditHandler_.getAuthzEvents()) {
      // We assume that all the logged events until now are table masking-related.
      Preconditions.checkState(legalEventTypes
          .contains(event.getAccessType().toUpperCase()),
          "Illegal event access type: %s. Should be one of %s. Event details: %s",
          event.getAccessType(), legalEventTypes, event);

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

  /**
   * Consolidate the audit log entries of column accesses granted by the same policy in
   * the same table.
   */
  public void consolidateAuthzEvents() {
    Map<String, AuthzAuditEvent> consolidatedEvents = new LinkedHashMap<>();
    List<AuthzAuditEvent> unconsolidatedEvents = new LinkedList<>();
    for (AuthzAuditEvent event : auditHandler_.getAuthzEvents()) {
      String resourceType = event.getResourceType();
      if ("@column".equals(resourceType)) {
        String[] parsedNames = event.getResourcePath().split("/", 3);
        if (parsedNames.length < 3) {
          LOG.error("The column resource path " + event.getResourcePath() + " could " +
              "not be parsed into the form of " +
              "<database name>/<table name>/<column name>.");
          continue;
        }
        long policyId = event.getPolicyId();
        String databaseName = parsedNames[0];
        String tableName = parsedNames[1];
        String columnName = parsedNames[2];
        // Currently the access type can only be "select" for a non-column masking audit
        // log entry. We include the access type as part of the key to avoid accidentally
        // combining audit log entries corresponding to the same column but of different
        // access types in the future.
        String accessType = event.getAccessType();
        String key = policyId + "/" + databaseName + "/" + tableName + "/" + accessType;
        if (!consolidatedEvents.containsKey(key)) {
          consolidatedEvents.put(key, event);
        } else {
          AuthzAuditEvent consolidatedEvent = consolidatedEvents.get(key);
          consolidatedEvent
              .setResourcePath(consolidatedEvent.getResourcePath() + "," + columnName);
          consolidatedEvents.put(key, consolidatedEvent);
        }
      } else {
        unconsolidatedEvents.add(event);
      }
    }
    auditHandler_.getAuthzEvents().clear();
    auditHandler_.getAuthzEvents().addAll(unconsolidatedEvents);
    auditHandler_.getAuthzEvents().addAll(consolidatedEvents.values());
  }

  public void applyDeduplicatedAuthzEvents() {
    auditHandler_.getAuthzEvents().addAll(deduplicatedAuditEvents_.values());
  }

  public TSessionState getSessionState() { return sessionState_; }
}
