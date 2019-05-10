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

import org.apache.ranger.audit.model.AuthzAuditEvent;
import org.apache.ranger.plugin.audit.RangerDefaultAuditHandler;
import org.apache.ranger.plugin.policyengine.RangerAccessRequest;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.policyengine.RangerAccessResultProcessor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Impala implementation of {@link RangerDefaultAuditHandler}. This audit handler batches
 * the audit events and flush them at the end via an explicit {@link #flush()} ()} method.
 * Most of the implementation here was copied from Hive/Ranger plugin code.
 *
 * This class is scoped once per-statement and the instance is not meant to be used by
 * multiple threads.
 */
public class RangerBufferAuditHandler implements RangerAccessResultProcessor {
  private final RangerDefaultAuditHandler auditHandler_ = new RangerDefaultAuditHandler();
  private final List<AuthzAuditEvent> auditEvents_ = new ArrayList<>();

  public static class AutoFlush extends RangerBufferAuditHandler
      implements AutoCloseable {
    @Override
    public void close() {
      super.flush();
    }
  }

  /**
   * Creates an instance of {@link RangerBufferAuditHandler} that will do an auto-flush.
   * Use it with try-resource.
   */
  public static AutoFlush autoFlush() {
    return new AutoFlush();
  }

  @Override
  public void processResult(RangerAccessResult result) {
    processResults(Collections.singletonList(result));
  }

  @Override
  public void processResults(Collection<RangerAccessResult> results) {
    auditEvents_.addAll(createAuditEvents(results));
  }

  /**
   * Flushes the audit events.
   */
  public void flush() {
    // When the first a failure, we only want to log the first failure.
    Optional<AuthzAuditEvent> firstFailure = auditEvents_.stream()
        .filter(evt -> evt.getAccessResult() == 0)
        .findFirst();
    if (firstFailure.isPresent()) {
      auditEvents_.clear();
      auditEvents_.add(firstFailure.get());
    }
    auditEvents_.forEach(event -> auditHandler_.logAuthzAudit(event));
  }

  private AuthzAuditEvent createAuditEvent(RangerAccessResult result) {
    RangerAccessRequest request = result.getAccessRequest();
    RangerAccessResource resource = request.getResource();
    String resourceType = resource != null ? resource.getLeafName() : null;

    AuthzAuditEvent auditEvent = auditHandler_.getAuthzEvents(result);
    auditEvent.setAccessType(request.getAccessType());
    auditEvent.setResourcePath(resource != null ? resource.getAsString() : null);
    if (resourceType != null) {
      auditEvent.setResourceType("@" + resourceType);
    }
    return auditEvent;
  }

  /**
   * Creates list of {@link AuthzAuditEvent} for a given list of
   * {@link RangerAccessResult}. Non-auditable results will be ignored. If there is
   * at least one access denied error, only that event will returned. Multiple policies
   * with the same policy ID will be grouped together.
   */
  private List<AuthzAuditEvent> createAuditEvents(
      Collection<RangerAccessResult> results) {
    List<AuthzAuditEvent> auditEvents = new ArrayList<>();
    for (RangerAccessResult result : results) {
      if (!result.getIsAudited()) continue; // ignore non-auditable result
      auditEvents.add(createAuditEvent(result));
    }
    return auditEvents;
  }

  protected List<AuthzAuditEvent> getAuthzEvents() { return auditEvents_; }
}
