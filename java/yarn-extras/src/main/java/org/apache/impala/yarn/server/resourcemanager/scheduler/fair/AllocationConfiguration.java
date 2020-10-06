/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.yarn.server.resourcemanager.scheduler.fair;
//YARNUTIL: MODIFIED

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.impala.yarn.server.resourcemanager.resource.ResourceWeights;

import com.google.common.annotations.VisibleForTesting;

public class AllocationConfiguration {
  private static final AccessControlList EVERYBODY_ACL = new AccessControlList("*");
  private static final AccessControlList NOBODY_ACL = new AccessControlList(" ");
  private static final ResourceCalculator RESOURCE_CALCULATOR =
      new DefaultResourceCalculator();
  // Minimum resource allocation for each queue
  private final Map<String, Resource> minQueueResources;
  // Maximum amount of resources per queue
  @VisibleForTesting
  final Map<String, Resource> maxQueueResources;

  private final Resource queueMaxResourcesDefault;

  // ACL's for each queue. Only specifies non-default ACL's from configuration.
  private final Map<String, Map<QueueACL, AccessControlList>> queueAcls;

  // Policy for mapping apps to queues
  @VisibleForTesting
  QueuePlacementPolicy placementPolicy;
  
  //Configured queues in the alloc xml
  @VisibleForTesting
  Map<FSQueueType, Set<String>> configuredQueues;

  public AllocationConfiguration(Map<String, Resource> minQueueResources,
      Map<String, Resource> maxQueueResources,
      Map<String, Resource> maxChildQueueResources,
      Map<String, Integer> queueMaxApps, Map<String, Integer> userMaxApps,
      Map<String, ResourceWeights> queueWeights,
      Map<String, Float> queueMaxAMShares, int userMaxAppsDefault,
      int queueMaxAppsDefault, Resource queueMaxResourcesDefault,
      float queueMaxAMShareDefault,
      Map<String, SchedulingPolicy> schedulingPolicies,
      SchedulingPolicy defaultSchedulingPolicy,
      Map<String, Long> minSharePreemptionTimeouts,
      Map<String, Long> fairSharePreemptionTimeouts,
      Map<String, Float> fairSharePreemptionThresholds,
      Map<String, Map<QueueACL, AccessControlList>> queueAcls,
      QueuePlacementPolicy placementPolicy,
      Map<FSQueueType, Set<String>> configuredQueues,
      Set<String> nonPreemptableQueues) {
    this.minQueueResources = minQueueResources;
    this.maxQueueResources = maxQueueResources;
    this.queueMaxResourcesDefault = queueMaxResourcesDefault;
    this.queueAcls = queueAcls;
    this.placementPolicy = placementPolicy;
    this.configuredQueues = configuredQueues;
  }
  
  public AllocationConfiguration(Configuration conf) {
    minQueueResources = new HashMap<>();
    maxQueueResources = new HashMap<>();
    queueMaxResourcesDefault = Resources.unbounded();
    queueAcls = new HashMap<>();
    configuredQueues = new HashMap<>();
    for (FSQueueType queueType : FSQueueType.values()) {
      configuredQueues.put(queueType, new HashSet<String>());
    }
    placementPolicy =
        QueuePlacementPolicy.fromConfiguration(conf, configuredQueues);
  }
  
  /**
   * Get the ACLs associated with this queue. If a given ACL is not explicitly
   * configured, include the default value for that ACL.  The default for the
   * root queue is everybody ("*") and the default for all other queues is
   * nobody ("")
   */
  public AccessControlList getQueueAcl(String queue, QueueACL operation) {
    Map<QueueACL, AccessControlList> queueAcls = this.queueAcls.get(queue);
    if (queueAcls != null) {
      AccessControlList operationAcl = queueAcls.get(operation);
      if (operationAcl != null) {
        return operationAcl;
      }
    }
    return (queue.equals("root")) ? EVERYBODY_ACL : NOBODY_ACL;
  }

  /**
   * Get the minimum resource allocation for the given queue.
   *
   * @param queue the target queue's name
   * @return the min allocation on this queue or {@link Resources#none}
   * if not set
   */
  public Resource getMinResources(String queue) {
    Resource minQueueResource = minQueueResources.get(queue);
    return (minQueueResource == null) ? Resources.none() : minQueueResource;
  }

  /**
   * Set the maximum resource allocation for the given queue.
   *
   * @param queue the target queue
   * @param maxResource the maximum resource allocation
   */
  void setMaxResources(String queue, Resource maxResource) {
    maxQueueResources.put(queue, maxResource);
  }

  /**
   * Get the maximum resource allocation for the given queue. If the max in not
   * set, return the larger of the min and the default max.
   *
   * @param queue the target queue's name
   * @return the max allocation on this queue
   */
  public Resource getMaxResources(String queue) {
    Resource maxQueueResource = maxQueueResources.get(queue);
    if (maxQueueResource == null) {
      Resource minQueueResource = minQueueResources.get(queue);
      if (minQueueResource != null &&
          Resources.greaterThan(RESOURCE_CALCULATOR, Resources.unbounded(),
          minQueueResource, queueMaxResourcesDefault)) {
        return minQueueResource;
      } else {
        return queueMaxResourcesDefault;
      }
    } else {
      return maxQueueResource;
    }
  }

  public boolean hasAccess(String queueName, QueueACL acl,
      UserGroupInformation user) {
    int lastPeriodIndex = queueName.length();
    while (lastPeriodIndex != -1) {
      String queue = queueName.substring(0, lastPeriodIndex);
      if (getQueueAcl(queue, acl).isUserAllowed(user)) {
        return true;
      }

      lastPeriodIndex = queueName.lastIndexOf('.', lastPeriodIndex - 1);
    }
    
    return false;
  }

  public Map<FSQueueType, Set<String>> getConfiguredQueues() {
    return configuredQueues;
  }
  
  public QueuePlacementPolicy getPlacementPolicy() {
    return placementPolicy;
  }
}
