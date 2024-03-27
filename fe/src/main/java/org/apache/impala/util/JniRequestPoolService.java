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

package org.apache.impala.util;

import com.google.common.base.Preconditions;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TErrorCode;
import org.apache.hadoop.security.Groups;
import org.apache.impala.thrift.TGetHadoopGroupsRequest;
import org.apache.impala.thrift.TGetHadoopGroupsResponse;
import org.apache.impala.thrift.TPoolConfigParams;
import org.apache.impala.thrift.TPoolConfig;
import org.apache.impala.thrift.TResolveRequestPoolParams;
import org.apache.impala.thrift.TResolveRequestPoolResult;
import org.apache.impala.thrift.TStatus;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * JNI interface for RequestPoolService.
 */
public class JniRequestPoolService {
  final static Logger LOG = LoggerFactory.getLogger(JniRequestPoolService.class);

  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  // A single instance is created by the backend and lasts the duration of the process.
  private final RequestPoolService requestPoolService_;


  private static final Configuration CONF = new Configuration();
  private static final Groups GROUPS = Groups.getUserToGroupsMappingService(CONF);

  /**
   * Creates a RequestPoolService instance with a configuration containing the specified
   * fair-scheduler.xml and llama-site.xml.
   *
   * @param fsAllocationPath path to the fair scheduler allocation file.
   * @param sitePath path to the configuration file.
   */
  public JniRequestPoolService(byte[] thriftBackendConfig, final String fsAllocationPath,
      final String sitePath, boolean isBackendTest) throws ImpalaException {
    Preconditions.checkNotNull(fsAllocationPath);
    TBackendGflags cfg = new TBackendGflags();
    JniUtil.deserializeThrift(protocolFactory_, cfg, thriftBackendConfig);

    BackendConfig.create(cfg, false);
    requestPoolService_ =
        RequestPoolService.getInstance(fsAllocationPath, sitePath, isBackendTest);
  }

  /**
   * Starts the RequestPoolService instance. It does the initial loading of the
   * configuration and starts the automatic reloading.
   */
  @SuppressWarnings("unused") // called from C++
  public void start() {
    requestPoolService_.start();
  }

  /**
   * Resolves a user and pool to the pool specified by the allocation placement policy
   * and checks if the user is authorized to submit requests.
   *
   * @param thriftResolvePoolParams Serialized {@link TResolveRequestPoolParams}
   * @return serialized {@link TResolveRequestPoolResult}
   */
  @SuppressWarnings("unused") // called from C++
  public byte[] resolveRequestPool(byte[] thriftResolvePoolParams)
      throws ImpalaException {
    TResolveRequestPoolParams resolvePoolParams = new TResolveRequestPoolParams();
    JniUtil.deserializeThrift(
        protocolFactory_, resolvePoolParams, thriftResolvePoolParams);
    TResolveRequestPoolResult result =
        requestPoolService_.resolveRequestPool(resolvePoolParams);
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Gets the pool configuration values for the specified pool.
   *
   * @param thriftPoolConfigParams Serialized {@link TPoolConfigParams}
   * @return serialized {@link TPoolConfig}
   */
  @SuppressWarnings("unused") // called from C++
  public byte[] getPoolConfig(byte[] thriftPoolConfigParams) throws ImpalaException {
    TPoolConfigParams poolConfigParams = new TPoolConfigParams();
    JniUtil.deserializeThrift(protocolFactory_, poolConfigParams, thriftPoolConfigParams);
    TPoolConfig result = requestPoolService_.getPoolConfig(poolConfigParams.getPool());
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns the list of Hadoop groups for the given user name.
   */
  public static byte[] getHadoopGroupsInternal(byte[] serializedRequest)
      throws ImpalaException {
    TGetHadoopGroupsRequest request = new TGetHadoopGroupsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, serializedRequest);
    TGetHadoopGroupsResponse result = new TGetHadoopGroupsResponse();
    String user  = request.getUser();
    String injectedGroups = BackendConfig.INSTANCE.getInjectedGroupMembersDebugOnly();
    if (StringUtils.isEmpty(injectedGroups)) {
      try {
        result.setGroups(GROUPS.getGroups(user));
      } catch (IOException e) {
        // HACK: https://issues.apache.org/jira/browse/HADOOP-15505
        // There is no easy way to know if no groups found for a user
        // other than reading the exception message.
        if (e.getMessage().startsWith("No groups found for user")) {
          result.setGroups(Collections.emptyList());
        } else {
          LOG.error("Error getting Hadoop groups for user: " + request.getUser(), e);
          throw new InternalException(e.getMessage());
        }
      }
    } else {
      List<String> groups = JniUtil.decodeInjectedGroups(injectedGroups, user);
      LOG.info("getHadoopGroups returns injected groups " + groups + " for user " + user);
      result.setGroups(groups);
    }
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Returns the list of Hadoop groups for the given user name.
   */
  public byte[] getHadoopGroups(byte[] serializedRequest) throws ImpalaException {
    return getHadoopGroupsInternal(serializedRequest);
  }
}
