/*
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

package org.apache.impala.externalfrontend;

import com.google.common.base.Preconditions;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.authorization.AuthorizationFactory;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.service.Frontend;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.service.JniFrontend;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.util.AuthorizationUtil;
import org.apache.thrift.TException;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TSerializer;

public class TestJniFrontend extends JniFrontend {
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  private final Frontend testFrontend_;

  public TestJniFrontend(byte[] thriftBackendConfig, boolean isBackendTest)
      throws ImpalaException, TException {
    super(thriftBackendConfig, isBackendTest);
    final AuthorizationFactory authzFactory =
        AuthorizationUtil.authzFactoryFrom(BackendConfig.INSTANCE);
    testFrontend_ = new Frontend(authzFactory, false);
  }

  /**
   * Jni wrapper for Frontend.createExecRequest(). Accepts a serialized
   * TQueryContext; returns a serialized TQueryExecRequest.
   * This specific version will execute normally with the exception of the
   * 'select 1' query which will instead implement a 'select 42' query, which
   * is so bizarre, that it will prove that we hit this code. '42' also answers
   * the question, "What is the meaning of life?"
   */
  @Override
  public byte[] createExecRequest(byte[] thriftQueryContext)
      throws ImpalaException {
    Preconditions.checkNotNull(testFrontend_);
    TQueryCtx queryCtx = new TQueryCtx();
    JniUtil.deserializeThrift(protocolFactory_, queryCtx, thriftQueryContext);
    if (queryCtx.getClient_request().getStmt().equals("select 1")) {
      queryCtx.getClient_request().setStmt("select 42");
    }

    PlanCtx planCtx = new PlanCtx(queryCtx);
    TExecRequest result = testFrontend_.createExecRequest(planCtx);

    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }
}
