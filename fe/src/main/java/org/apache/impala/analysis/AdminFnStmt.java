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

package org.apache.impala.analysis;

import java.util.ArrayList;
import java.util.List;

import org.apache.impala.authorization.AuthorizationConfig;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TAdminRequest;
import org.apache.impala.thrift.TAdminRequestType;
import org.apache.impala.thrift.TEventProcessorCmdParams;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TShutdownParams;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Represents an administrative function call, e.g. ": shutdown('hostname:123')".
 *
 * This "admin statement" framework provides a way to expand the set of supported admin
 * statements without modifying the SQL grammar. For now, the only supported functions are
 * shutdown() and event_processor(), so the logic in here is not generic.
 */
public class AdminFnStmt extends StatementBase {
  // Name of the function. Validated during analysis.
  private final String fnName_;

  // Arguments to the function. Always non-null.
  private final List<Expr> params_;

  private TAdminRequestType type_;

  // Parameters for the shutdown() command.
  // Address of the backend to shut down, If 'backend_' is null, that means the current
  // server. If 'backend_.port' is 0, we assume the backend has the same port as this
  // impalad.
  private TNetworkAddress backend_;
  // Deadline in seconds. -1 if no deadline specified.
  private long deadlineSecs_;

  // Parameters for the event_processor() command
  // Currently supported actions: pause, start
  private String action_;
  // Event id to start at. Defaults to reusing the last synced event id.
  // Set to -1 for using the latest event id.
  private long event_id_ = 0;

  public AdminFnStmt(String fnName, List<Expr> params) {
    this.fnName_ = fnName;
    this.params_ = params;
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder();
    sb.append(":").append(fnName_).append("(");
    List<String> paramsSql = new ArrayList<>();
    for (Expr param : params_) paramsSql.add(param.toSql(options));
    sb.append(Joiner.on(", ").join(paramsSql));
    sb.append(")");
    return sb.toString();
  }

  public TAdminRequest toThrift() throws InternalException {
    TAdminRequest result = new TAdminRequest();
    result.type = type_;
    if (type_ == TAdminRequestType.SHUTDOWN) {
      result.shutdown_params = new TShutdownParams();
      if (backend_ != null) result.shutdown_params.setBackend(backend_);
      if (deadlineSecs_ != -1) result.shutdown_params.setDeadline_s(deadlineSecs_);
    } else if (type_ == TAdminRequestType.EVENT_PROCESSOR) {
      result.event_processor_cmd_params = new TEventProcessorCmdParams(action_);
      if (event_id_ != 0) result.event_processor_cmd_params.setEvent_id(event_id_);
    } else {
      Preconditions.checkState(false, "Unsupported TAdminRequest type %s", type_);
    }
    return result;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    for (Expr param : params_) param.analyze(analyzer);
    if (fnName_.equalsIgnoreCase("shutdown")) {
      type_ = TAdminRequestType.SHUTDOWN;
      analyzeShutdown(analyzer);
    } else if (fnName_.equalsIgnoreCase("event_processor")) {
      type_ = TAdminRequestType.EVENT_PROCESSOR;
      analyzeEventProcessorCmd(analyzer);
    } else {
      throw new AnalysisException("Unknown admin function: " + fnName_);
    }
  }

  private void registerPrivReq(Analyzer analyzer) {
    AuthorizationConfig authzConfig = analyzer.getAuthzConfig();
    if (authzConfig.isEnabled()) {
      // Only admins (i.e. user with ALL privilege on server) can execute admin functions.
      String authzServer = authzConfig.getServerName();
      Preconditions.checkNotNull(authzServer);
      analyzer.registerPrivReq(builder -> builder.onServer(authzServer).all().build());
    }
  }

  /**
   * Supports optionally specifying the backend and the deadline: either shutdown(),
   * shutdown('host:port'), shutdown(deadline), shutdown('host:port', deadline).
   */
  private void analyzeShutdown(Analyzer analyzer) throws AnalysisException {
    registerPrivReq(analyzer);

    // TODO: this parsing and type checking logic is specific to the command, similar to
    // handling of other top-level commands. If we add a lot more of these functions we
    // could consider making it generic, similar to handling of normal function calls.
    Pair<Expr, Expr> args = getShutdownArgs();
    Expr backendExpr = args.first;
    Expr deadlineExpr = args.second;
    backend_ = null;
    deadlineSecs_ = -1;
    if (backendExpr != null) {
      if (!(backendExpr instanceof StringLiteral)) {
        throw new AnalysisException(
            "Invalid backend, must be a string literal: " + backendExpr.toSql());
      }
      backend_ = parseBackendAddress(((StringLiteral) backendExpr).getUnescapedValue());
    }
    if (deadlineExpr != null) {
      deadlineSecs_ = deadlineExpr.evalToNonNegativeInteger(analyzer, "deadline");
    }
  }

  // Return a pair of the backend and deadline arguments, null if not present.
  private Pair<Expr, Expr> getShutdownArgs() throws AnalysisException {
    if (params_.size() == 0) {
      return Pair.create(null, null);
    } else if (params_.size() == 1) {
      if (params_.get(0).getType().isStringType()) {
        return Pair.create(params_.get(0), null);
      } else {
        return Pair.create(null, params_.get(0));
      }
    } else if (params_.size() == 2) {
      return Pair.create(params_.get(0), params_.get(1));
    } else {
      throw new AnalysisException("Shutdown takes 0, 1 or 2 arguments: " + toSql());
    }
  }

  // Parse the backend and optional port from 'backend'. Port is set to 0 if not set in
  // the string.
  private TNetworkAddress parseBackendAddress(String backend) throws AnalysisException {
    TNetworkAddress result = new TNetworkAddress();
    // Extract host and port from backend string.
    String[] toks = backend.trim().split(":");
    if (toks.length == 0 || toks.length > 2) {
      throw new AnalysisException("Invalid backend address: " + backend);
    }
    result.hostname = toks[0];
    result.port = 0;
    if (toks.length == 2) {
      try {
        result.port = Integer.parseInt(toks[1]);
      } catch (NumberFormatException nfe) {
        throw new AnalysisException(
            "Invalid port number in backend address: " + backend);
      }
    }
    return result;
  }

  private void analyzeEventProcessorCmd(Analyzer analyzer) throws AnalysisException {
    registerPrivReq(analyzer);

    if (params_.isEmpty() || params_.size() > 2) {
      throw new AnalysisException("event_processor() takes 1 or 2 arguments: " + toSql());
    }
    if (!(params_.get(0) instanceof StringLiteral)) {
      throw new AnalysisException("First argument of event_processor() should be STRING");
    }
    action_ = ((StringLiteral)params_.get(0)).getStringValue().toUpperCase();
    if (params_.size() > 1) {
      event_id_ = params_.get(1).evalToInteger(analyzer, "event_id");
    }
  }

  @Override
  public boolean requiresHmsMetadata() { return false; }
}
