// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.catalog.CatalogServiceCatalog;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.thrift.TCatalogUpdateResult;
import com.cloudera.impala.thrift.TDdlExecRequest;
import com.cloudera.impala.thrift.TGetAllCatalogObjectsRequest;
import com.cloudera.impala.thrift.TGetAllCatalogObjectsResponse;
import com.cloudera.impala.thrift.TGetDbsParams;
import com.cloudera.impala.thrift.TGetDbsResult;
import com.cloudera.impala.thrift.TGetTablesParams;
import com.cloudera.impala.thrift.TGetTablesResult;
import com.cloudera.impala.thrift.TResetMetadataRequest;
import com.cloudera.impala.thrift.TResetMetadataResponse;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import com.cloudera.impala.thrift.TUniqueId;
import com.cloudera.impala.thrift.TUpdateMetastoreRequest;
import com.google.common.base.Preconditions;

/**
 * JNI-callable interface for the CatalogService. The main point is to serialize
 * and de-serialize thrift structures between C and Java parts of the CatalogService.
 */
public class JniCatalog {
  private final static Logger LOG = LoggerFactory.getLogger(JniCatalog.class);
  private final static TBinaryProtocol.Factory protocolFactory =
      new TBinaryProtocol.Factory();
  private final CatalogServiceCatalog catalog_;
  private final DdlExecutor ddlExecutor_;

  // A unique identifier for this instance of the Catalog Service.
  private static final TUniqueId catalogServiceId_ = generateId();

  private static TUniqueId generateId() {
    UUID uuid = UUID.randomUUID();
    return new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
  }

  public JniCatalog() {
    catalog_ = new CatalogServiceCatalog(getServiceId());
    ddlExecutor_ = new DdlExecutor(catalog_);
  }

  public static TUniqueId getServiceId() { return catalogServiceId_; }

  /**
   * Gets all catalog objects
   */
  public byte[] getCatalogObjects(byte[] req) throws ImpalaException, TException {
    TGetAllCatalogObjectsRequest request = new TGetAllCatalogObjectsRequest();
    JniUtil.deserializeThrift(protocolFactory, request, req);

    TGetAllCatalogObjectsResponse resp =
        catalog_.getCatalogObjects(request.getFrom_version());

    TSerializer serializer = new TSerializer(protocolFactory);
    return serializer.serialize(resp);
  }

  /**
   * Executes the given DDL request and returns the result.
   */
  public byte[] execDdl(byte[] thriftDdlExecReq) throws ImpalaException {
    TDdlExecRequest params = new TDdlExecRequest();
    JniUtil.deserializeThrift(protocolFactory, params, thriftDdlExecReq);
    TSerializer serializer = new TSerializer(protocolFactory);
    try {
      return serializer.serialize(ddlExecutor_.execDdlRequest(params));
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Execute a reset metadata statement.
   */
  public byte[] resetMetadata(byte[] thriftResetMetadataReq)
      throws ImpalaException, TException {
    TResetMetadataRequest req = new TResetMetadataRequest();
    JniUtil.deserializeThrift(protocolFactory, req, thriftResetMetadataReq);
    TResetMetadataResponse resp = new TResetMetadataResponse();
    resp.setResult(new TCatalogUpdateResult());
    resp.getResult().setCatalog_service_id(getServiceId());

    if (req.isSetTable_name()) {
      resp.result.setVersion(catalog_.resetTable(req.getTable_name(),
          req.isIs_refresh()));
    } else {
      // Invalidate the catalog if no table name is provided.
      Preconditions.checkArgument(!req.isIs_refresh());
      resp.result.setVersion(catalog_.reset());
    }
    resp.getResult().setStatus(
        new TStatus(TStatusCode.OK, new ArrayList<String>()));

    TSerializer serializer = new TSerializer(protocolFactory);
    return serializer.serialize(resp);
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialized TGetTablesResult object.
   */
  public byte[] getDbNames(byte[] thriftGetTablesParams) throws ImpalaException,
      TException {
    TGetDbsParams params = new TGetDbsParams();
    JniUtil.deserializeThrift(protocolFactory, params, thriftGetTablesParams);
    TGetDbsResult result = new TGetDbsResult();
    result.setDbs(catalog_.getDbNames(null));
    TSerializer serializer = new TSerializer(protocolFactory);
    return serializer.serialize(result);
  }

  /**
   * Returns a list of table names matching an optional pattern.
   * The argument is a serialized TGetTablesParams object.
   * The return type is a serialized TGetTablesResult object.
   */
  public byte[] getTableNames(byte[] thriftGetTablesParams) throws ImpalaException,
      TException {
    TGetTablesParams params = new TGetTablesParams();
    JniUtil.deserializeThrift(protocolFactory, params, thriftGetTablesParams);
    List<String> tables = catalog_.getTableNames(params.db, params.pattern);
    TGetTablesResult result = new TGetTablesResult();
    result.setTables(tables);
    TSerializer serializer = new TSerializer(protocolFactory);
    return serializer.serialize(result);
  }

  /**
   * Process any updates to the metastore required after a query executes.
   * The argument is a serialized TCatalogUpdate.
   */
  public byte[] updateMetastore(byte[] thriftUpdateCatalog) throws ImpalaException,
      TException  {
    TUpdateMetastoreRequest request = new TUpdateMetastoreRequest();
    JniUtil.deserializeThrift(protocolFactory, request, thriftUpdateCatalog);
    TSerializer serializer = new TSerializer(protocolFactory);
    return serializer.serialize(ddlExecutor_.updateMetastore(request));
  }
}