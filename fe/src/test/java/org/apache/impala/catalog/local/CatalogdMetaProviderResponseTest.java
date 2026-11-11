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

package org.apache.impala.catalog.local;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;

import org.apache.impala.thrift.CatalogLookupStatus;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TPartialTableInfo;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

/** Catalog replies use the same bounded JNI decoder as catalog statistics writes. */
public class CatalogdMetaProviderResponseTest {
  @Test
  public void testLargePartitionResponse() throws TException {
    String name = String.join("", Collections.nCopies(8192, "p"));
    TPartialPartitionInfo part = new TPartialPartitionInfo(1L);
    part.setName(name);
    TPartialTableInfo table = new TPartialTableInfo();
    table.setPartitions(Collections.nCopies(12801, part));
    TGetPartialCatalogObjectResponse response = new TGetPartialCatalogObjectResponse();
    response.setTable_info(table);
    response.setObject_version_number(42);
    byte[] bytes = new TSerializer().serialize(response);
    assertTrue(bytes.length > TConfiguration.DEFAULT_MAX_MESSAGE_SIZE);
    assertEquals(response, CatalogdMetaProvider.deserializeResponse(bytes));
  }

  @Test
  public void testSmallResponsesPreserveLookupStatus() throws TException {
    for (CatalogLookupStatus status : CatalogLookupStatus.values()) {
      TGetPartialCatalogObjectResponse response = new TGetPartialCatalogObjectResponse();
      response.setLookup_status(status);
      byte[] bytes = new TSerializer().serialize(response);
      assertEquals(response, CatalogdMetaProvider.deserializeResponse(bytes));
    }
  }

  @Test
  public void testTruncatedResponseRemainsThriftException() throws TException {
    TPartialPartitionInfo part = new TPartialPartitionInfo(1L);
    part.setName("partition");
    TPartialTableInfo table = new TPartialTableInfo();
    table.setPartitions(Collections.singletonList(part));
    TGetPartialCatalogObjectResponse response = new TGetPartialCatalogObjectResponse();
    response.setTable_info(table);
    byte[] bytes = new TSerializer().serialize(response);
    for (int length : new int[] {0, 1, bytes.length / 2}) {
      try {
        CatalogdMetaProvider.deserializeResponse(Arrays.copyOf(bytes, length));
        fail("Truncated catalog reply was accepted");
      } catch (TTransportException expected) {
        // Preserve the decoder exception rather than wrapping it twice.
      }
    }
  }
}
