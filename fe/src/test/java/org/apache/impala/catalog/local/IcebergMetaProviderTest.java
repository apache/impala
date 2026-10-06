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

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.impala.catalog.iceberg.IcebergRESTCatalog;
import org.apache.thrift.TException;
import org.junit.Test;

/**
 * Tests that IcebergMetaProvider reports REST failures to load a table as TException,
 * which LocalDb turns into a failed table that DROP TABLE can still analyze.
 */
public class IcebergMetaProviderTest {
  private static final String CATALOG_NAME = "rest-1";

  @Test
  public void testUnreadableTable() throws Exception {
    assertLoadFails(new ServiceFailureException("Cannot read metadata"));
  }

  @Test
  public void testMissingTable() throws Exception {
    assertLoadFails(new NoSuchTableException("Table does not exist: db.tbl"));
  }

  @Test
  public void testMissingNamespace() throws Exception {
    // The namespace disappeared after its tables were listed.
    assertLoadFails(new NoSuchNamespaceException("Namespace does not exist: db"));
  }

  private static void assertLoadFails(RuntimeException restError) throws Exception {
    IcebergRESTCatalog restCatalog = mock(IcebergRESTCatalog.class);
    when(restCatalog.getName()).thenReturn(CATALOG_NAME);
    when(restCatalog.loadTable(any(TableIdentifier.class), isNull(), isNull()))
        .thenThrow(restError);
    IcebergMetaProvider provider =
        new IcebergMetaProvider(new Properties(), restCatalog, CATALOG_NAME);

    try {
      provider.loadTable("db", "tbl");
      fail("Expected the REST failure to be reported as TException");
    } catch (TException e) {
      assertTrue(e.getMessage().contains("Error loading Iceberg table db.tbl"));
      assertSame(restError, e.getCause());
    }
    assertNull(provider.getTableIfPresent("db", "tbl"));
  }
}
