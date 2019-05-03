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

import java.util.Map;

/**
 * Contains utility functions for working with Acid tables.
 * <p>
 * The code is mostly copy pasted from Hive. Ideally we should use the
 * the code directly from Hive.
 * </p>
 */
public class AcidUtils {
  // Constant also defined in TransactionalValidationListener
  public static final String INSERTONLY_TRANSACTIONAL_PROPERTY = "insert_only";
  // Constant also defined in hive_metastoreConstants
  public static final String TABLE_IS_TRANSACTIONAL = "transactional";
  public static final String TABLE_TRANSACTIONAL_PROPERTIES = "transactional_properties";


  // The code is same as what exists in AcidUtils.java in hive-exec.
  // Ideally we should move the AcidUtils code from hive-exec into
  // hive-standalone-metastore or some other jar and use it here.
  private static boolean isInsertOnlyTable(Map<String, String> props) {
    Preconditions.checkNotNull(props);
    if (!isTransactionalTable(props)) {
      return false;
    }
    String transactionalProp = props.get(TABLE_TRANSACTIONAL_PROPERTIES);
    return transactionalProp != null && INSERTONLY_TRANSACTIONAL_PROPERTY.
        equalsIgnoreCase(transactionalProp);
  }

  public static boolean isTransactionalTable(Map<String, String> props) {
    Preconditions.checkNotNull(props);
    String tableIsTransactional = props.get(TABLE_IS_TRANSACTIONAL);
    if (tableIsTransactional == null) {
      tableIsTransactional = props.get(TABLE_IS_TRANSACTIONAL.toUpperCase());
    }
    return tableIsTransactional != null && tableIsTransactional.equalsIgnoreCase("true");
  }

  public static boolean isFullAcidTable(Map<String, String> props) {
    return isTransactionalTable(props) && !isInsertOnlyTable(props);
  }

}
