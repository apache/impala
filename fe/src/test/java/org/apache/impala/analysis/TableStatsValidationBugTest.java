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

import org.apache.impala.common.FrontendTestBase;
import org.junit.Test;

/**
 * Test case for IMPALA-12918 fix.
 *
 * Validates that non-numeric values for numRows, totalSize, and rawDataSize
 * table properties are rejected during ALTER TABLE operations.
 */
public class TableStatsValidationBugTest extends FrontendTestBase {

    @Test
    public void testEmptyStringValuesRejected() {
        // Empty string for numRows should fail
        AnalysisError("alter table functional.alltypes set tblproperties('numRows'='')",
                "Table property 'numRows' must have a valid numeric value, got empty value.");

        // Empty string for totalSize should fail
        AnalysisError("alter table functional.alltypes set tblproperties('totalSize'='')",
                "Table property 'totalSize' must have a valid numeric value, got empty value.");

        // Empty string for rawDataSize should fail
        AnalysisError("alter table functional.alltypes set tblproperties('rawDataSize'='')",
                "Table property 'rawDataSize' must have a valid numeric value, got empty value.");
    }

    @Test
    public void testNonNumericValuesRejected() {
        // Non-numeric value for numRows should fail
        AnalysisError("alter table functional.alltypes set tblproperties('numRows'='abc')",
                "Table property 'numRows' must have a valid numeric value, got 'abc'.");

        // Non-numeric value for totalSize should fail
        AnalysisError("alter table functional.alltypes set tblproperties('totalSize'='not_a_number')",
                "Table property 'totalSize' must have a valid numeric value, got 'not_a_number'.");

        // Non-numeric value for rawDataSize should fail
        AnalysisError("alter table functional.alltypes set tblproperties('rawDataSize'='xyz')",
                "Table property 'rawDataSize' must have a valid numeric value, got 'xyz'.");
    }

    @Test
    public void testWhitespaceOnlyValuesRejected() {
        // Whitespace-only values should fail
        AnalysisError("alter table functional.alltypes set tblproperties('numRows'='   ')",
                "Table property 'numRows' must have a valid numeric value, got empty value.");
    }

    @Test
    public void testValidNumericValuesAccepted() {
        // Valid numeric values should pass
        AnalyzesOk("alter table functional.alltypes set tblproperties('numRows'='100')");
        AnalyzesOk("alter table functional.alltypes set tblproperties('numRows'='0')");
        AnalyzesOk("alter table functional.alltypes set tblproperties('numRows'='-1')");
        AnalyzesOk("alter table functional.alltypes set tblproperties('totalSize'='1000')");
        AnalyzesOk("alter table functional.alltypes set tblproperties('rawDataSize'='500')");
    }

    @Test
    public void testPartitionLevelStatsValidated() {
        // Partition-level stats should also be validated
        AnalysisError("alter table functional.alltypes partition(year=2010, month=11) " +
                "set tblproperties('numRows'='')",
                "Table property 'numRows' must have a valid numeric value, got empty value.");

        // Valid partition-level stats should pass
        AnalyzesOk("alter table functional.alltypes partition(year=2010, month=11) " +
                "set tblproperties('numRows'='100')");
    }
}
