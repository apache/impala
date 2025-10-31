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

package org.apache.impala.util.paimon;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.paimon.arrow.ArrowFieldTypeConversion;
import org.apache.paimon.types.DecimalType;

/**
 * It is an extension of {@link ArrowFieldTypeConversion.ArrowFieldTypeVisitor} class.
 * To change the decimal conversion behavior.
 * Paimon decimal type will convert to arrow Decimal128 type, it involves byte copy
 * and padding, which will cause additional overhead to pass data to BE.
 * To Eliminate the overhead,  will directly pass the decimal unscaled bytes to BE, So
 * Arrow binary type will be used instead of Decimal128 data type.
 */
public class PaimonArrowFieldTypeFactory
    extends ArrowFieldTypeConversion.ArrowFieldTypeVisitor {
  @Override
  public FieldType visit(DecimalType decimalType) {
    return new FieldType(decimalType.isNullable(), new ArrowType.Binary(), null);
  }
}
