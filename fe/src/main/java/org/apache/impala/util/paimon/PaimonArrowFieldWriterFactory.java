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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactory;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.DataGetters;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.columnar.ColumnVector;
import org.apache.paimon.data.columnar.DecimalColumnVector;
import org.apache.paimon.types.DecimalType;

import javax.annotation.Nullable;

/**
 * It is an extension of {@link ArrowFieldWriterFactoryVisitor} class.
 * To change the decimal field writer behavior.
 * Will directly convert paimon decimal type to arrow binary value.
 */
public class PaimonArrowFieldWriterFactory extends ArrowFieldWriterFactoryVisitor {
  @Override
  public ArrowFieldWriterFactory visit(DecimalType decimalType) {
    return (fieldVector, isNullable)
               -> new DecimalWriter(fieldVector, decimalType.getPrecision(),
                   decimalType.getScale(), isNullable);
  }

  public static class DecimalWriter extends ArrowFieldWriter {
    // decimal precision
    private final int precision_;
    // decimal scale
    private final int scale_;

    public DecimalWriter(
        FieldVector fieldVector, int precision, int scale, boolean isNullable) {
      super(fieldVector, isNullable);
      this.precision_ = precision;
      this.scale_ = scale;
    }

    protected void doWrite(ColumnVector columnVector, @Nullable int[] pickedInColumn,
        int startIndex, int batchRows) {
      VarBinaryVector decimalVector = (VarBinaryVector) this.fieldVector;

      for (int i = 0; i < batchRows; ++i) {
        int row = this.getRowNumber(startIndex, i, pickedInColumn);
        if (columnVector.isNullAt(row)) {
          decimalVector.setNull(i);
        } else {
          Decimal value = ((DecimalColumnVector) columnVector)
                              .getDecimal(row, this.precision_, this.scale_);
          byte[] bytes = value.toUnscaledBytes();
          decimalVector.setSafe(i, bytes);
        }
      }
    }

    protected void doWrite(int rowIndex, DataGetters getters, int pos) {
      ((VarBinaryVector) this.fieldVector)
          .setSafe(rowIndex,
              getters.getDecimal(pos, this.precision_, this.scale_).toUnscaledBytes());
    }
  }
}
