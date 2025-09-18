// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.apache.impala.calcite.rel.util;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.Type;
import org.apache.impala.calcite.type.ImpalaTypeConverter;
import org.apache.impala.common.ImpalaException;

import java.util.ArrayList;
import java.util.List;

/**
 * TupleDescriptorFactory creates an ImpalaTupleDescriptor needed for a PlanNode.
 */
public class TupleDescriptorFactory {

  private final String tupleLabel;
  private final List<RelDataTypeField> relDataTypeFields;
  private final List<String> fieldLabels;
  private final List<ColumnStats> columnStats_;

  public TupleDescriptorFactory(String tupleLabel, RelDataType rowType) {
    this(tupleLabel, getLabelsFromRelDataType(rowType), rowType.getFieldList(), null);
  }

  public TupleDescriptorFactory(String tupleLabel, RelDataType rowType,
      List<ColumnStats> columnStats) {
    this(tupleLabel, getLabelsFromRelDataType(rowType), rowType.getFieldList(),
        columnStats);
  }

  private TupleDescriptorFactory(String tupleLabel, List<String> fieldLabels,
      List<RelDataTypeField> relDataTypeFields, List<ColumnStats> columnStats) {
    this.tupleLabel = tupleLabel;
    this.fieldLabels = fieldLabels;
    this.relDataTypeFields = relDataTypeFields;
    this.columnStats_ = columnStats;
    Preconditions.checkArgument(fieldLabels.size() == relDataTypeFields.size());
  }

  /**
   * Create the TupleDescriptor. This method will mutate the analyzer by adding its
   * TupleDescriptor and associated SlotDescriptors.
   */
  public TupleDescriptor create(Analyzer analyzer) throws ImpalaException {
    TupleDescriptor tupleDesc = analyzer.getDescTbl().createTupleDescriptor(tupleLabel);
    tupleDesc.setIsMaterialized(true);

    for (int i = 0; i < relDataTypeFields.size(); i++) {
      RelDataTypeField relDataTypeField = relDataTypeFields.get(i);
      String fieldLabel = fieldLabels.get(i);
      SlotDescriptor slotDesc = analyzer.addSlotDescriptor(tupleDesc);
      Type impalaType = ImpalaTypeConverter.createImpalaType(relDataTypeField.getType());
      slotDesc.setType(impalaType);
      slotDesc.setLabel(fieldLabel);
      slotDesc.setIsMaterialized(true);
      if (columnStats_ != null) {
        slotDesc.setStats(columnStats_.get(i));
      }
      if (!relDataTypeField.getType().isNullable()) {
        slotDesc.setIsNullable(false);
      }
    }
    tupleDesc.computeMemLayout();
    return tupleDesc;
  }

  private static List<String> getLabelsFromRelDataType(RelDataType rowType) {
    List<String> fieldLabels = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      fieldLabels.add(field.getName());
    }
    return fieldLabels;
  }
}
