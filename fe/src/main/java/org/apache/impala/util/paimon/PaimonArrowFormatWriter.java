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

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.OversizedAllocationException;
import org.apache.paimon.arrow.vector.ArrowFormatWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriter;
import org.apache.paimon.arrow.writer.ArrowFieldWriterFactoryVisitor;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write paimon Internal rows to arrow RecordBatch batch in Java side.
 * TODO: this class is based on ${@link ArrowFormatWriter} to allow the customization
 *       of Field writer. will remove if relevant PR is accepted by paimon
 *       community. Refer to
 *       ${@link <a href="https://github.com/apache/paimon/pull/6695">...</a>}
 *       for more detail.
 */
public class PaimonArrowFormatWriter implements AutoCloseable {
  private static final Logger LOG =
      LoggerFactory.getLogger(PaimonArrowFormatWriter.class);
  // writer factory
  private static final ArrowFieldWriterFactoryVisitor FIELD_WRITER_FACTORY =
      new PaimonArrowFieldWriterFactory();
  // field type factory
  private static final PaimonArrowFieldTypeFactory FIELD_TYPE_FACTORY =
      new PaimonArrowFieldTypeFactory();
  // arrow vector schema root
  private final VectorSchemaRoot vectorSchemaRoot_;
  // arrow field writers
  private final ArrowFieldWriter[] fieldWriters_;
  // arrow RecordBatch batch size
  private final int batchSize_;
  // buffer allocator.
  private final BufferAllocator allocator_;
  // rowid for current batch.
  private int rowId_;

  public PaimonArrowFormatWriter(
      RowType rowType, int writeBatchSize, boolean caseSensitive) {
    this(rowType, writeBatchSize, caseSensitive, new RootAllocator());
  }

  public PaimonArrowFormatWriter(RowType rowType, int writeBatchSize,
      boolean caseSensitive, BufferAllocator allocator) {
    this(rowType, writeBatchSize, caseSensitive, allocator, FIELD_WRITER_FACTORY);
  }

  public PaimonArrowFormatWriter(RowType rowType, int writeBatchSize,
      boolean caseSensitive, BufferAllocator allocator,
      ArrowFieldWriterFactoryVisitor fieldWriterFactory) {
    this.allocator_ = allocator;

    vectorSchemaRoot_ = PaimonArrowUtils.createVectorSchemaRoot(
        rowType, allocator, caseSensitive, FIELD_TYPE_FACTORY);

    fieldWriters_ = new ArrowFieldWriter[rowType.getFieldCount()];

    for (int i = 0; i < fieldWriters_.length; i++) {
      DataType type = rowType.getFields().get(i).type();
      fieldWriters_[i] = type.accept(fieldWriterFactory)
                             .create(vectorSchemaRoot_.getVector(i), type.isNullable());
    }

    this.batchSize_ = writeBatchSize;
  }

  public void flush() { vectorSchemaRoot_.setRowCount(rowId_); }

  public boolean write(InternalRow currentRow) {
    if (rowId_ >= batchSize_) { return false; }
    for (int i = 0; i < currentRow.getFieldCount(); i++) {
      try {
        fieldWriters_[i].write(rowId_, currentRow, i);
      } catch (OversizedAllocationException | IndexOutOfBoundsException e) {
        // maybe out of memory
        LOG.warn("Arrow field writer failed while writing", e);
        return false;
      }
    }

    rowId_++;
    return true;
  }

  public boolean empty() { return rowId_ == 0; }

  public void reset() {
    for (ArrowFieldWriter fieldWriter : fieldWriters_) { fieldWriter.reset(); }
    rowId_ = 0;
  }

  @Override
  public void close() {
    vectorSchemaRoot_.close();
    allocator_.close();
  }

  public VectorSchemaRoot getVectorSchemaRoot() { return vectorSchemaRoot_; }

  public BufferAllocator getAllocator() { return allocator_; }
}
