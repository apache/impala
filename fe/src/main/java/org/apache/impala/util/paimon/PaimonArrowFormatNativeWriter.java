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

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.paimon.arrow.vector.ArrowCStruct;
import org.apache.paimon.arrow.vector.ArrowFormatCWriter;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.types.RowType;

/**
 * The wrapper of {@link PaimonArrowFormatWriter} to expose JVM off heap address to
 * BE.
 * TODO: this class is based on ${@link ArrowFormatCWriter} to allow the customization
 *       of Field writer. will remove if relevant PR is accepted by paimon
 *       community. Refer to
 *       ${@link <a href="https://github.com/apache/paimon/pull/6695">...</a>}
 *       for more detail.
 */
public class PaimonArrowFormatNativeWriter implements AutoCloseable {
  // arrow array vector
  private final ArrowArray array_;
  // arrow schema
  private final ArrowSchema schema_;
  // arrow RecordBatch writer.
  private final PaimonArrowFormatWriter realWriter_;

  public PaimonArrowFormatNativeWriter(
      RowType rowType, int writeBatchSize, boolean caseSensitive) {
    this(new PaimonArrowFormatWriter(rowType, writeBatchSize, caseSensitive));
  }

  public PaimonArrowFormatNativeWriter(RowType rowType, int writeBatchSize,
      boolean caseSensitive, BufferAllocator allocator) {
    this(new PaimonArrowFormatWriter(rowType, writeBatchSize, caseSensitive, allocator));
  }

  private PaimonArrowFormatNativeWriter(PaimonArrowFormatWriter arrowFormatWriter) {
    this.realWriter_ = arrowFormatWriter;
    BufferAllocator allocator = realWriter_.getAllocator();
    array_ = ArrowArray.allocateNew(allocator);
    schema_ = ArrowSchema.allocateNew(allocator);
  }

  public boolean write(InternalRow currentRow) { return realWriter_.write(currentRow); }

  public ArrowCStruct flush() {
    realWriter_.flush();
    VectorSchemaRoot vectorSchemaRoot = realWriter_.getVectorSchemaRoot();
    return PaimonArrowUtils.serializeToCStruct(
        vectorSchemaRoot, array_, schema_, realWriter_.getAllocator());
  }

  public void reset() { realWriter_.reset(); }

  public boolean empty() { return realWriter_.empty(); }

  public void release() {
    array_.release();
    schema_.release();
  }

  @Override
  public void close() {
    array_.close();
    schema_.close();
    realWriter_.close();
  }

  public VectorSchemaRoot getVectorSchemaRoot() {
    return realWriter_.getVectorSchemaRoot();
  }

  public BufferAllocator getAllocator() { return realWriter_.getAllocator(); }
}
