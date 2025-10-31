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

package org.apache.impala.util.paimon;

import com.google.common.collect.Lists;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.impala.catalog.paimon.PaimonUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.planner.paimon.PaimonSplit;
import org.apache.impala.thrift.TPaimonJniScanParam;
import org.apache.paimon.arrow.vector.ArrowCStruct;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.thrift.protocol.TBinaryProtocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The Fe Paimon Jni Scanner, used by backend PaimonJniScanner.
 */
public class PaimonJniScanner implements AutoCloseable {
  private final static Logger LOG = LoggerFactory.getLogger(PaimonJniScanner.class);

  public final static int DEFAULT_ROWBATCH_SIZE = 1024;
  public final static long DEFAULT_INITIAL_RESERVATION = 32 * 1024;

  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();
  // Paimon api table.
  private Table table_ = null;
  // Paimon splits assigned to the scanner.
  private List<PaimonSplit> splits_ = null;
  // Paimon schema after projection.
  private RowType projectedSchema_;
  // Paimon data record iterator.
  private RecordReaderIterator<InternalRow> iterator_;
  // batch size;
  int batchSize_;
  // paimon to arrow RecordBatch writer.
  private PaimonArrowFormatNativeWriter writer_;
  // arrow off heap allocator.
  private BufferAllocator bufferAllocator_;
  // total rows metrics.
  private long totalRows_ = 0;
  // upper bound mem limit.
  // -1 means no limit.
  private long allocator_mem_limit_ = -1;

  /**
   * Constructor for PaimonJniScanner, will be called in Open
   * method of BE PaimonJniScanNode.
   * @param jni_scan_param_thrift: thrift form of paimon scan param.
   * */
  public PaimonJniScanner(byte[] jni_scan_param_thrift) {
    TPaimonJniScanParam paimonJniScanParam = new TPaimonJniScanParam();
    try {
      JniUtil.deserializeThrift(
          protocolFactory_, paimonJniScanParam, jni_scan_param_thrift);
    } catch (ImpalaException ex) { LOG.error("failed to get paimon jni scan param"); }
    // table
    table_ = SerializationUtils.deserialize(paimonJniScanParam.getPaimon_table_obj());
    // splits
    splits_ = Lists.newArrayList();
    for (ByteBuffer split_data : paimonJniScanParam.getSplits()) {
      ByteBuffer split_data_serialized = split_data.compact();
      splits_.add(SerializationUtils.deserialize(split_data_serialized.array()));
    }
    // projection field ids
    int[] projectionFieldIds =
        paimonJniScanParam.getProjection().stream().mapToInt(Integer::intValue).toArray();
    // projected fields and schema
    DataField[] projectedFields =
        Arrays.stream(projectionFieldIds)
            .mapToObj(fieldId -> table_.rowType().getField(fieldId))
            .toArray(DataField[] ::new);
    projectedSchema_ = RowType.of(projectedFields);
    // get batch size
    batchSize_ = paimonJniScanParam.getBatch_size();
    if (batchSize_ <= 0) { batchSize_ = DEFAULT_ROWBATCH_SIZE; }
    // get mem limit
    allocator_mem_limit_ = paimonJniScanParam.getMem_limit_bytes();
    String allocatorName =
        "paimonscan_" + table_.uuid() + paimonJniScanParam.getFragment_id().toString();
    // create allocator
    if (allocator_mem_limit_ > 0) {
      bufferAllocator_ = ArrowRootAllocation.rootAllocator().newChildAllocator(
          allocatorName, DEFAULT_INITIAL_RESERVATION, allocator_mem_limit_);
    } else {
      bufferAllocator_ = ArrowRootAllocation.rootAllocator().newChildAllocator(
          allocatorName, DEFAULT_INITIAL_RESERVATION, Long.MAX_VALUE);
    }
    LOG.info(String.format("Open with mem_limit: %d bytes, batch_size:%d rows, "
            + "Projection field ids:%s",
        allocator_mem_limit_, batchSize_, Arrays.toString(projectionFieldIds)));
  }

  /**
   * Perform table splits scanning, will be called in Open
   * method of BE PaimonJniScanNode.
   * */
  public void ScanTable() {
    // If we are on a stack frame that was created through JNI we need to set the context
    // class loader as Paimon might use reflection to dynamically load classes and
    // methods.
    JniUtil.setContextClassLoaderForThisThread(this.getClass().getClassLoader());
    writer_ = new PaimonArrowFormatNativeWriter(
        projectedSchema_, batchSize_, false, bufferAllocator_);
    // Create and scan the metadata table
    initReader();
  }

  /**
   * Get the next arrow batch, will be called in GetNext
   * method of BE PaimonJniScanNode.
   * @param address: return three long values to BE
   *        address[0]: schema address of arrow batch.
   *        address[1]: vector address of arrow batch.
   *        address[2]: offheap memory consumption for current batch.
   * */
  public long GetNextBatch(long[] address) {
    if (!writer_.empty()) { writer_.reset(); }
    int rows = 0;
    for (int i = 0; i < batchSize_; i++) {
      if (iterator_.hasNext()) {
        boolean result = writer_.write(iterator_.next());
        if (result) { rows++; }
      } else {
        break;
      }
    }
    totalRows_ += rows;
    if (rows > 0) {
      ArrowCStruct cStruct = writer_.flush();
      address[0] = cStruct.schemaAddress();
      address[1] = cStruct.arrayAddress();
      address[2] = bufferAllocator_.getAllocatedMemory();
      return rows;
    } else {
      return 0;
    }
  }

  protected boolean initReader() {
    try {
      ReadBuilder readBuilder = table_.newReadBuilder().withReadType(projectedSchema_);
      // Apply push down predicates if present.
      // Currently predicates are always null/empty,
      // All conjuncts are evaluated by the C++ scanner.
      List<Predicate> predicates = splits_.get(0).getPredicates();
      if (predicates != null && !predicates.isEmpty()) {
        readBuilder.withFilter(predicates);
      }
      // Create Iterator for given splits.
      List<Split> splits =
          splits_.stream().map(PaimonSplit::getSplit).collect(Collectors.toList());
      RecordReader<InternalRow> reader = readBuilder.newRead().createReader(splits);
      iterator_ = new RecordReaderIterator<>(reader);
      LOG.info(
          String.format("Reading %d splits for %s", splits.size(), table_.fullName()));
      return true;
    } catch (Exception ex) {
      LOG.error("failed to init reader for " + table_.fullName(), ex);
      return false;
    }
  }

  /**
   * Perform clean up operation , will be called in Close
   * method of BE PaimonJniScanNode.
   * */
  @Override
  public void close() throws Exception {
    // release writer resources.
    PaimonUtil.closeQuitely(writer_);

    // release arrow allocator resources owned by current scanner.
    PaimonUtil.closeQuitely(bufferAllocator_);
    // used to check mem leak in more detail if arrow allocation
    // debug is turned on.
    if (bufferAllocator_.getAllocatedMemory() > 0) {
      LOG.error(
          String.format("Leaked memory for %s is %d bytes, dump:%s", table_.fullName(),
              bufferAllocator_.getAllocatedMemory(), bufferAllocator_.toVerboseString()));
    }
    LOG.info(String.format("Peak memory for %s is %d bytes, total rows: %d",
        table_.fullName(), bufferAllocator_.getPeakMemoryAllocation(), totalRows_));

    // release iterator resources
    PaimonUtil.closeQuitely(iterator_);
  }
}
