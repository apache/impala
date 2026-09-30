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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.iceberg.Accessor;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.StructLike;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterator;

/**
 * The metadata table scanner class to scan Iceberg metadata tables through the Iceberg
 * API. This object is instantiated and governed by {IcebergMetadataScanNode} at the
 * backend during scanning.
 *
 * Iceberg generally throws RuntimeExceptions, these have to be taken care of by the
 * caller of {@code IcebergMetadataScanner}.
 */
public class IcebergMetadataScanner implements AutoCloseable {
  private final static Logger LOG = LoggerFactory.getLogger(IcebergMetadataScanner.class);

  // Metadata table instance.
  final private Table metadataTable_;

  // FeTable object is extracted by the backend and passed when this object is created
  final private FeIcebergTable iceTbl_;

  // Name of the metadata table
  final private String metadataTableName_;

  // Persist the file scan task iterator so we can continue after a RowBatch is full
  private CloseableIterator<FileScanTask> fileScanTaskIterator_;

  // Persist the data rows iterator, so we can continue after a batch is filled
  private CloseableIterator<StructLike> dataRowsIterator_;

  public IcebergMetadataScanner(FeTable table, String metadataTableName)
      throws ImpalaException {
    this.metadataTableName_ = metadataTableName;
    this.iceTbl_ = checkIcebergTable(table, metadataTableName);
    this.metadataTable_ = MetadataTableUtils.createMetadataTableInstance(
      iceTbl_.getIcebergApiTable(), MetadataTableType.valueOf(metadataTableName_));
  }

  /**
   * Returns 'table' as an FeIcebergTable. The backend gets the base table again at
   * execution time (JniFrontend.getCatalogTable()), so it may differ from the table the
   * query was analyzed with: it may have failed to load, or it may have been dropped and
   * recreated as a non-Iceberg table. Throws an exception in these cases; a load failure
   * is kept as the cause.
   */
  @VisibleForTesting
  static FeIcebergTable checkIcebergTable(FeTable table, String metadataTableName)
      throws ImpalaException {
    Preconditions.checkNotNull(table);
    String tblName = table.getFullName();
    String errorPrefix = String.format("Cannot scan metadata table '%s.%s': ", tblName,
        metadataTableName.toLowerCase());
    if (table instanceof FeIncompleteTable) {
      ImpalaException cause = ((FeIncompleteTable) table).getCause();
      String reason = cause != null ?
          "failed to load table '" + tblName + "'" :
          "the metadata of table '" + tblName + "' is not loaded";
      throw new TableLoadingException(errorPrefix + reason, cause);
    }
    if (!(table instanceof FeIcebergTable)) {
      throw new ImpalaRuntimeException(
          errorPrefix + "table '" + tblName + "' is not an Iceberg table");
    }
    return (FeIcebergTable) table;
  }

  /**
   * Creates the Metadata{Table} which is a predifined Iceberg {Table} object. This method
   * also starts an Iceberg {TableScan} to scan the {Table}. After the scan is ready it
   * initializes the iterators, so the {GetNext} call can start fetching the rows through
   * the Iceberg Api.
   */
  public void ScanMetadataTable() {
    // If we are on a stack frame that was created through JNI we need to set the context
    // class loader as Iceberg might use reflection to dynamically load classes and
    // methods.
    JniUtil.setContextClassLoaderForThisThread(this.getClass().getClassLoader());
    // Create and scan the metadata table
    LOG.trace("Metadata table schema: " + metadataTable_.schema().toString());
    TableScan scan = metadataTable_.newScan();
    // Init the FileScanTask iterator and DataRowsIterator
    fileScanTaskIterator_ = scan.planFiles().iterator();
    FindFileScanTaskWithRows();
  }

  /**
   * Iterates over the {{fileScanTaskIterator_}} to find a {FileScanTask} that has rows.
   * Closes the rows iterator of the previous task, as it may hold an open manifest
   * reader.
   */
  private boolean FindFileScanTaskWithRows() {
    closeDataRowsIterator();
    if (fileScanTaskIterator_ == null) return false;
    while (fileScanTaskIterator_.hasNext()) {
      DataTask dataTask = (DataTask)fileScanTaskIterator_.next();
      dataRowsIterator_ = dataTask.rows().iterator();
      if (dataRowsIterator_.hasNext()) return true;
      closeDataRowsIterator();
    }
    return false;
  }

  /**
   * Closes the iterators, releasing the Iceberg readers that are still open. Safe to call
   * before ScanMetadataTable() and more than once. GetNext() returns null afterwards.
   */
  @Override
  public void close() {
    closeDataRowsIterator();
    if (fileScanTaskIterator_ != null) {
      closeQuietly(fileScanTaskIterator_);
      fileScanTaskIterator_ = null;
    }
  }

  private void closeDataRowsIterator() {
    if (dataRowsIterator_ == null) return;
    // Reset the field first, so a failed close is not retried.
    CloseableIterator<StructLike> iterator = dataRowsIterator_;
    dataRowsIterator_ = null;
    closeQuietly(iterator);
  }

  private void closeQuietly(CloseableIterator<?> iterator) {
    try {
      iterator.close();
    } catch (IOException | RuntimeException e) {
      LOG.warn(String.format("Failed to close iterator of metadata table %s.%s",
          iceTbl_.getFullName(), metadataTableName_.toLowerCase()), e);
    }
  }

  /**
   * Returns the next available row of the scan result. The row is a {StructLike} object
   * and its fields can be accessed with {Accessor}s.
   */
  public StructLike GetNext() {
    // Return the next row in the DataRows iterator
    if (dataRowsIterator_ != null && dataRowsIterator_.hasNext()) {
      return dataRowsIterator_.next();
    }
    // Otherwise this DataTask is empty, find a FileScanTask that has a non-empty DataTask
    if (FindFileScanTaskWithRows()) {
      return dataRowsIterator_.next();
    }
    return null;
  }

  /**
   * Uses the Accessor to access the value in the StructLike object. This only works for
   * non collection types.
   */
  public Object GetValueByFieldId(StructLike structLike, int fieldId) {
    Accessor accessor = metadataTable_.schema().accessorForField(fieldId);
    return accessor.get(structLike);
  }

  /**
   * Accesses the value inside the StructLike by its position.
   */
  public <T> T GetValueByPosition(StructLike structLike, int pos, Class<T> javaClass)
  {
    return structLike.get(pos, javaClass);
  }

  /**
   * Returns the UTF-8 encoding of a CharSequence. The JNI string functions only provide
   * modified UTF-8, which differs from standard UTF-8 for NUL and supplementary
   * characters.
   */
  public byte[] CharSequenceToUtf8Bytes(CharSequence charSequence) {
    return charSequence.toString().getBytes(StandardCharsets.UTF_8);
  }

  /**
   * Extracts the contents of a ByteBuffer into a byte array.
   */
  public byte[] ByteBufferToByteArray(ByteBuffer buffer) {
    int length = buffer.remaining();
    byte[] res = new byte[length];
    buffer.get(res);
    return res;
  }

  /**
   * Wrapper around an array or a map that is the result of a metadata table scan.
   * It is used to avoid iterating over a list through JNI.
   * For arrays the returned objects are the elements of the array, for maps they are
   * {Map.Entry} objects containing the key and value.
   */
  public static class CollectionScanner<T> {
    private Iterator<T> iterator_;

    private CollectionScanner(Iterator<T> iterator) {
      Preconditions.checkNotNull(iterator);
      iterator_ = iterator;
    }

    public static <G> CollectionScanner<G> fromArray(List<G> array) {
      CollectionScanner<G> res = new CollectionScanner<>(array.iterator());
      LOG.trace("Created metadata table array scanner, array size: " + array.size());
      return res;
    }

    public static <K, V> CollectionScanner<Map.Entry<K, V>> fromMap(Map<K, V> map) {
      CollectionScanner<Map.Entry<K, V>> res =
          new CollectionScanner<>(map.entrySet().iterator());
      LOG.trace("Created metadata table map scanner, map size: " + map.size());
      return res;
    }

    public T GetNextCollectionItem() {
      if (iterator_.hasNext()) return iterator_.next();
      return null;
    }
  }
}
