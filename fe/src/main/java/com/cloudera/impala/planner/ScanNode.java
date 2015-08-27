// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.planner;

import java.util.List;

import com.cloudera.impala.analysis.SlotDescriptor;
import com.cloudera.impala.analysis.TupleDescriptor;
import com.cloudera.impala.catalog.HdfsFileFormat;
import com.cloudera.impala.common.NotImplementedException;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TNetworkAddress;
import com.cloudera.impala.thrift.TScanRangeLocations;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Representation of the common elements of all scan nodes.
 */
abstract public class ScanNode extends PlanNode {
  protected final TupleDescriptor desc_;

  // Total number of rows this node is expected to process
  protected long inputCardinality_ = -1;

  // Counter indicating if partitions have missing statistics
  protected int numPartitionsMissingStats_ = 0;

  // List of scan-range locations. Populated in init().
  protected List<TScanRangeLocations> scanRanges_;

  public ScanNode(PlanNodeId id, TupleDescriptor desc, String displayName) {
    super(id, desc.getId().asList(), displayName);
    desc_ = desc;
  }

  public TupleDescriptor getTupleDesc() { return desc_; }

  /**
   * Checks if this scan is supported based on the types of scanned columns and the
   * underlying file formats, in particular, whether complex types are supported.
   *
   * The default implementation throws if this scan would need to materialize a nested
   * field or collection. The scan is ok if the table schema contains complex types, as
   * long as the query does not reference them.
   *
   * Subclasses should override this function as appropriate.
   */
  protected void checkForSupportedFileFormats() throws NotImplementedException {
    Preconditions.checkNotNull(desc_);
    Preconditions.checkNotNull(desc_.getTable());
    for (SlotDescriptor slotDesc: desc_.getSlots()) {
      if (slotDesc.getType().isComplexType() || slotDesc.getColumn() == null) {
        Preconditions.checkNotNull(slotDesc.getPath());
        throw new NotImplementedException(String.format(
            "Scan of table '%s' is not supported because '%s' references a nested " +
            "field/collection.\nComplex types are supported for these file formats: %s.",
            slotDesc.getPath().toString(), desc_.getAlias(),
            Joiner.on(", ").join(HdfsFileFormat.complexTypesFormats())));
      }
    }
  }

  /**
   * Returns all scan ranges plus their locations.
   */
  public List<TScanRangeLocations> getScanRangeLocations() {
    Preconditions.checkNotNull(scanRanges_, "Need to call init() first.");
    return scanRanges_;
  }

  @Override
  protected String debugString() {
    return Objects.toStringHelper(this)
        .add("tid", desc_.getId().asInt())
        .add("tblName", desc_.getTable().getFullName())
        .add("keyRanges", "")
        .addValue(super.debugString())
        .toString();
  }

  /**
   * Returns the explain string for table and columns stats to be included into the
   * a ScanNode's explain string. The given prefix is prepended to each of the lines.
   * The prefix is used for proper formatting when the string returned by this method
   * is embedded in a query's explain plan.
   */
  protected String getStatsExplainString(String prefix, TExplainLevel detailLevel) {
    StringBuilder output = new StringBuilder();
    // Table stats.
    if (desc_.getTable().getNumRows() == -1) {
      output.append(prefix + "table stats: unavailable");
    } else {
      output.append(prefix + "table stats: " + desc_.getTable().getNumRows() +
          " rows total");
      if (numPartitionsMissingStats_ > 0) {
        output.append(" (" + numPartitionsMissingStats_ + " partition(s) missing stats)");
      }
    }
    output.append("\n");

    // Column stats.
    List<String> columnsMissingStats = Lists.newArrayList();
    for (SlotDescriptor slot: desc_.getSlots()) {
      if (!slot.getStats().hasStats() && slot.getColumn() != null) {
        columnsMissingStats.add(slot.getColumn().getName());
      }
    }
    if (columnsMissingStats.isEmpty()) {
      output.append(prefix + "column stats: all");
    } else if (columnsMissingStats.size() == desc_.getSlots().size()) {
      output.append(prefix + "column stats: unavailable");
    } else {
      output.append(String.format("%scolumns missing stats: %s", prefix,
          Joiner.on(", ").join(columnsMissingStats)));
    }
    return output.toString();
  }

  /**
   * Returns true if the table underlying this scan is missing table stats
   * or column stats relevant to this scan node.
   */
  public boolean isTableMissingStats() {
    return isTableMissingColumnStats() || isTableMissingTableStats();
  }

  public boolean isTableMissingTableStats() {
    if (desc_.getTable().getNumRows() == -1) return true;
    return numPartitionsMissingStats_ > 0;
  }

  public boolean isTableMissingColumnStats() {
    for (SlotDescriptor slot: desc_.getSlots()) {
      if (!slot.getStats().hasStats()) return true;
    }
    return false;
  }

  /**
   * Returns true, if the scanned table is suspected to have corrupt table stats,
   * in particular, if the scan is non-empty and 'numRows' is 0 or negative (but not -1).
   */
  public boolean hasCorruptTableStats() { return false; }

  /**
   * Helper function to parse a "host:port" address string into TNetworkAddress
   * This is called with ipaddress:port when doing scan range assignment.
   */
  protected static TNetworkAddress addressToTNetworkAddress(String address) {
    TNetworkAddress result = new TNetworkAddress();
    String[] hostPort = address.split(":");
    result.hostname = hostPort[0];
    result.port = Integer.parseInt(hostPort[1]);
    return result;
  }

  @Override
  public long getInputCardinality() {
    if (getConjuncts().isEmpty() && hasLimit()) return getLimit();
    return inputCardinality_;
  }
}
