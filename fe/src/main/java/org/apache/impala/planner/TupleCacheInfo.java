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

package org.apache.impala.planner;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;

import org.apache.impala.analysis.DescriptorTable;
import org.apache.impala.analysis.SlotDescriptor;
import org.apache.impala.analysis.SlotId;
import org.apache.impala.analysis.TupleDescriptor;
import org.apache.impala.analysis.TupleId;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.FeView;
import org.apache.impala.common.IdGenerator;
import org.apache.impala.common.ThriftSerializationCtx;
import org.apache.impala.thrift.TFileSplitGeneratorSpec;
import org.apache.impala.thrift.TScanRange;
import org.apache.impala.thrift.TScanRangeLocationList;
import org.apache.impala.thrift.TScanRangeSpec;
import org.apache.impala.thrift.TSlotDescriptor;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTupleDescriptor;
import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.base.Preconditions;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

/**
 * TupleCacheInfo stores the eligibility and cache key information for a PlanNode.
 * It is calculated by a bottom-up traversal of the plan node tree.
 *
 * In order to be eligible, a node's children must all be eligible and the node must
 * implement tuple caching support. Eligibility will expand to require deterministic
 * execution.
 *
 * The cache key for a node is a combination of the cache keys of children and
 * the hash of a node's Thrift structure (and any other linked Thrift structures).
 * To support this, it provides a function to incorporate any Thrift structure's
 * contents into the hash.
 *
 * One critical piece of making the Thrift structures more general is id translation.
 * Since TupleIds and SlotIds are global to the query, the value of any id will be
 * influenced by the rest of the query unless we translate it to a local id.
 * Plan nodes register their tuples via registerTuple(). This allows the tuple / slot
 * information to be incorporated into the hash (by accessing the DescriptorTable),
 * but it also allocates a local id and adds an entry to the translation map. Exprs
 * and other structures can use translateSlotId() and translateTupleId() to adjust
 * global ids to local ids. When TupleCacheInfos are merged, they merge the translations
 * so there are no conflicts. Translation always goes in the global to local direction.
 *
 * There are a few reason that we don't try to maintain local ids earlier in planning:
 * 1. Only tuple caching needs local ids. The extra modifications introduce risk and
 *    don't dramatically improve the outcome.
 * 2. The plan shape can change across the various phases of planning. In particular,
 *    runtime filters add edges to the PlanNode graph. It is hard to produce a stable
 *    local id until the plan is stable.
 * 3. There is ongoing work to add a Calcite planner, and we will want to support tuple
 *    caching for that planner. Any logic in analysis/planning that produces local ids
 *    will need to also work for Calcite analyzer/planner.
 *
 * For debuggability, this keeps a human-readable trace of what has been incorporated
 * into the cache key. This will help track down why two cache keys are different.
 * Anything hashed will have a representation incorporated into the trace.
 *
 * This accumulates information from various sources, then it is finalized and cannot
 * be modified further. The hash key and hash trace cannot be accessed until
 * finalizeHash() is called.
 */
public class TupleCacheInfo {
  // Keep track of the reasons why a location in the plan is ineligible. This may be
  // multiple things, and it is useful to keep the various causes separate.
  public enum IneligibilityReason {
    NOT_IMPLEMENTED,
    CHILDREN_INELIGIBLE,
    // Limits are ineligible because they are implemented in a non-deterministic
    // way. In future, this can support locations that are deterministic (e.g.
    // limits on a sorted input).
    LIMIT,
    NONDETERMINISTIC_FN,
    MERGING_EXCHANGE,
    PARTITIONED_EXCHANGE,
  }
  private EnumSet<IneligibilityReason> ineligibilityReasons_;

  // read-only reference to the query's descriptor table
  // used for incorporating tuple/slot/table information
  private DescriptorTable descriptorTable_;

  // The tuple translation uses a tree map because we need a deterministic order
  // for visting elements when merging two translation maps.
  private final Map<TupleId, TupleId> tupleTranslationMap_ = new TreeMap<>();
  // The slot translation does not need to be a deterministic order, so it
  // can use a HashMap.
  private final Map<SlotId, SlotId> slotTranslationMap_ = new HashMap<>();
  private final IdGenerator<TupleId> translatedTupleIdGenerator_ =
      TupleId.createGenerator();
  private final IdGenerator<SlotId> translatedSlotIdGenerator_ =
      SlotId.createGenerator();

  // This tracks all the HdfsScanNodes that are inputs. This is used for several
  // purposes:
  // 1. Input scan nodes need to use deterministic scan range scheduling.
  // 2. At runtime, the tuple cache needs to hash the input scan ranges, so this
  //    provides information about which scan nodes feed in.
  // 3. In future, when tuple caching moves past exchanges, the exchange will need
  //    to hash the scan ranges of input scan nodes to generate the key.
  private final List<HdfsScanNode> inputScanNodes_ = new ArrayList<HdfsScanNode>();

  // These fields accumulate partial results until finalizeHash() is called.
  private Hasher hasher_ = Hashing.murmur3_128().newHasher();

  // The hash trace keeps a human-readable record of the items hashed into the cache key.
  private StringBuilder hashTraceBuilder_ = new StringBuilder();

  // When finalizeHash() is called, these final values are filled in and the hasher and
  // hash trace builder are destroyed.
  private boolean finalized_ = false;
  private String finalizedHashTrace_ = null;
  private String finalizedHashString_ = null;

  public TupleCacheInfo(DescriptorTable descTbl) {
    ineligibilityReasons_ = EnumSet.noneOf(IneligibilityReason.class);
    descriptorTable_ = descTbl;
  }

  public void setIneligible(IneligibilityReason reason) {
    Preconditions.checkState(!finalized_,
        "TupleCacheInfo is finalized and can't be modified");
    ineligibilityReasons_.add(reason);
  }

  public boolean isEligible() {
    return ineligibilityReasons_.isEmpty();
  }

  public String getHashString() {
    Preconditions.checkState(isEligible(),
        "TupleCacheInfo only has a hash if it is cache eligible");
    Preconditions.checkState(finalized_, "TupleCacheInfo not finalized");
    return finalizedHashString_;
  }

  public String getHashTrace() {
    Preconditions.checkState(isEligible(),
        "TupleCacheInfo only has a hash trace if it is cache eligible");
    Preconditions.checkState(finalized_, "TupleCacheInfo not finalized");
    return finalizedHashTrace_;
  }

  /**
   * Finish accumulating information and calculate the final hash value and
   * hash trace. This must be called before accessing the hash or hash trace.
   * No further modifications can be made after calling finalizeHash().
   */
  public void finalizeHash() {
    finalizedHashString_ = hasher_.hash().toString();
    hasher_ = null;
    finalizedHashTrace_ = hashTraceBuilder_.toString();
    hashTraceBuilder_ = null;
    finalized_ = true;
  }

  /**
   * Pull in a child's TupleCacheInfo into this TupleCacheInfo. If the child is
   * ineligible, then this is marked ineligible and there is no need to calculate
   * a hash. If the child is eligible, it incorporates the child's hash into this
   * hash. Returns true if the child was merged, false if it was ineligible.
   */
  public boolean mergeChild(TupleCacheInfo child) {
    if (!mergeChildImpl(child)) {
      return false;
    }

    // Merge the child's inputScanNodes_
    inputScanNodes_.addAll(child.inputScanNodes_);
    return true;
  }

  /**
   * Pull in a child's TupleCacheInfo into this TupleCacheInfo while also incorporating
   * all of its scan ranges into the key. This returns true if the child is eligible
   * and false otherwise.
   */
  public boolean mergeChildWithScans(TupleCacheInfo child) {
    if (!child.isEligible()) {
      return mergeChild(child);
    }
    // Use a temporary TupleCacheInfo to incorporate the scan ranges for this child.
    TupleCacheInfo tmpInfo = new TupleCacheInfo(descriptorTable_);
    boolean success = tmpInfo.mergeChild(child);
    Preconditions.checkState(success);
    tmpInfo.incorporateScans();
    tmpInfo.finalizeHash();
    return mergeChild(tmpInfo);
  }

  /**
   * Incorporate all the scan range information from input scan nodes into the
   * cache key. This clears the lists of input scan nodes, as the information is
   * now built into the cache key.
   */
  public void incorporateScans() {
    // Add all scan range specs to the hash. Copy only the relevant fields, primarily:
    // filename, mtime, size, and offset. Others like partition_id may change after
    // reloading metadata.
    for (HdfsScanNode scanNode: inputScanNodes_) {
      TScanRangeSpec orig = scanNode.getScanRangeSpecs();
      TScanRangeSpec spec = new TScanRangeSpec();
      if (orig.isSetConcrete_ranges()) {
        for (TScanRangeLocationList origLocList: orig.concrete_ranges) {
          // We only need the TScanRange, which provides the file segment info.
          TScanRangeLocationList locList = new TScanRangeLocationList();
          TScanRange scanRange = origLocList.scan_range.deepCopy();
          if (scanRange.isSetHdfs_file_split()) {
            // Zero out partition_id, it's not stable.
            scanRange.hdfs_file_split.partition_id = 0;
          }
          locList.setScan_range(scanRange);
          spec.addToConcrete_ranges(locList);
        }
        // Reloaded partitions may have a different order. Sort for stability.
        spec.concrete_ranges.sort(null);
      }
      if (orig.isSetSplit_specs()) {
        for (TFileSplitGeneratorSpec origSplitSpec: orig.split_specs) {
          TFileSplitGeneratorSpec splitSpec = origSplitSpec.deepCopy();
          // Zero out partition_id, it's not stable.
          splitSpec.partition_id = 0;
          spec.addToSplit_specs(splitSpec);
        }
        // Reloaded partitions may have a different order. Sort for stability.
        spec.split_specs.sort(null);
      }
      hashThrift(spec);
    }
    // The scan ranges have been incorporated into the key and are no longer needed
    // at runtime.
    inputScanNodes_.clear();
  }

  /**
   * Pull in a child's TupleCacheInfo that can be exhaustively determined during planning.
   * Public interfaces may add additional info that is more dynamic, such as scan ranges.
   */
  private boolean mergeChildImpl(TupleCacheInfo child) {
    Preconditions.checkState(!finalized_,
        "TupleCacheInfo is finalized and can't be modified");
    if (!child.isEligible()) {
      ineligibilityReasons_.add(IneligibilityReason.CHILDREN_INELIGIBLE);
      return false;
    } else {
      // The child is eligible, so incorporate its hash into our hasher.
      hasher_.putBytes(child.getHashString().getBytes());
      // Also, aggregate its hash trace into ours.
      // TODO: It might be more useful to have the hash trace just for this
      // node. We could display each node's hash trace in explain plan,
      // and each contribution would be clear.
      hashTraceBuilder_.append(child.getHashTrace());

      // Incorporate the child's tuple references. This is creating a new translation
      // of TupleIds, because it will be incorporating multiple children.
      for (TupleId id : child.tupleTranslationMap_.keySet()) {
        // Register the tuples, but don't incorporate their content into the hash.
        // The content was already hashed by the children, so we only need the
        // id translation maps.
        registerTupleHelper(id, false);
      }
      return true;
    }
  }

  /**
   * All Thrift objects inherit from TBase, so this function can incorporate any Thrift
   * object into the hash.
   */
  public void hashThrift(TBase<?, ?> thriftObj) {
    Preconditions.checkState(!finalized_,
        "TupleCacheInfo is finalized and can't be modified");
    try {
      TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
      hasher_.putBytes(serializer.serialize(thriftObj));
    } catch (org.apache.thrift.TException e) {
      // This should not happen. Having a precondition rather than throwing an exception
      // avoids needing to include the exception in the function specification.
      Preconditions.checkState(false, "Unexpected Thrift exception: " + e.toString());
    }
    // All Thrift objects have a toString() function with a human-readable
    // representation of all fields that have been set. Looking at the implementation,
    // Thrift's toString() function doesn't return null.
    String thriftString = thriftObj.toString();
    Preconditions.checkState(thriftString != null);
    hashTraceBuilder_.append(thriftString);
  }

  /**
   * registerTuple() does two things:
   * 1. It incorporates a tuple's layout (and slot information) into the cache key.
   * 2. It establishes a mapping from the global TupleIds/SlotIds to local
   *    TupleIds/SlotIds. See explanation above about id translation.
   * It should be called for any tuple that is referenced from a PlanNode that supports
   * tuple caching. It is usually called via the ThriftSerializationCtx. If the tuple has
   * already been registered, this immediately returns.
   */
  public void registerTuple(TupleId id) {
    registerTupleHelper(id, true);
  }

  private void registerTupleHelper(TupleId id, boolean incorporateIntoHash) {
    Preconditions.checkState(!finalized_,
        "TupleCacheInfo is finalized and can't be modified");
    ThriftSerializationCtx serialCtx = new ThriftSerializationCtx(this);
    // If we haven't seen this tuple before:
    // - assign an index for the tuple
    // - assign indexes for the tuple's slots
    // - incorporate the tuple and slots into the hash / hash trace
    if (!tupleTranslationMap_.containsKey(id)) {
      // Assign a translated tuple id and add it to the map
      tupleTranslationMap_.put(id, translatedTupleIdGenerator_.getNextId());

      TupleDescriptor tupleDesc = descriptorTable_.getTupleDesc(id);
      // This matches the behavior of DescriptorTable::toThrift() and skips
      // non-materialized tuple descriptors. See comment in DescriptorTable::toThrift().
      if (!tupleDesc.isMaterialized()) return;
      if (incorporateIntoHash) {
        // Incorporate the tupleDescriptor into the hash
        boolean needs_table_id =
            (tupleDesc.getTable() != null && !(tupleDesc.getTable() instanceof FeView));
        TTupleDescriptor thriftTupleDesc =
            tupleDesc.toThrift(needs_table_id ? new Integer(1) : null, serialCtx);
        hashThrift(thriftTupleDesc);
      }

      // Go through the tuple's slots and add them. This matches the behavior of
      // DescriptorTable::toThrift() and only serializes the materialized slots.
      for (SlotDescriptor slotDesc : tupleDesc.getMaterializedSlots()) {
        // Assign a translated slot id and it to the map
        slotTranslationMap_.put(slotDesc.getId(), translatedSlotIdGenerator_.getNextId());

        // Slots can have nested tuples, so this can recurse. The depth is limited.
        // The parent can reference tuple ids from children, so this needs to recurse
        // to the children first.
        TupleDescriptor nestedTupleDesc = slotDesc.getItemTupleDesc();
        if (nestedTupleDesc != null) {
          registerTupleHelper(nestedTupleDesc.getId(), incorporateIntoHash);
        }
        if (incorporateIntoHash) {
           // Incorporate the SlotDescriptor into the hash
          TSlotDescriptor thriftSlotDesc = slotDesc.toThrift(serialCtx);
          hashThrift(thriftSlotDesc);
        }
      }
    }
  }

  /**
   * registerTable() incorporates a table's information into the cache key. This is
   * designed to be called by scan nodes via the ThriftSerializationCtx. In future,
   * this will store information about the table's scan ranges.
   */
  private void registerTable(FeTable tbl) {
    Preconditions.checkState(!(tbl instanceof FeView),
        "registerTable() only applies to base tables");
    Preconditions.checkState(tbl != null, "Invalid null argument to registerTable()");

    // Right now, we only hash the database / table name.
    TTableName tblName = tbl.getTableName().toThrift();
    hashThrift(tblName);
  }

  /**
   * registerInputScanNode() is used to keep track of which HdfsScanNodes feed into a
   * particular location for tuple caching. Tuple caching only supports HDFS tables at
   * the moment, so this is limited to HdfsScanNode. Tuple caching uses this for
   * multiple things:
   * 1. HdfsScanNodes that feed into a TupleCacheNode need to be marked to use
   *    deterministic scheduling.
   * 2. Each fragment instance needs to construct the fragment instance specific key
   *    based on the scan ranges it will process. To construct that, it needs to know
   *    which HdfsScanNodes feed into it.
   * 3. There will be future uses when tuple caching extends past exchanges.
   *
   * Since this has all the information needed, it also calls registerTable() under
   * the covers.
   */
  public void registerInputScanNode(HdfsScanNode hdfsScanNode) {
    registerTable(hdfsScanNode.getTupleDesc().getTable());
    inputScanNodes_.add(hdfsScanNode);
  }

  public List<HdfsScanNode> getInputScanNodes() { return inputScanNodes_; }

  /**
   * getLocalTupleId() converts a global TupleId to a local TupleId (i.e an id that is
   * not influenced by the structure of the rest of the query). Most users should access
   * this via the ThriftSerializationCtx's translateTupleId().
   */
  public TupleId getLocalTupleId(TupleId globalId) {
    // The tuple must have been registered before this reference happens
    Preconditions.checkState(tupleTranslationMap_.containsKey(globalId));
    return tupleTranslationMap_.get(globalId);
  }

  /**
   * getLocalSlotId() converts a global TupleId to a local TupleId (i.e an id that is
   * not influenced by the structure of the rest of the query). Most users should access
   * this via the ThriftSerializationCtx's translateSlotId().
   */
  public SlotId getLocalSlotId(SlotId globalId) {
    // The slot must have been registered before this reference happens
    Preconditions.checkState(slotTranslationMap_.containsKey(globalId));
    return slotTranslationMap_.get(globalId);
  }

  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TupleCacheInfo:");
    if (isEligible()) {
      builder.append("cache key: ");
      builder.append(getHashString());
      builder.append("\n");
      builder.append("cache key hash trace: ");
      builder.append(getHashTrace());
      builder.append("\n");
    } else {
      builder.append("ineligibility reasons: ");
      builder.append(getIneligibilityReasonsString());
      builder.append("\n");
    }
    return builder.toString();
  }

  /**
   * Construct a comma separated list of the ineligibility reasons.
   */
  public String getIneligibilityReasonsString() {
    StringJoiner joiner = new StringJoiner(",");
    for (IneligibilityReason reason : ineligibilityReasons_) {
      joiner.add(reason.toString());
    }
    return joiner.toString();
  }
}
