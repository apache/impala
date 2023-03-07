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

import java.util.EnumSet;

import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.common.hash.Hasher;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.base.Preconditions;

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
 * For debuggability, this keeps a human-readable trace of what has been incorporated
 * into the cache key. This will help track down why two cache keys are different.
 *
 * This accumulates information from various sources, then it is finalized and cannot
 * be modified further. The hash key and hash trace cannot be accessed until finalize()
 * is called.
 */
public class TupleCacheInfo {
  // Keep track of the reasons why a location in the plan is ineligible. This may be
  // multiple things, and it is useful to keep the various causes separate.
  public enum IneligibilityReason {
    NOT_IMPLEMENTED,
    CHILDREN_INELIGIBLE,
  }
  private EnumSet<IneligibilityReason> ineligibilityReasons_;

  // These fields accumulate partial results until finalize() is called.
  private Hasher hasher_ = Hashing.murmur3_128().newHasher();

  // The hash trace keeps a human-readable record of the items hashed into the cache key.
  private StringBuilder hashTraceBuilder_ = new StringBuilder();

  // When finalize() is called, these final values are filled in and the hasher and
  // hash trace builder are destroyed.
  private boolean finalized_ = false;
  private String finalizedHashTrace_ = null;
  private String finalizedHashString_ = null;

  public TupleCacheInfo() {
    ineligibilityReasons_ = EnumSet.noneOf(IneligibilityReason.class);
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
   * No further modifications can be made after calling finalize().
   */
  public void finalize() {
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
   * hash.
   */
  public void mergeChild(TupleCacheInfo child) {
    Preconditions.checkState(!finalized_,
        "TupleCacheInfo is finalized and can't be modified");
    if (!child.isEligible()) {
      ineligibilityReasons_.add(IneligibilityReason.CHILDREN_INELIGIBLE);
    } else {
      // The child is eligible, so incorporate its hash into our hasher.
      hasher_.putBytes(child.getHashString().getBytes());
      // Also, aggregate its hash trace into ours.
      // TODO: It might be more useful to have the hash trace just for this
      // node. We could display each node's hash trace in explain plan,
      // and each contribution would be clear.
      hashTraceBuilder_.append(child.getHashTrace());
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
}
