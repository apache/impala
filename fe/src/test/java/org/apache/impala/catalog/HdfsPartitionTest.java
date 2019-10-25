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

package org.apache.impala.catalog;

import static org.apache.impala.catalog.HdfsPartition.comparePartitionKeyValues;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.impala.analysis.BoolLiteral;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.analysis.NullLiteral;
import org.apache.impala.analysis.NumericLiteral;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;
import org.junit.Test;

import com.google.common.collect.Lists;

public class HdfsPartitionTest {

  static {
    FeSupport.loadLibrary();
  }

  private List<LiteralExpr> valuesNull_= new ArrayList<>();
  private List<LiteralExpr> valuesDecimal_ = new ArrayList<>();
  private List<LiteralExpr> valuesDecimal1_ = new ArrayList<>();
  private List<LiteralExpr> valuesDecimal2_ = new ArrayList<>();
  private List<LiteralExpr> valuesMixed_= new ArrayList<>();
  private List<LiteralExpr> valuesMixed1_ = new ArrayList<>();
  private List<LiteralExpr> valuesMixed2_ = new ArrayList<>();

  public HdfsPartitionTest() {
    valuesNull_.add(NullLiteral.create(Type.BIGINT));

    valuesDecimal_.add(NumericLiteral.create(1));
    valuesDecimal1_.add(NumericLiteral.create(3));
    valuesDecimal2_.add(NumericLiteral.create(5));

    valuesMixed_.add(NumericLiteral.create(3));
    valuesMixed_.add(NullLiteral.create(Type.BIGINT));

    valuesMixed1_.add(NumericLiteral.create(1));
    valuesMixed1_.add(NullLiteral.create(Type.STRING));
    valuesMixed1_.add(new BoolLiteral(true));

    valuesMixed2_.add(NumericLiteral.create(1));
    valuesMixed2_.add(new StringLiteral("Large"));
    valuesMixed2_.add(new BoolLiteral(false));
  }

  @Test
  public void testCompare() {
    List<List<LiteralExpr>> allLists = new ArrayList<>();
    allLists.add(valuesNull_);
    allLists.add(valuesDecimal_);
    allLists.add(valuesDecimal1_);
    allLists.add(valuesDecimal2_);
    allLists.add(valuesMixed_);
    allLists.add(valuesMixed1_);
    allLists.add(valuesMixed2_);

    for (List<LiteralExpr> l1: allLists) {
      verifyReflexive(l1);
      for (List<LiteralExpr> l2: allLists) {
        verifySymmetric(l1, l2);
        for (List<LiteralExpr> l3: allLists) {
          verifyTransitive(l1, l2, l3);
        }
      }
    }

    List<LiteralExpr> valuesTest = new ArrayList<>();
    valuesTest.add(NumericLiteral.create(3));
    verifyAntiSymmetric(valuesDecimal1_, valuesTest, valuesNull_);
    valuesTest.add(NullLiteral.create(Type.BIGINT));
    verifyAntiSymmetric(valuesMixed_, valuesTest, valuesDecimal_);
  }

  private void verifySymmetric(List<LiteralExpr> o1, List<LiteralExpr> o2) {
    // sgn(compare(x, y)) == -sgn(compare(y, x)),
    // compare(x, y) == 0, then compare(y, x) == 0
    assertTrue(Integer.signum(comparePartitionKeyValues(o1, o2)) ==
        -Integer.signum(comparePartitionKeyValues(o2, o1)));
  }

  private void verifyTransitive(List<LiteralExpr> o1, List<LiteralExpr> o2,
                                List<LiteralExpr> o3) {
    // ((compare(x, y)>0) && (compare(y, z)>0)) implies compare(x, z)>0
    if ((comparePartitionKeyValues(o1, o2) > 0) &&
        (comparePartitionKeyValues(o2, o3) > 0)) {
      assertTrue(comparePartitionKeyValues(o1, o3) > 0);
    }
  }

  private void verifyReflexive(List<LiteralExpr> o1) {
    // (compare(x, x)==0) is always true
    assertTrue(comparePartitionKeyValues(o1, o1) == 0);
  }

  private void verifyAntiSymmetric(List<LiteralExpr> o1, List<LiteralExpr> o2,
                                   List<LiteralExpr> o3) {
    // compare(x, y)==0 implies that sgn(compare(x, z))==sgn(compare(y, z)) for all z.
    if (comparePartitionKeyValues(o1, o2) == 0) {
      assertTrue(Integer.signum(comparePartitionKeyValues(o1, o3)) ==
          Integer.signum(comparePartitionKeyValues(o2, o3)));
    }
  }

  /**
   * Get the list of all locations of blocks from the given file descriptor.
   */
  private static List<TNetworkAddress> getAllReplicaAddresses(FileDescriptor fd,
      ListMap<TNetworkAddress> hostIndex) {
    List<TNetworkAddress> ret = new ArrayList<>();
    for (int i = 0; i < fd.getNumFileBlocks(); i++) {
      for (int j = 0; j < fd.getFbFileBlock(i).replicaHostIdxsLength(); j++) {
        int idx = fd.getFbFileBlock(i).replicaHostIdxs(j);
        ret.add(hostIndex.getEntry(idx));
      }
    }
    return ret;
  }

  @Test
  public void testCloneWithNewHostIndex() throws Exception {
    // Fetch some metadata from a directory in HDFS.
    Path p = new Path("hdfs://localhost:20500/test-warehouse/schemas");
    ListMap<TNetworkAddress> origIndex = new ListMap<>();
    FileMetadataLoader fml = new FileMetadataLoader(p, /* recursive= */false,
        Collections.emptyList(), origIndex, /*validTxnList=*/null, /*writeIds=*/null);
    fml.load();
    List<FileDescriptor> fileDescriptors = fml.getLoadedFds();
    assertTrue(!fileDescriptors.isEmpty());

    FileDescriptor fd = fileDescriptors.get(0);
    // Get the list of locations, using the original host index.
    List<TNetworkAddress> origAddresses = getAllReplicaAddresses(fd, origIndex);

    // Make a new host index with the hosts in the opposite order.
    ListMap<TNetworkAddress> newIndex = new ListMap<>();
    newIndex.populate(Lists.reverse(origIndex.getList()));

    // Clone the FD over to the reversed index. The actual addresses should be the same.
    FileDescriptor cloned = fd.cloneWithNewHostIndex(origIndex.getList(), newIndex);
    List<TNetworkAddress> newAddresses = getAllReplicaAddresses(cloned, newIndex);

    assertEquals(origAddresses, newAddresses);
  }
}
