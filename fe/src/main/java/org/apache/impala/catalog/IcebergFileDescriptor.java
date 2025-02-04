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

import com.google.common.base.Preconditions;

import org.apache.impala.fb.FbFileDesc;
import org.apache.impala.fb.FbFileMetadata;
import org.apache.impala.thrift.THdfsFileDesc;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.util.ListMap;

import java.nio.ByteBuffer;
import java.util.List;

public class IcebergFileDescriptor extends FileDescriptor {
  // Internal representation of Iceberg metadata.
  private final FbFileMetadata fbFileMetadata_;

  public IcebergFileDescriptor(FbFileDesc fileDescData, FbFileMetadata fileMetadata) {
    super(fileDescData);

    Preconditions.checkNotNull(fileMetadata);
    Preconditions.checkNotNull(fileMetadata.icebergMetadata());
    fbFileMetadata_ = fileMetadata;
  }

  public static IcebergFileDescriptor fromThrift(THdfsFileDesc desc) {
    Preconditions.checkState(desc.isSetFile_metadata());

    ByteBuffer bb = ByteBuffer.wrap(desc.getFile_desc_data());
    ByteBuffer bbMd = ByteBuffer.wrap(desc.getFile_metadata());

    return new IcebergFileDescriptor(
        FbFileDesc.getRootAsFbFileDesc(bb),
        FbFileMetadata.getRootAsFbFileMetadata(bbMd));
  }

  @Override
  public THdfsFileDesc toThrift() {
    THdfsFileDesc fd = super.toThrift();
    fd.setFile_metadata(fbFileMetadata_.getByteBuffer());
    return fd;
  }

  public static IcebergFileDescriptor cloneWithFileMetadata(
      FileDescriptor fd, FbFileMetadata fileMetadata) {
    return new IcebergFileDescriptor(fd.getFbFileDescriptor(), fileMetadata);
  }

  @Override
  public IcebergFileDescriptor cloneWithNewHostIndex(
      List<TNetworkAddress> origIndex, ListMap<TNetworkAddress> dstIndex) {
    return new IcebergFileDescriptor(
        fbFileDescWithNewHostIndex(origIndex, dstIndex), fbFileMetadata_);
  }

  public FbFileMetadata getFbFileMetadata() {
    return fbFileMetadata_;
  }

  // The default equals() and hashCode() functions aren't sufficient for
  // IcebergFileDescriptors. The reason is that these are stored in
  // IcebergContentFileStore in an encoded format and every 'get' operation on the store
  // returns a different object for the same file descriptor. Instead, this function
  // checks for the equality of the underlying byte array representations.
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof IcebergFileDescriptor)) return false;

    IcebergFileDescriptor otherFD = (IcebergFileDescriptor) obj;
    Preconditions.checkNotNull(this.getFbFileMetadata());
    Preconditions.checkNotNull(otherFD.getFbFileMetadata());

    return this.getFbFileDescriptor().getByteBuffer().array() ==
        otherFD.getFbFileDescriptor().getByteBuffer().array() &&
        this.getFbFileMetadata().getByteBuffer().array() ==
            otherFD.getFbFileMetadata().getByteBuffer().array();
  }

  @Override
  public int hashCode() {
    return getAbsolutePath().hashCode();
  }
}