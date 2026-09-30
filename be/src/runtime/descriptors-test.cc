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

#include <string>
#include <vector>

#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/Descriptors_types.h"
#include "rpc/thrift-util.h"
#include "runtime/descriptors.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

namespace {

const TableId TABLE_ID = 0;

TIcebergPartitionField MakeField(int32_t source_id, int32_t field_id,
    const string& name, TIcebergPartitionTransformType::type transform_type) {
  TIcebergPartitionTransform transform;
  transform.__set_transform_type(transform_type);
  TScalarType type;
  type.__set_type(TPrimitiveType::STRING);
  TIcebergPartitionField field;
  field.__set_source_id(source_id);
  field.__set_field_id(field_id);
  field.__set_orig_field_name(name);
  field.__set_field_name(name);
  field.__set_transform(transform);
  field.__set_type(type);
  return field;
}

TIcebergPartitionField IdentityField(int32_t source_id, int32_t field_id,
    const string& name) {
  return MakeField(source_id, field_id, name, TIcebergPartitionTransformType::IDENTITY);
}

TIcebergPartitionSpec MakeSpec(int32_t spec_id,
    const vector<TIcebergPartitionField>& fields) {
  TIcebergPartitionSpec spec;
  spec.__set_spec_id(spec_id);
  spec.__set_partition_fields(fields);
  return spec;
}

/// Returns a serialized descriptor table with a single table 'db.t' with id TABLE_ID.
/// If 'specs' is not null, it is an Iceberg table with partition specs 'specs' in this
/// order and default partition spec id 'default_spec_id'.
TDescriptorTableSerialized SerializeDescriptorTable(
    const vector<TIcebergPartitionSpec>* specs, int32_t default_spec_id) {
  THdfsTable hdfs_table;
  hdfs_table.__set_hdfsBaseDir("hdfs://localhost/test-warehouse/db.db/t");
  hdfs_table.__set_nullPartitionKeyValue("__HIVE_DEFAULT_PARTITION__");
  hdfs_table.__set_nullColumnValue("\\N");
  TTableDescriptor tdesc;
  tdesc.__set_id(TABLE_ID);
  tdesc.__set_numClusteringCols(0);
  tdesc.__set_dbName("db");
  tdesc.__set_tableName("t");
  tdesc.__set_hdfsTable(hdfs_table);
  if (specs == nullptr) {
    tdesc.__set_tableType(TTableType::HDFS_TABLE);
  } else {
    TIcebergTable iceberg_table;
    iceberg_table.__set_table_location(hdfs_table.hdfsBaseDir);
    iceberg_table.__set_partition_spec(*specs);
    iceberg_table.__set_default_partition_spec_id(default_spec_id);
    iceberg_table.__set_format_version(2);
    tdesc.__set_tableType(TTableType::ICEBERG_TABLE);
    tdesc.__set_icebergTable(iceberg_table);
  }
  TDescriptorTable desc_tbl;
  desc_tbl.__set_tableDescriptors({tdesc});
  TDescriptorTableSerialized serialized;
  ThriftSerializer serializer(/* compact */ false);
  Status status = serializer.SerializeToString(&desc_tbl, &serialized.thrift_desc_tbl);
  EXPECT_OK(status);
  return serialized;
}

/// Creates the descriptor of an Iceberg table with partition specs 'specs' and default
/// partition spec id 'default_spec_id', like the coordinator does for a DML target.
Status CreateIcebergTableDescriptor(const vector<TIcebergPartitionSpec>& specs,
    int32_t default_spec_id, ObjectPool* pool, HdfsTableDescriptor** desc) {
  return DescriptorTbl::CreateHdfsTblDescriptor(
      SerializeDescriptorTable(&specs, default_spec_id), TABLE_ID, pool, desc);
}

vector<string> NonVoidFieldNames(const HdfsTableDescriptor& desc) {
  vector<string> names;
  for (const TIcebergPartitionField& field : desc.IcebergNonVoidPartitionFields()) {
    names.push_back(field.field_name);
  }
  return names;
}

} // anonymous namespace

// A table whose only partition spec has id 1, e.g. after Iceberg's
// ExpireSnapshots.cleanExpiredMetadata(true) removed the unused spec 0 (IMPALA-15461).
TEST(DescriptorsTest, IcebergNonDenseSpecIds) {
  ObjectPool pool;
  HdfsTableDescriptor* desc = nullptr;
  ASSERT_OK(CreateIcebergTableDescriptor(
      {MakeSpec(1, {IdentityField(2, 1000, "region")})}, 1, &pool, &desc));
  ASSERT_TRUE(desc != nullptr);
  EXPECT_TRUE(desc->IsIcebergTable());
  EXPECT_EQ(1, desc->IcebergSpecId());
  EXPECT_EQ(vector<string>({"region"}), NonVoidFieldNames(*desc));
  const TIcebergPartitionSpec* spec = desc->GetIcebergPartitionSpec(1);
  ASSERT_TRUE(spec != nullptr);
  EXPECT_EQ(1, spec->spec_id);
  ASSERT_EQ(1, spec->partition_fields.size());
  EXPECT_EQ("region", spec->partition_fields[0].field_name);
  EXPECT_TRUE(desc->GetIcebergPartitionSpec(0) == nullptr);
  EXPECT_TRUE(desc->GetIcebergPartitionSpec(2) == nullptr);
}

// Specs [1, 2] with default spec 1: position 1 of the list holds spec 2.
TEST(DescriptorsTest, IcebergDefaultSpecNotAtItsPosition) {
  ObjectPool pool;
  HdfsTableDescriptor* desc = nullptr;
  ASSERT_OK(CreateIcebergTableDescriptor(
      {MakeSpec(1, {}), MakeSpec(2, {IdentityField(2, 1000, "region")})}, 1, &pool,
      &desc));
  ASSERT_TRUE(desc != nullptr);
  EXPECT_EQ(1, desc->IcebergSpecId());
  EXPECT_TRUE(NonVoidFieldNames(*desc).empty());
  const TIcebergPartitionSpec* spec_1 = desc->GetIcebergPartitionSpec(1);
  ASSERT_TRUE(spec_1 != nullptr);
  EXPECT_EQ(1, spec_1->spec_id);
  EXPECT_TRUE(spec_1->partition_fields.empty());
  const TIcebergPartitionSpec* spec_2 = desc->GetIcebergPartitionSpec(2);
  ASSERT_TRUE(spec_2 != nullptr);
  EXPECT_EQ(2, spec_2->spec_id);
  EXPECT_EQ(1, spec_2->partition_fields.size());
}

// The list of specs is not ordered by spec id.
TEST(DescriptorsTest, IcebergSpecsOutOfIdOrder) {
  ObjectPool pool;
  HdfsTableDescriptor* desc = nullptr;
  ASSERT_OK(CreateIcebergTableDescriptor(
      {MakeSpec(2, {IdentityField(1, 1001, "id")}), MakeSpec(0, {}),
       MakeSpec(1, {IdentityField(2, 1000, "region")})},
      1, &pool, &desc));
  ASSERT_TRUE(desc != nullptr);
  EXPECT_EQ(1, desc->IcebergSpecId());
  EXPECT_EQ(vector<string>({"region"}), NonVoidFieldNames(*desc));
  for (int32_t spec_id : {0, 1, 2}) {
    const TIcebergPartitionSpec* spec = desc->GetIcebergPartitionSpec(spec_id);
    ASSERT_TRUE(spec != nullptr);
    EXPECT_EQ(spec_id, spec->spec_id);
  }
  const TIcebergPartitionSpec* spec_2 = desc->GetIcebergPartitionSpec(2);
  ASSERT_EQ(1, spec_2->partition_fields.size());
  EXPECT_EQ("id", spec_2->partition_fields[0].field_name);
}

// Dense spec ids, the common case. VOID partition fields are skipped.
TEST(DescriptorsTest, IcebergDenseSpecIds) {
  ObjectPool pool;
  HdfsTableDescriptor* desc = nullptr;
  ASSERT_OK(CreateIcebergTableDescriptor(
      {MakeSpec(0, {}),
       MakeSpec(1, {MakeField(2, 1000, "region", TIcebergPartitionTransformType::VOID),
                    IdentityField(1, 1001, "id")})},
      1, &pool, &desc));
  ASSERT_TRUE(desc != nullptr);
  EXPECT_EQ(1, desc->IcebergSpecId());
  EXPECT_EQ(vector<string>({"id"}), NonVoidFieldNames(*desc));
  ASSERT_TRUE(desc->GetIcebergPartitionSpec(0) != nullptr);
  EXPECT_EQ(0, desc->GetIcebergPartitionSpec(0)->spec_id);
  ASSERT_TRUE(desc->GetIcebergPartitionSpec(1) != nullptr);
  EXPECT_EQ(2, desc->GetIcebergPartitionSpec(1)->partition_fields.size());
}

// A descriptor without the default partition spec is an error, not a crash.
TEST(DescriptorsTest, IcebergMissingDefaultSpec) {
  const string expected_error = "Iceberg table db.t has no partition spec with the "
      "default partition spec id 1. Partition spec ids: [0]";
  vector<TIcebergPartitionSpec> specs = {MakeSpec(0, {})};
  {
    ObjectPool pool;
    HdfsTableDescriptor* desc = nullptr;
    Status status = CreateIcebergTableDescriptor(specs, 1, &pool, &desc);
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), expected_error);
    EXPECT_TRUE(desc == nullptr);
  }
  {
    // The same through DescriptorTbl::Create(), which fragment instances use.
    ObjectPool pool;
    DescriptorTbl* desc_tbl = nullptr;
    Status status =
        DescriptorTbl::Create(&pool, SerializeDescriptorTable(&specs, 1), &desc_tbl);
    ASSERT_FALSE(status.ok());
    EXPECT_STR_CONTAINS(status.GetDetail(), expected_error);
    ASSERT_TRUE(desc_tbl != nullptr);
    EXPECT_TRUE(desc_tbl->GetTableDescriptor(TABLE_ID) == nullptr);
    desc_tbl->ReleaseResources();
  }
}

// Non-Iceberg HDFS tables have no partition specs.
TEST(DescriptorsTest, HdfsTable) {
  ObjectPool pool;
  HdfsTableDescriptor* desc = nullptr;
  ASSERT_OK(DescriptorTbl::CreateHdfsTblDescriptor(
      SerializeDescriptorTable(nullptr, -1), TABLE_ID, &pool, &desc));
  ASSERT_TRUE(desc != nullptr);
  EXPECT_FALSE(desc->IsIcebergTable());
  EXPECT_TRUE(desc->IcebergNonVoidPartitionFields().empty());
  EXPECT_TRUE(desc->GetIcebergPartitionSpec(0) == nullptr);
  desc->ReleaseResources();
}

} // namespace impala
