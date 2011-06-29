// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Types.thrift"
include "Descriptors.thrift"
include "Exprs.thrift"
include "PlanNodes.thrift"

struct TExecutePlanRequest {
  1: PlanNodes.TPlan plan
  2: Descriptors.TDescriptorTable descTbl
  3: list<Exprs.TExpr> selectListExprs
  4: list<Types.TPrimitiveType> selectListExprTypes;
}
