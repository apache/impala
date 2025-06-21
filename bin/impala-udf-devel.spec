# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

Name:           impala-udf-devel
Version:        %{version}
Release:        1%{?dist}
Summary:        Apache Impala UDF development package

License:        Apache-2.0
Group:          Development/Libraries
Source0:        %{name}-%{version}.tar.gz

# some rhel distros error out if there are no symbols or source files so
# skip creating -debuginfo subpackage
%global debug_package %{nil}

%description
This RPM provides Apache Impala UDF headers, and shared library.

%prep
%setup -q

%build

%install
mkdir -p %{buildroot}/usr/include/impala_udf
cp -a usr/include/impala_udf/* %{buildroot}/usr/include/impala_udf/

mkdir -p %{buildroot}/usr/lib64
cp -a usr/lib64/* %{buildroot}/usr/lib64/

%files
/usr/include/impala_udf/uda-test-harness-impl.h
/usr/include/impala_udf/uda-test-harness.h
/usr/include/impala_udf/udf-debug.h
/usr/include/impala_udf/udf-test-harness.h
/usr/include/impala_udf/udf.h
/usr/lib64/libImpalaUdf-debug.a
/usr/lib64/libImpalaUdf-retail.a

