<!---
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
-->
# How to update the CRoaring library

The CRoaring library offers amalgamated files for each release. Source
amalgamation means that all the source files of a C/C++ libary can
be combined into very few files. In case of CRoaring:

- roaring.h
- roaring.c

So one just needs to download the amalgamated files and override the
contents of this folder. The files can be found at
https://github.com/RoaringBitmap/CRoaring/releases
