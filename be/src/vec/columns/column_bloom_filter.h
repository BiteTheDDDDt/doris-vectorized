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

#pragma once

#include "exprs/bloomfilter_predicate.h"
#include "vec/columns/column_dummy.h"

namespace doris::vectorized {

using ConstBloomFilterPtr = std::shared_ptr<IBloomFilterFuncBase>;

class ColumnBloomFilter final : public COWHelper<IColumnDummy, ConstBloomFilterPtr> {
public:
    friend class COWHelper<IColumnDummy, ConstBloomFilterPtr>;

    ConstBloomFilterPtr(size_t size, const ConstBloomFilterPtr& data) : _data(data) { s = size; }
    ConstBloomFilterPtr(const ConstBloomFilterPtr&) = default;

    const char* get_family_name() const override { return "BloomFilter"; }
    MutableColumnPtr clone_dummy(size_t size) const override {
        return ColumnBloomFilter::create(size, data);
    }

    ConstBloomFilterPtr get_data() const { return data; }

private:
    ConstBloomFilterPtr _data;
};

} // namespace doris::vectorized