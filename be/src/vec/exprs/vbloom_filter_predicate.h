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
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris::vectorized {
class VBloomFilterPredicate final : public VExpr {
public:
    VBloomFilterPredicate(const TExprNode& node);
    ~VBloomFilterPredicate() = default;

    Status execute(Block* block, int* result_column_id) override;

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status prepare(RuntimeState* state, IBloomFilterFuncBase* bloom_filter);

    Status open(RuntimeState* state, VExprContext* context) override;

    void close(RuntimeState* state, VExprContext* context) override;
    VExpr* clone(ObjectPool* pool) const override {
        return pool->add(new VBloomFilterPredicate(*this));
    }
    const std::string& expr_name() const override;

private:
    Status change_mode();
    // if we set always = true, we will skip bloom filter
    bool _has_calculate_filter;
    bool _is_prepare;
    std::string _expr_name;

    ColumnsWithTypeAndName _argument_template;
    FunctionBasePtr _function;
    ColumnPtr _bloom_filter_param;
    std::shared_ptr<IBloomFilterFuncBase> _bloom_filter;

    /// TODO: statistic filter rate in the profile
    std::atomic<int64_t> _filtered_rows;
    std::atomic<int64_t> _scan_rows;

    static constexpr char* function_name = "bloomfilter";
    // loop size must be power of 2
    static constexpr int64_t _loop_size = 8192;
    // if filter rate less than this, bloom filter will set always true
    static constexpr double _expect_filter_rate = 0.2;
};
} // namespace doris::vectorized