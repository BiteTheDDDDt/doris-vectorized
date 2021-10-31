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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_BLOOM_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_BLOOM_PREDICATE_H
#include <algorithm>
#include <cmath>
#include <memory>
#include <string>

#include "common/object_pool.h"
#include "exprs/block_bloom_filter.hpp"
#include "exprs/bloom_filter_function.h"
#include "exprs/predicate.h"
#include "olap/bloom_filter.hpp"
#include "olap/decimal12.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/uint24.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"

namespace doris {

// BloomFilterPredicate only used in runtime filter
class BloomFilterPredicate : public Predicate {
public:
    virtual ~BloomFilterPredicate();
    BloomFilterPredicate(const TExprNode& node);
    BloomFilterPredicate(const BloomFilterPredicate& other);
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BloomFilterPredicate(*this));
    }
    Status prepare(RuntimeState* state, IBloomFilterFuncBase* bloomfilterfunc);

    std::shared_ptr<IBloomFilterFuncBase> get_bloom_filter_func() { return _filter; }

    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;

    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope) override;

protected:
    friend class Expr;
    virtual std::string debug_string() const override;

private:
    bool _is_prepare;
    // if we set always = true, we will skip bloom filter
    bool _always_true;
    /// TODO: statistic filter rate in the profile
    std::atomic<int64_t> _filtered_rows;
    std::atomic<int64_t> _scan_rows;

    std::shared_ptr<IBloomFilterFuncBase> _filter;
    bool _has_calculate_filter = false;
    // loop size must be power of 2
    constexpr static int64_t _loop_size = 8192;
    // if filter rate less than this, bloom filter will set always true
    constexpr static double _expect_filter_rate = 0.2;
};

} // namespace doris
#endif
