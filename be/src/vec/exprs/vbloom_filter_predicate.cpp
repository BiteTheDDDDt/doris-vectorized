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

#include "vec/exprs/vbloom_filter_predicate.h"

#include "vec/columns/column_bloom_filter.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

VBloomFilterPredicate::VBloomFilterPredicate(const TExprNode& node)
        : VExpr(node),
          _has_calculate_filter(false),
          _is_prepare(false),
          _filtered_rows(0),
          _scan_rows(0) {}

Status VBloomFilterPredicate::prepare(RuntimeState* state, const RowDescriptor& desc,
                                      VExprContext* context) {
    RETURN_IF_ERROR(VExpr::prepare(state, desc, context));

    if (_is_prepare) {
        return Status::OK();
    }
    if (_children.size() < 1) {
        return Status::InternalError("No Function operator bloom filter.");
    }

    _bloom_filter.reset(IBloomFilterFuncBase::create_bloom_filter(state->fragment_mem_tracker(),
                                                                  _children[0]->type().type, true));
    if (_bloom_filter.get() == nullptr) {
        return Status::InternalError("Unknown column type.");
    }

    _expr_name = fmt::format("({} bloom filter)", _children[0]->expr_name());
    _is_prepare = true;
    return Status::OK();
}

Status VBloomFilterPredicate::prepare(RuntimeState* state, IBloomFilterFuncBase* bloom_filter) {
    if (_is_prepare) {
        return Status::OK();
    }

    _bloom_filter.reset(bloom_filter);

    if (_bloom_filter.get() == nullptr) {
        return Status::InternalError("Unknown column type.");
    }
    if (_children.size() < 1) {
        return Status::InternalError("No Function operator bloom filter.");
    }

    _expr_name = fmt::format("({} bloom filter)", _children[0]->expr_name());
    _is_prepare = true;
    return Status::OK();
}

Status VBloomFilterPredicate::open(RuntimeState* state, VExprContext* context) {
    RETURN_IF_ERROR(VExpr::open(state, context));

    _bloom_filter_param = COWHelper<IColumn, ColumnSet>::create(1, _bloom_filter);

    DCHECK(_children.size() > 1);
    auto child = _children[0];
    const auto& child_name = child->expr_name();
    auto child_column = child->data_type()->create_column();
    _argument_template.reserve(2);
    _argument_template.emplace_back(std::move(child_column), child->data_type(), child_name);
    _argument_template.emplace_back(std::move(child_column), child->data_type(), child_name);

    _function = SimpleFunctionFactory::instance().get_function(function_name, _argument_template,
                                                               _data_type);
    if (_function == nullptr) {
        return Status::NotSupported(
                fmt::format("Function {} is not implemented", _fn.name.function_name));
    }

    return Status::OK();
}

Status VBloomFilterPredicate::change_mode() {
    _function = SimpleFunctionFactory::instance().get_function(function_name + "_always_true",
                                                               _argument_template, _data_type);
    if (_function == nullptr) {
        return Status::NotSupported(
                fmt::format("Function {} is not implemented", _fn.name.function_name));
    }

    return Status::OK();
}

void VBloomFilterPredicate::close(RuntimeState* state, VExprContext* context) {
    VExpr::close(state, context);
}

Status VBloomFilterPredicate::execute(vectorized::Block* block, int* result_column_id) {
    // for each child call execute
    vectorized::ColumnNumbers arguments(2);
    int column_id = -1;
    _children[0]->execute(block, &column_id);
    arguments[0] = column_id;

    size_t const_param_id = block->columns();
    // here we should use column_set type, to simplify the code. we dirrect use the DataTypeInt64
    block->insert({_bloom_filter_param, std::make_shared<DataTypeInt64>(), "bloom filter"});
    arguments[1] = const_param_id;

    // call function
    size_t num_columns_without_result = block->columns();
    // prepare a column to save result
    block->insert({nullptr, _data_type, _expr_name});
    _function->execute(*block, arguments, num_columns_without_result, block->rows(), false);
    *result_column_id = num_columns_without_result;

    if (!_has_calculate_filter) {
        ColumnPtr result_column_ptr = block.get_by_position(*result_column_id).column;
        auto* nullable = check_and_get_column<ColumnNullable>(*result_column_ptr);
        const auto& nested_column = nullable->get_nested_column();
        const auto& null_map = nullable->get_null_map_column().get_data();

        for (size_t i = 0; i < block->rows(); i++) {
            if (null_map[i]) {
                continue;
            }
            _scan_rows++;
            _filtered_rows += !nested_column.get_bool(i);
        }

        if (_scan_rows >= _loop_size) {
            double rate = (double)_filtered_rows / _scan_rows;
            if (rate < _expect_filter_rate) {
                RETURN_IF_ERROR(change_mode());
            }
            _has_calculate_filter = true;
        }
    }

    return Status::OK();
}

const std::string& VInPredicate::expr_name() const {
    return _expr_name;
}

} // namespace doris::vectorized