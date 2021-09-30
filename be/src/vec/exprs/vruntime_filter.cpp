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

#include "vec/exprs/vruntime_filter.h"

namespace doris {
namespace vectorized {

Status VRuntimeFilterSlots::init(RuntimeState* state, int64_t hash_table_size) {
    DCHECK(_probe_expr_context.size() == _build_expr_context.size());
    // runtime filter effect stragety
    // 1. we will ignore IN filter when hash_table_size is too big
    // 2. we will ignore BLOOM filter and MinMax filter when hash_table_size
    // is too small and IN filter has effect

    std::map<int, bool> has_in_filter;

    auto ignore_filter = [state](int filter_id) {
        IRuntimeFilter* consumer_filter = nullptr;
        state->runtime_filter_mgr()->get_consume_filter(filter_id, &consumer_filter);
        DCHECK(consumer_filter != nullptr);
        consumer_filter->set_ignored();
        consumer_filter->signal();
    };

    for (auto& filter_desc : _runtime_filter_descs) {
        IRuntimeFilter* runtime_filter = nullptr;
        RETURN_IF_ERROR(state->runtime_filter_mgr()->get_producer_filter(filter_desc.filter_id,
                                                                         &runtime_filter));
        DCHECK(runtime_filter != nullptr);
        DCHECK(runtime_filter->expr_order() >= 0);
        DCHECK(runtime_filter->expr_order() < _probe_expr_context.size());

        if (runtime_filter->type() == RuntimeFilterType::IN_FILTER &&
            hash_table_size >= state->runtime_filter_max_in_num()) {
            ignore_filter(filter_desc.filter_id);
            continue;
        }
        if (has_in_filter[runtime_filter->expr_order()] && !runtime_filter->has_remote_target() &&
            runtime_filter->type() != RuntimeFilterType::IN_FILTER &&
            hash_table_size < state->runtime_filter_max_in_num()) {
            ignore_filter(filter_desc.filter_id);
            continue;
        }
        has_in_filter[runtime_filter->expr_order()] =
                (runtime_filter->type() == RuntimeFilterType::IN_FILTER);
        _runtime_filters[runtime_filter->expr_order()].push_back(runtime_filter);
        LOG(INFO) << "MYTEST: PUSH";
    }

    return Status::OK();
}

void VRuntimeFilterSlots::insert(RowRefList& rows) {
    for (int i = 0; i < _build_expr_context.size(); ++i) {
        auto iter = _runtime_filters.find(i);
        if (iter == _runtime_filters.end()) continue;
        for (const RowRef& row : rows) {
            int result_column_id = _build_expr_context[i]->get_last_result_column_id();
            auto& column = row.block->get_by_position(result_column_id).column;
            if (auto* nullable = check_and_get_column<ColumnNullable>(*column)) {
                auto& column_nested = nullable->get_nested_column();
                auto& column_nullmap = nullable->get_null_map_column();
                if (column_nullmap.get_bool(row.row_num)) {
                    continue;
                }
                const auto& ref_data = column_nested.get_data_at(row.row_num);
                for (auto filter : iter->second) {
                    filter->insert(ref_data.data);
                }
            } else {
                const auto& ref_data = column->get_data_at(row.row_num);
                for (auto filter : iter->second) {
                    filter->insert(ref_data.data);
                }
            }
        }
    }
}

void VRuntimeFilterSlots::publish() {
    for (int i = 0; i < _probe_expr_context.size(); ++i) {
        auto iter = _runtime_filters.find(i);
        if (iter != _runtime_filters.end()) {
            for (auto filter : iter->second) {
                filter->publish();
            }
        }
    }
    for (auto& pair : _runtime_filters) {
        for (auto filter : pair.second) {
            filter->publish_finally();
        }
    }
}

} // namespace vectorized
} // namespace doris
