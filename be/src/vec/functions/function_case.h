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

#include "vec/data_types/data_type_nullable.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct CaseState {
    DataTypePtr result_type = nullptr;
};

template <bool has_case, bool has_else>
struct FunctionCaseName;

template <>
struct FunctionCaseName<false, false> {
    static constexpr auto name = "case";
};

template <>
struct FunctionCaseName<true, false> {
    static constexpr auto name = "case_has_case";
};

template <>
struct FunctionCaseName<false, true> {
    static constexpr auto name = "case_has_else";
};

template <>
struct FunctionCaseName<true, true> {
    static constexpr auto name = "case_has_case_has_else";
};

template <bool has_case, bool has_else>
class FunctionCase : public IFunction {
public:
    static constexpr auto name = FunctionCaseName<has_case, has_else>::name;
    static FunctionPtr create() { return std::make_shared<FunctionCase>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 0; }
    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        int loop_start = has_case ? 2 : 1;
        int loop_end = has_else ? arguments.size() - 1 : arguments.size();

        bool is_nullable = false;
        if (!has_else || arguments[loop_end].get()->is_nullable()) {
            is_nullable = true;
        }
        for (int i = loop_start; !is_nullable && i < loop_end; i += 2) {
            if (arguments[i].get()->is_nullable()) {
                is_nullable = true;
            }
        }

        if (is_nullable) {
            return make_nullable(arguments[loop_start]);
        } else {
            return arguments[loop_start];
        }
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    template <bool has_null>
    Status execute_impl_has_case(const DataTypePtr& data_type, Block& block,
                                 const ColumnNumbers& arguments, size_t result,
                                 size_t input_rows_count) {
        auto result_column_ptr = data_type->create_column();
        auto case_column_ptr = block.get_by_position(arguments[0]).column;
        auto else_column_ptr =
                has_else ? block.get_by_position(arguments[arguments.size() - 1]).column : nullptr;

        for (int row_idx = 0; row_idx < input_rows_count; row_idx++) {
            bool insert_else = true;
            if (!case_column_ptr->is_null_at(row_idx)) {
                for (int arg_idx = 1; arg_idx + 1 < arguments.size(); arg_idx += 2) {
                    auto when_column_ptr = block.get_by_position(arguments[arg_idx]).column;
                    if (when_column_ptr->is_null_at(row_idx)) {
                        continue;
                    }

                    if (case_column_ptr->compare_at_adapted(row_idx, row_idx, *when_column_ptr,
                                                            -1) == 0) {
                        auto then_column_ptr = block.get_by_position(arguments[arg_idx + 1]).column;
                        result_column_ptr->insert_from_adapted(*then_column_ptr, row_idx);
                        insert_else = false;
                        break;
                    }
                }
            }

            if (insert_else) {
                if constexpr (has_else) {
                    result_column_ptr->insert_default();
                } else {
                    result_column_ptr->insert_from_adapted(*else_column_ptr, row_idx);
                }
            }
        }

        block.replace_by_position(result, std::move(result_column_ptr));
        return Status::OK();
    }

    template <bool has_null, bool insert_not_nullable, typename ColumnType>
    Status execute_impl_no_case(const DataTypePtr& data_type, Block& block,
                                const ColumnNumbers& arguments, size_t result,
                                size_t input_rows_count) {
        std::vector<uint8_t> then_idx(input_rows_count, 0);

        ColumnWithTypeAndName* columns_ptr[arguments.size()];

        for (uint8_t arg_idx = 0; arg_idx + 1 < arguments.size(); arg_idx += 2) {
            auto when_column_ptr = block.get_by_position(arguments[arg_idx]).column;
            const auto& cond_data =
                    reinterpret_cast<const ColumnUInt8*>(when_column_ptr.get())->get_data();

            for (int row_idx = 0; row_idx < input_rows_count; row_idx++) {
                if constexpr (has_null) {
                    if (when_column_ptr->is_null_at(row_idx)) {
                        continue;
                    }
                }

                then_idx[row_idx] |= (!then_idx[row_idx] && cond_data[row_idx]) * (arg_idx + 1);
            }

            columns_ptr[arg_idx + 1] = &block.get_by_position(arguments[arg_idx + 1]);
        }

        auto result_column_ptr = data_type->create_column();
        
        auto else_column_ptr =
                has_else ? block.get_by_position(arguments[arguments.size() - 1]).column : nullptr;
        auto* tmp_nullable_column_ptr =
                has_null ? reinterpret_cast<ColumnNullable*>(result_column_ptr.get()) : nullptr;
        auto* tmp_column_ptr = reinterpret_cast<ColumnType*>(
                has_null ? &tmp_nullable_column_ptr->get_nested_column() : result_column_ptr.get());

        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            for (int row_idx = 0; row_idx < input_rows_count; row_idx++) {
                if (!then_idx[row_idx]) {
                    if constexpr (has_else) {
                        tmp_column_ptr->insert_from(*else_column_ptr, row_idx);
                    } else {
                        if constexpr (has_null) {
                            tmp_nullable_column_ptr->insert_default();
                        } else {
                            tmp_column_ptr->insert_default();
                        }
                    }
                } else {
                    if constexpr (insert_not_nullable) {
                        tmp_nullable_column_ptr->insert_from_not_nullable(
                                *columns_ptr[then_idx[row_idx]]->column, row_idx);
                    } else {
                        tmp_column_ptr->insert_from(*columns_ptr[then_idx[row_idx]]->column,
                                                    row_idx);
                    }
                }
            }
        } else {
            result_column_ptr->reserve(input_rows_count);

            auto& tmp_data = tmp_column_ptr->get_data();
            auto* null_map = has_null ? &(tmp_nullable_column_ptr->get_null_map_data()) : nullptr;

            // set default
            for (int row_idx = 0; row_idx < input_rows_count; row_idx++) {
                tmp_data[row_idx] = 0;
                if constexpr (has_null) {
                    (*null_map)[row_idx] = 1;
                }
            }

            // TODO: simd here and optmize other condition
            for (uint8_t arg_idx = 1; arg_idx < arguments.size(); arg_idx += 2) {
                auto& column_data =
                        reinterpret_cast<const ColumnType*>(columns_ptr[arg_idx]->column.get())
                                ->get_data();
                for (int row_idx = 0; row_idx < input_rows_count; row_idx++) {
                    if constexpr (std::is_same_v<ColumnType, ColumnFloat64> ||
                                  std::is_same_v<ColumnType, ColumnFloat32>) {
                        tmp_data[row_idx] += (then_idx[row_idx] == arg_idx) * column_data[row_idx];
                    } else {
                        tmp_data[row_idx] |= (then_idx[row_idx] == arg_idx) * column_data[row_idx];
                    }

                    if constexpr (has_null) {
                        (*null_map)[row_idx] = 0;
                    }
                }
            }

            if constexpr (has_else) {
                auto& else_data =
                        reinterpret_cast<const ColumnType*>(else_column_ptr.get())->get_data();
                for (int row_idx = 0; row_idx < input_rows_count; row_idx++) {
                    if constexpr (std::is_same_v<ColumnType, ColumnFloat64> ||
                                  std::is_same_v<ColumnType, ColumnFloat32>) {
                        tmp_data[row_idx] += (!then_idx[row_idx]) * else_data[row_idx];
                    } else {
                        tmp_data[row_idx] |= (!then_idx[row_idx]) * else_data[row_idx];
                    }

                    if constexpr (has_null) {
                        (*null_map)[row_idx] = 0;
                    }
                }
            }
        }

        block.replace_by_position(result, std::move(result_column_ptr));
        return Status::OK();
    }

    template <bool has_null, bool insert_not_nullable, typename ColumnType>
    Status execute_impl_raw(const DataTypePtr& data_type, Block& block,
                            const ColumnNumbers& arguments, size_t result,
                            size_t input_rows_count) {
        if constexpr (has_case) {
            return execute_impl_has_case<has_null>(data_type, block, arguments, result,
                                                   input_rows_count);
        } else {
            return execute_impl_no_case<has_null, insert_not_nullable, ColumnType>(
                    data_type, block, arguments, result, input_rows_count);
        }
    }

    template <bool has_null, typename ColumnType>
    Status execute_get_insert_from_not_null(const DataTypePtr& data_type, Block& block,
                                            const ColumnNumbers& arguments, size_t result,
                                            size_t input_rows_count) {
        bool insert_not_nullable = false;
        auto when_column_ptr = block.get_by_position(arguments[0]).column;
        if (has_null && !when_column_ptr->is_nullable()) {
            insert_not_nullable = true;
        }

        if (insert_not_nullable) {
            return execute_impl_raw<has_null, true, ColumnType>(data_type, block, arguments, result,
                                                                input_rows_count);
        } else {
            return execute_impl_raw<has_null, false, ColumnType>(data_type, block, arguments,
                                                                 result, input_rows_count);
        }
    }

    template <bool has_null>
    Status execute_get_type(const DataTypePtr& data_type, Block& block,
                            const ColumnNumbers& arguments, size_t result,
                            size_t input_rows_count) {
        WhichDataType which(data_type);

        if (which.is_uint8()) {
            return execute_get_insert_from_not_null<has_null, ColumnUInt8>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_int16()) {
            return execute_get_insert_from_not_null<has_null, ColumnInt16>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_uint32()) {
            return execute_get_insert_from_not_null<has_null, ColumnUInt32>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_uint64()) {
            return execute_get_insert_from_not_null<has_null, ColumnUInt64>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_int8()) {
            return execute_get_insert_from_not_null<has_null, ColumnInt8>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_int16()) {
            return execute_get_insert_from_not_null<has_null, ColumnInt16>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_int32()) {
            return execute_get_insert_from_not_null<has_null, ColumnInt32>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_int64()) {
            return execute_get_insert_from_not_null<has_null, ColumnInt64>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_date_or_datetime()) {
            return execute_get_insert_from_not_null<has_null, ColumnVector<DateTime>>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_float32()) {
            return execute_get_insert_from_not_null<has_null, ColumnFloat32>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_float64()) {
            return execute_get_insert_from_not_null<has_null, ColumnFloat64>(
                    data_type, block, arguments, result, input_rows_count);
        } else if (which.is_string()) {
            return execute_get_insert_from_not_null<has_null, ColumnString>(
                    data_type, block, arguments, result, input_rows_count);
        } else {
            LOG(FATAL) << fmt::format("Unexpected type {} of argument of function {}",
                                      data_type->get_name(), get_name());
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        CaseState* case_state = reinterpret_cast<CaseState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (case_state->result_type->is_nullable()) {
            return execute_get_type<true>(case_state->result_type, block, arguments, result,
                                          input_rows_count);
        } else {
            return execute_get_type<false>(case_state->result_type, block, arguments, result,
                                           input_rows_count);
        }
    }
};

} // namespace doris::vectorized