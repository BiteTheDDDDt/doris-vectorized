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

#include "exprs/bloom_filter_function.h"
#include "exprs/hybrid_set.h"
#include "exprs/minmax_function.h"
#include "exprs/runtime_filter.h"
#include "olap/bloom_filter_predicate.h"

namespace doris {

class MinmaxFunctionTraits {
public:
    using BasePtr = MinMaxFuncBase*;
    template <PrimitiveType type, bool vectorized_enable>
    static BasePtr get_function(MemTracker* /*unused*/) {
        return new (std::nothrow)
                MinMaxNumFunc<typename PrimitiveTypeTraits<type, vectorized_enable>::CppType>();
    };
};

template <PrimitiveType type, bool vectorized_enable>
class HybridSetTraitsBase {
public:
    using Set = HybridSet<typename PrimitiveTypeTraits<type, vectorized_enable>::CppType>;
};

#define SPECIALIZATION_STRING_SET(TYPE)                  \
    template <bool vectorized_enable>                    \
    class HybridSetTraitsBase<TYPE, vectorized_enable> { \
    public:                                              \
        using Set = StringValueSet;                      \
    };

SPECIALIZATION_STRING_SET(TYPE_CHAR);
SPECIALIZATION_STRING_SET(TYPE_VARCHAR);
SPECIALIZATION_STRING_SET(TYPE_STRING);

class HybridSetTraits {
public:
    using BasePtr = HybridSetBase*;

    template <PrimitiveType type, bool vectorized_enable>
    static BasePtr get_function(MemTracker* /*unused*/) {
        return new (std::nothrow) typename HybridSetTraitsBase<type, vectorized_enable>::Set();
    };
};

class BloomFilterTraits {
public:
    using BasePtr = IBloomFilterFuncBase*;

    template <PrimitiveType type, bool vectorized_enable>
    static BasePtr get_function(MemTracker* tracker) {
        return new BloomFilterFunc<type, CurrentBloomFilterAdaptor>(tracker);
    };
};

template <class Traits, PrimitiveType type>
typename Traits::BasePtr create_predicate_function_raw(bool vectorized_enable = false,
                                                       MemTracker* tracker = nullptr) {
    if (vectorized_enable) {
        return Traits::template get_function<type, true>(tracker);
    } else {
        return Traits::template get_function<type, false>(tracker);
    }
}

template <class Traits>
typename Traits::BasePtr create_predicate_function(PrimitiveType type,
                                                   bool vectorized_enable = false,
                                                   MemTracker* tracker = nullptr) {
    switch (type) {
    case TYPE_BOOLEAN:
        return create_predicate_function_raw<Traits, TYPE_BOOLEAN>(vectorized_enable, tracker);
    case TYPE_TINYINT:
        return create_predicate_function_raw<Traits, TYPE_TINYINT>(vectorized_enable, tracker);
    case TYPE_SMALLINT:
        return create_predicate_function_raw<Traits, TYPE_SMALLINT>(vectorized_enable, tracker);
    case TYPE_INT:
        return create_predicate_function_raw<Traits, TYPE_INT>(vectorized_enable, tracker);
    case TYPE_BIGINT:
        return create_predicate_function_raw<Traits, TYPE_BIGINT>(vectorized_enable, tracker);
    case TYPE_LARGEINT:
        return create_predicate_function_raw<Traits, TYPE_LARGEINT>(vectorized_enable, tracker);

    case TYPE_FLOAT:
        return create_predicate_function_raw<Traits, TYPE_FLOAT>(vectorized_enable, tracker);
    case TYPE_DOUBLE:
        return create_predicate_function_raw<Traits, TYPE_DOUBLE>(vectorized_enable, tracker);

    case TYPE_DECIMALV2:
        return create_predicate_function_raw<Traits, TYPE_DECIMALV2>(vectorized_enable, tracker);

    case TYPE_DATE:
        return create_predicate_function_raw<Traits, TYPE_DATE>(vectorized_enable, tracker);
    case TYPE_DATETIME:
        return create_predicate_function_raw<Traits, TYPE_DATETIME>(vectorized_enable, tracker);

    case TYPE_CHAR:
        return create_predicate_function_raw<Traits, TYPE_CHAR>(vectorized_enable, tracker);
    case TYPE_VARCHAR:
        return create_predicate_function_raw<Traits, TYPE_VARCHAR>(vectorized_enable, tracker);
    case TYPE_STRING:
        return create_predicate_function_raw<Traits, TYPE_STRING>(vectorized_enable, tracker);

    default:
        DCHECK(false) << "Invalid type.";
    }

    return nullptr;
}
} // namespace doris
