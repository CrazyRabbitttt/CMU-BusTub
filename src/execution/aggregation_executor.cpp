//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();

  Tuple receive_tuple;
  RID receive_rid;
  while (child_->Next(&receive_tuple, &receive_rid)) {
    not_empty_flag_ = true;
    aht_.InsertCombine(MakeAggregateKey(&receive_tuple), MakeAggregateValue(&receive_tuple));
  }
  aht_iterator_ = aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 需要 group_by 属性 但是具体的插入到 hash_table 中的为空 (group_by 不能为空)
  if (!plan_->GetGroupBys().empty() && aht_.IsEmpty()) {
    return false;
  }

  if (!not_empty_flag_ && !finish_) {  // 空表
    std::vector<Value> vec;
    for (auto &it : plan_->GetAggregateTypes()) {
      if (it == AggregationType::CountStarAggregate) {
        vec.emplace_back(TypeId::INTEGER, 0);
      } else {
        vec.emplace_back(ValueFactory::GetNullValueByType(TypeId::INTEGER));
      }
    }

    *tuple = Tuple(std::move(vec), &GetOutputSchema());
    finish_ = true;
    return true;
  }
  if (aht_iterator_ == aht_.End()) {
    return false;
  }

  std::vector<Value> res_value{aht_iterator_.Key().group_bys_};
  for (auto &it : aht_iterator_.Val().aggregates_) {
    res_value.emplace_back(it);
  }

  *tuple = Tuple(std::move(res_value), &GetOutputSchema());
  aht_iterator_.operator++();
  return true;
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_.get(); }

}  // namespace bustub
