#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();

  Tuple receive_tuple;
  RID receive_rid;
  while (child_executor_->Next(&receive_tuple, &receive_rid)) {
    tuples_.emplace_back(receive_tuple);
  }

  std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto &[order_by_type_, expre] : plan_->GetOrderBy()) {
      OrderByType order_by_type = order_by_type_;
      AbstractExpressionRef express = expre;
      Value value_of_a = express->Evaluate(&a, child_executor_->GetOutputSchema());
      Value value_of_b = express->Evaluate(&b, child_executor_->GetOutputSchema());

      if (value_of_a.CompareEquals(value_of_b) == CmpBool::CmpTrue) {
        continue;
      }

      if (order_by_type == OrderByType::DESC) {
        return (value_of_a.CompareGreaterThan(value_of_b) == CmpBool::CmpTrue);
      }
      return (value_of_a.CompareLessThan(value_of_b) == CmpBool::CmpTrue);
    }
    return true;
  });
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (out_put_index_ != static_cast<int>(tuples_.size())) {
    *tuple = tuples_[out_put_index_++];
    return true;
  }
  return false;
}

}  // namespace bustub
