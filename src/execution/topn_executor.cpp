#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), limit_n_(plan_->GetN()), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();

  std::function<bool(const Tuple &a, const Tuple &b)> func = [&](const Tuple &a, const Tuple &b) -> bool {
    for (auto &[order_by_type, express] : plan_->GetOrderBy()) {
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
  };

  little_heap_ = Priority_Queue(func);

  Tuple receive_tuple;
  RID receive_rid;
  while (child_executor_->Next(&receive_tuple, &receive_rid)) {
    // 小根堆还没满
    if (cur_size_ < limit_n_) {
      little_heap_.push(std::move(receive_tuple));  // 内部是按照 谁的优先级比较低谁就排在堆顶部
      cur_size_++;
    } else {
      if (!func(little_heap_.top(), receive_tuple)) {  // 堆顶部的 tuple 的优先级是比较低的， 可以进行替换
        little_heap_.pop();
        little_heap_.push(std::move(receive_tuple));
      }
    }
    assert(little_heap_.size() <= limit_n_);
  }

  // 目前已经是维护好了一个小根堆：内部就就是 limit n【堆顶向下升序】
  while (!little_heap_.empty()) {
    res_stack_.push(little_heap_.top());
    little_heap_.pop();
  }
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (!res_stack_.empty()) {
    *tuple = res_stack_.top();
    res_stack_.pop();
    return true;
  }
  return false;
}

}  // namespace bustub
