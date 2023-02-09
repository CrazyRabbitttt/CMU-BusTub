//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// topn_executor.h
//
// Identification: src/include/execution/executors/topn_executor.h
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>
#include <memory>
#include <queue>
#include <stack>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "execution/plans/topn_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * The TopNExecutor executor executes a topn.
 */
class TopNExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new TopNExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The topn plan to be executed
   */
  TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan, std::unique_ptr<AbstractExecutor> &&child_executor);

  /** Initialize the topn */
  void Init() override;

  /**
   * Yield the next tuple from the topn.
   * @param[out] tuple The next tuple produced by the topn
   * @param[out] rid The next tuple RID produced by the topn
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the topn */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  //  struct Cmp {
  //    bool operator()(const Tuple &a, const Tuple &b) {
  //      for (auto &[order_by_type, express] : order_bys_) {
  //        Value value_of_a = express->Evaluate(&a, child_executor_->GetOutputSchema());
  //        Value value_of_b = express->Evaluate(&b, child_executor_->GetOutputSchema());
  //
  //        if (value_of_a.CompareEquals(value_of_b) == CmpBool::CmpTrue) {
  //          continue;
  //        }
  //
  //        if (order_by_type == OrderByType::DESC) {
  //          return !(value_of_a.CompareGreaterThan(value_of_b) == CmpBool::CmpTrue);
  //        } else {
  //          return !(value_of_a.CompareLessThan(value_of_b) == CmpBool::CmpTrue);
  //        }
  //      }
  //      return true;
  //    }
  //  };

 private:
  using Priority_Queue =
      std::priority_queue<Tuple, std::vector<Tuple>, std::function<bool(const Tuple &a, const Tuple &b)>>;

  /** The topn plan node to be executed */
  const TopNPlanNode *plan_;

  size_t limit_n_{0};

  std::unique_ptr<AbstractExecutor> child_executor_;

  //  std::vector<std::pair<OrderByType, AbstractExpressionRef>> order_bys_;

  size_t cur_size_{0};

  std::stack<Tuple> res_stack_;

  Priority_Queue little_heap_;
  //  std::priority_queue<Tuple, std::vector<Tuple>, Cmp> little_heap_;
};
}  // namespace bustub
