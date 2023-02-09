#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  /** 上层节点是 limit， 下层节点是 sort */
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSortLimitAsTopN(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 目前是 limit, 子节点是 Sort就可以优化
  if (optimized_plan->GetType() == PlanType::Limit) {
    const auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto child_plan = limit_plan.GetChildPlan();
    if (child_plan->GetType() == PlanType::Sort) {
      const auto &sort_plan = dynamic_cast<const SortPlanNode &>(*child_plan);
      const auto &order_bys = sort_plan.GetOrderBy();
      return std::make_shared<TopNPlanNode>(optimized_plan->output_schema_, sort_plan.GetChildPlan(), order_bys,
                                            limit_plan.GetLimit());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
