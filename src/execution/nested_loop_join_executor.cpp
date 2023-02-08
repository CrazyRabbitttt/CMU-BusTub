//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/*
 * TODO:
 *  for out_tuple in out_table
 *    for in_tuple in in_table
 *      if out_tuple matches in_tuple
 *        emit
 *   但是每次都只能返回一个结果，所以需要保存一些上下文
 */

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  Tuple receive_tuple;
  RID receive_rid;
  // temporary store the right tuples
  while (right_executor_->Next(&receive_tuple, &receive_rid)) {
    right_tuples_.emplace_back(receive_tuple);
  }
  last_right_index_ = right_tuples_.size() - 1;
}

auto NestedLoopJoinExecutor::CombineTuples(Tuple *left_tuple, Tuple *right_tuple, const Schema &left_schema,
                                           const Schema &right_schema, const Schema &final_schema) -> Tuple {
  std::vector<Value> res_values;
  for (uint64_t i = 0; i < left_schema.GetColumns().size(); i++) {
    res_values.emplace_back(left_tuple->GetValue(&left_schema, i));
  }
  for (uint64_t i = 0; i < right_schema.GetColumns().size(); i++) {
    res_values.emplace_back(right_tuple->GetValue(&right_schema, i));
  }
  return {std::move(res_values), &final_schema};
}

auto NestedLoopJoinExecutor::CombineLeftJoinTuple(Tuple *left_tuple, Tuple *right_tuple, const Schema &left_schema,
                                                  const Schema &right_schema, const Schema &final_schema) -> Tuple {
  std::vector<Value> res_values;
  for (uint64_t i = 0; i < left_schema.GetColumns().size(); i++) {
    res_values.emplace_back(left_tuple->GetValue(&left_schema, i));
  }
  // 剩下的都填充为 null
  for (uint64_t i = 0; i < right_schema.GetColumns().size(); i++) {
    res_values.emplace_back(ValueFactory::GetNullValueByType(right_schema.GetColumn(i).GetType()));
    //    res_values.emplace_back(right_schema.GetColumn(i).GetType());  // insert invalid value
  }
  return {std::move(res_values), &final_schema};
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple left_tuple;
  RID left_rid;

  int right_table_size = right_tuples_.size();

  const Schema left_schema = left_executor_->GetOutputSchema();
  const Schema right_schema = right_executor_->GetOutputSchema();

  const Schema final_schema{GetOutputSchema()};  // 构造最终的 schema

  bool has_no_left_data = (last_right_index_ >= right_table_size - 1);
  int i{};
  if (!has_no_left_data) {  // 还有上次没有处理完的数据
    for (i = last_right_index_ + 1; i < right_table_size; i++) {
      Tuple right_tuple = right_tuples_[i];

      Value match = plan_->Predicate().EvaluateJoin(&last_left_tuple_, left_schema, &right_tuple, right_schema);
      if (match.GetAs<bool>()) {
        last_right_index_ = i;  // 保存上次的结果
        std::vector<Value> values{};
        //        if (plan_->GetJoinType() == JoinType::INNER) {
        *tuple = CombineTuples(&last_left_tuple_, &right_tuple, left_schema, right_schema, final_schema);
        //        }
        return true;
      }
    }
  }

  // 剩余的数据都处理完了，遍历新的左边节点
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    bool if_matched_once = false;  // 判断左边的数据是否进行了至少一次的match， 否则 left join 需要进行一次新的
    for (i = 0; i < right_table_size; i++) {
      Tuple right_tuple = right_tuples_[i];

      Value match = plan_->Predicate().EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema);
      if (match.GetAs<bool>()) {
        if_matched_once = true;
        last_left_tuple_ = left_tuple;  // 本次的结果已经是 match 了， 但是需要保存一下
        last_right_index_ = i;          // 保存上次的结果

        *tuple = CombineTuples(&left_tuple, &right_tuple, left_schema, right_schema, final_schema);
        return true;
      }
    }
    if (!if_matched_once && plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = CombineLeftJoinTuple(&left_tuple, right_tuples_.data(), left_schema, right_schema, final_schema);
      return true;
    }
  }

  return false;
}

}  // namespace bustub
