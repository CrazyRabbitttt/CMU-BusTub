//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() {
  child_executor_->Init();

  // 初始化右表的数据
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
}

auto NestIndexJoinExecutor::CombineTuples(Tuple *left_tuple, Tuple *right_tuple, const Schema &left_schema,
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

auto NestIndexJoinExecutor::CombineLeftJoinTuple(Tuple *left_tuple, const Schema &left_schema,
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

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple receive_tuple;
  RID receive_rid;

  auto key_predicate = plan_->KeyPredicate();
  while (child_executor_->Next(&receive_tuple, &receive_rid)) {
    Value key_for_index = key_predicate->Evaluate(&receive_tuple, child_executor_->GetOutputSchema());
    //    std::cout << "The key from the out table to search in index:" << key_for_index.ToString() << std::endl;
    std::vector<RID> res_rids{};
    Schema tmp_schema{{Column{"probe key", key_for_index.GetTypeId()}}};
    Tuple tmp_tuple({key_for_index}, &tmp_schema);
    tree_->ScanKey(tmp_tuple, &res_rids, exec_ctx_->GetTransaction());

    if (res_rids.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      *tuple = CombineLeftJoinTuple(&receive_tuple, child_executor_->GetOutputSchema(), plan_->InnerTableSchema(),
                                    plan_->OutputSchema());
      return true;
    }

    if (res_rids.empty() && plan_->GetJoinType() == JoinType::INNER) {
      continue;
    }

    // 目前获得了 right_tuple's rid
    RID right_rid = res_rids[0];
    Tuple right_tuple{};
    table_info_->table_->GetTuple(right_rid, &right_tuple, exec_ctx_->GetTransaction());

    *tuple = CombineTuples(&receive_tuple, &right_tuple, child_executor_->GetOutputSchema(), plan_->InnerTableSchema(),
                           plan_->OutputSchema());
    return true;
  }

  return false;
}

}  // namespace bustub
