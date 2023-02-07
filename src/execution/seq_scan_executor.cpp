//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_heap_(exec_ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get()),
      iter_(table_heap_->Begin(exec_ctx->GetTransaction())) {}

// fine, I just don't call this virtual function in constructor
void SeqScanExecutor::Init() {
  //  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  //  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

/*
 * 我们即使是进行 seq scan, 也不是说所有的 column 都是需要进行选取的， 这里通过 schema 进行过滤
 */
auto SeqScanExecutor::GetValuesFromTuple(const Tuple *tuple, const Schema *schema) -> std::vector<Value> {
  std::vector<Value> res;
  for (const auto &col : schema->GetColumns()) {
    Value tmp = tuple->GetValue(schema, schema->GetColIdx(col.GetName()));
    res.emplace_back(tmp);
  }
  return res;
}

// @return `true` if a tuple was produced,
// `false` if there are no more tuples (means the iterator if the end of the table)
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == table_heap_->End()) {
    Schema res_schema{{Column("resname", TypeId::INTEGER)}};
    *tuple = Tuple{{Value{INTEGER, 0}}, &res_schema};
    return false;
  }
  // 将对应的 values 进行过滤(plan中的 Schema), 然后生成一个新的 Tuple
  std::vector<Value> tmp = GetValuesFromTuple(iter_.operator->(), &GetOutputSchema());

  Tuple tmp_tuple(tmp, &GetOutputSchema());
  *tuple = tmp_tuple;
  *rid = iter_->GetRid();
  iter_++;
  return true;
}

}  // namespace bustub
