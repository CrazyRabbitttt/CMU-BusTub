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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {}

// fine, I just don't call this virtual function in constructor
void SeqScanExecutor::Init() {
  // if we want to sequentially get the value of the table, need the iterator to get it
//  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get(); // get in unique_ptr is get the raw pointer
//  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

// @return `true` if a tuple was produced, `false` if there are no more tuples (means the iterator if the end of the table)
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
//  // get the next tuple & rid
//  iter_++;
//  if (table_heap_->End() == iter_) {
//    return false;
//  }
//  *tuple = *iter_;
//  *rid = iter_->GetRid();
  return true;
}

}  // namespace bustub
