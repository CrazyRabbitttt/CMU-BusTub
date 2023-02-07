//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // just use the
  index_info_ = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  table_info_ = exec_ctx_->GetCatalog()->GetTable(index_info_->table_name_);
  /** 生成 b plus tree 的迭代器(类型确定为 integer 类型)*/
  tree_ = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  iter_ = (tree_->GetBeginIterator());
}

void IndexScanExecutor::Init() {}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  // 如果说迭代器已经到达了 index 的末尾位置
  if (iter_.EmptyIter()) {
    Schema res_schema{{Column("resname", TypeId::INTEGER)}};
    *tuple = Tuple{{Value{INTEGER, 0}}, &res_schema};
    return false;
  }

  if (iter_.IsEnd()) {
    Schema res_schema{{Column("resname", TypeId::INTEGER)}};
    *tuple = Tuple{{Value{INTEGER, 0}}, &res_schema};
    return false;
  }

  // Get the value of the index(Rid)
  auto index_value = (*iter_).second;
  table_info_->table_->GetTuple(index_value, tuple, exec_ctx_->GetTransaction());
  ++iter_;

  return true;
}

}  // namespace bustub
