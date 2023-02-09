//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->table_.get();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple receive_tuple{};
  RID receive_rid{};

  // get the index of the table
  std::string table_name = table_info_->name_;
  std::vector<IndexInfo *> table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);
  int cnt = 0;
  // get the value from the child executor
  while (child_executor_->Next(&receive_tuple, &receive_rid)) {
    // Insert the tuple into the table heap, 本身就能够保证 col 的类型是对应的
    if (!table_heap_->InsertTuple(receive_tuple, &receive_rid, exec_ctx_->GetTransaction())) {
      throw "The insertion into the tuple failed";
    }
    // 插入到了 table heap 中了，下面需要对于索引进行更新
    for (auto &index_info : table_indexes) {
      // 生成一个能够对应的 tuple 来进行插入(类型要匹配) : TODO(sgx) : 比如说要生成一个指定了 col 类型啊 个数啊 什么的
      // tuple 来对应的进行插入
      const Tuple index_key =
          receive_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(index_key, receive_rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }

  *tuple = Tuple{{Value{INTEGER, cnt}}, &GetOutputSchema()};
  // 第一次的情况 还是返回 true， 让上层拿到结果， 之后的返回 false ：停止上层的循环
  if (cnt == 0 && !finish_) {
    finish_ = true;
    return true;
  }
  if (cnt != 0) {
    finish_ = true;
  }
  return cnt != 0;
}

}  // namespace bustub
