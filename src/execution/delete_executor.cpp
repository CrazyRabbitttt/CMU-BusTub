//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  // Remember to initialize the child executor first
  child_executor_->Init();
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  Tuple receive_tuple{};
  RID receive_rid{};

  // the index of the table
  std::string table_name = table_info_->name_;
  auto table_indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_name);

  int cnt = 0;  // the number of the rows to be deleted

  // get the value from the child executor
  while (child_executor_->Next(&receive_tuple, &receive_rid)) {
    // delete the tuple from the table
    if (!table_heap_->MarkDelete(receive_rid, exec_ctx_->GetTransaction())) {
      throw "The tuple to be deleted do not exists";
    }

    // 将数据进行了删除，同时更新一下 index
    for (auto &index_info : table_indexes) {
      const Tuple index_key =
          receive_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(index_key, receive_rid, exec_ctx_->GetTransaction());
    }
    cnt++;
  }

  *tuple = Tuple{{Value{INTEGER, cnt}}, &GetOutputSchema()};
  // 第一次删除，但是数据为0(仍然需要返回 true)
  if (cnt == 0 && !finish_) {
    finish_ = true;
    return true;
  }
  // 只要出现一次 cnt != 0, 直接终止上述的特殊情况
  if (cnt != 0) { finish_ = true; }
  return cnt != 0;
}

}  // namespace bustub
