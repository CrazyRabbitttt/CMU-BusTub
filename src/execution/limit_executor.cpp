//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      limit_size_(plan_->GetLimit()) {}

void LimitExecutor::Init() { child_executor_->Init(); }

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  Tuple receive_tuple;
  RID receive_rid;
  if (!child_executor_->Next(&receive_tuple, &receive_rid) || cur_size_ == limit_size_) {
    return false;
  }

  *tuple = receive_tuple;
  *rid = receive_rid;
  cur_size_++;
  return true;
}

}  // namespace bustub
