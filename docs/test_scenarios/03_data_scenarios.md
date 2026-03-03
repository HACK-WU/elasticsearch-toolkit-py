# 模块 3+7：批量操作 + 响应解析 测试场景

## 模块 3：BulkOperationTool — 批量操作

### 3.1 批量写入场景（3个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 3.1 | 批量索引文档（test_logs 500条） | BulkResult.success == 500, failed == 0 | P0 |
| 3.2 | 批量创建文档（test_geo 50条） | 指定 doc_id 创建，无重复 | P0 |
| 3.3 | 流式批量操作（test_nested 100条） | `bulk_stream()` + progress_callback | P1 |

### 3.2 批量更新场景（3个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 3.4 | 批量更新文档 | 指定字段值变更成功 | P0 |
| 3.5 | 批量删除文档 | 文档被删除，查询不到 | P0 |
| 3.6 | 批量 UPSERT | 新文档插入 + 旧文档更新 | P0 |

### 3.3 输出示例

```
── 场景 3.1: 批量索引文档 (test_logs 500条) ─────────
  操作: BulkOperationTool.bulk_index(index="test_logs", docs=500条)
        文档结构: {timestamp, level, service, message, response_time, 
                   status_code, ip, tags}
        服务分布: api-gateway / user-service / order-service / 
                  payment-service / notification-service
        级别分布: DEBUG / INFO / WARN / ERROR
        时间范围: 最近7天
  预期: BulkResult.success == 500, BulkResult.failed == 0
  实际: success=500, failed=0, batch_count=3, took=0.24s
  结果: ✅ PASS (241.05ms)

── 场景 3.6: 流式批量操作 (test_nested 100条) ────────
  操作: BulkOperationTool.bulk_stream(
          operations=[BulkOperation(action=INDEX, ...) for _ in 100],
          progress_callback=on_progress
        )
  预期: success=100, progress_callback 被调用至少1次
  实际: success=100, callbacks=1, batches=1
  结果: ✅ PASS (63.05ms)
```

---

## 模块 7：ResponseParser — 响应解析

### 7.1 文档解析场景（2个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 7.1 | parse_hits | 文档列表正确，字段完整 | P0 |
| 7.2 | parse_paged | PagedResponse 结构完整（total/page/page_size/has_next） | P0 |

### 7.2 聚合解析场景（4个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 7.3 | parse_terms_agg | TermsBucket 列表，含 key/doc_count | P0 |
| 7.4 | parse_stats_agg | StatsResult 含 count/min/max/avg/sum | P0 |
| 7.5 | parse_cardinality_agg | CardinalityResult 含 value | P0 |
| 7.6 | parse_percentiles_agg | PercentilesResult 含 values 字典 | P0 |

### 7.3 高级解析场景（2个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 7.7 | parse_top_hits_agg (子聚合) | 从父聚合桶中提取 top_hits | P1 |
| 7.8 | 元数据方法 | get_total/get_took/get_shards_info/is_timed_out | P0 |

### 7.4 输出示例

```
── 场景 7.2: parse_paged ─────────────────────────────
  操作: ResponseParser.parse_paged(response, page=1, page_size=20)
  预期: PagedResponse(total>0, page=1, page_size=20, has_next=True/False)
  实际: total=500, page=1, page_size=20, total_pages=25, has_next=True
  结果: ✅ PASS (0.06ms)

── 场景 7.4: parse_stats_agg ─────────────────────────
  操作: ResponseParser.parse_stats_agg(response, "rt_stats")
  预期: StatsResult 各字段不为 None
  实际: count=500, avg=2487.3, min=10.5, max=4998.7, sum=1243650.0
  结果: ✅ PASS (0.02ms)
```

### 7.5 大数据量解析（可选）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 7.9 | 解析10000+文档 | 内存占用合理，耗时<5s | P1 |
| 7.10 | 解析大型聚合结果 | 1000+桶的正确解析 | P2 |
