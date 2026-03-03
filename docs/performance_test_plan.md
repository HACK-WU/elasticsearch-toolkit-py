# ElasticFlow 性能与大数据测试方案

> 本文档定义性能基准测试和大数据场景测试方案，用于评估代码在大规模数据下的运行效率与质量。

---

## 一、测试环境配置

### 1.1 集群配置建议

| 规模 | 节点数 | 单节点配置 | 适用场景 |
|------|--------|------------|----------|
| 小型 | 3 | 4C8G | 功能测试、开发调试 |
| 中型 | 3 | 8C16G | 性能基准测试 |
| 大型 | 5+ | 16C32G | 压力测试、极限测试 |

### 1.2 数据规模定义

| 规模 | 文档数 | 单文档大小 | 总数据量 |
|------|--------|------------|----------|
| 小 | 1万 | ~1KB | ~10MB |
| 中 | 10万 | ~1KB | ~100MB |
| 大 | 100万 | ~1KB | ~1GB |
| 超大 | 1000万 | ~1KB | ~10GB |

---

## 二、性能基准测试

### 2.1 批量写入性能

| # | 场景 | 数据规模 | 基准指标 | 优先级 |
|---|------|----------|----------|--------|
| P.1 | 批量写入吞吐 | 1万条 | TPS > 1000/s | P0 |
| P.2 | 批量写入吞吐 | 10万条 | TPS > 500/s | P0 |
| P.3 | 批量写入吞吐 | 100万条 | TPS > 200/s | P1 |
| P.4 | 流式写入内存 | 10万条 | 峰值内存 < 500MB | P0 |
| P.5 | 流式写入内存 | 100万条 | 峰值内存 < 2GB | P1 |

**测试方法：**

```python
def test_bulk_write_throughput(data_size: int):
    """
    批量写入吞吐测试
    1. 生成 data_size 条测试文档
    2. 记录开始时间
    3. 执行 bulk_index
    4. 记录结束时间
    5. 计算 TPS = data_size / elapsed_seconds
    6. 验证数据完整性（抽样检查）
    """
    docs = generate_test_docs(data_size)
    start = time.time()
    result = bulk_tool.bulk_index(index_name, docs)
    elapsed = time.time() - start
    
    tps = data_size / elapsed
    assert tps >= threshold, f"TPS {tps:.0f} 低于阈值 {threshold}"
    assert result.success == data_size
```

### 2.2 查询延迟测试

| # | 场景 | 查询复杂度 | 基准指标 | 优先级 |
|---|------|------------|----------|--------|
| P.6 | 简单查询 | term 过滤 | P99 < 100ms | P0 |
| P.7 | 范围查询 | range 过滤 | P99 < 150ms | P0 |
| P.8 | 聚合查询 | 1个聚合 | P99 < 200ms | P0 |
| P.9 | 复杂聚合 | 5个聚合 | P99 < 500ms | P1 |
| P.10 | 深度分页 | search_after | P99 < 200ms | P1 |

**测试方法：**

```python
def test_query_latency(query_fn, iterations: int = 100):
    """
    查询延迟测试
    1. 预热：执行 10 次查询，不记录
    2. 正式测试：执行 iterations 次查询
    3. 记录每次延迟
    4. 计算 P50/P95/P99
    5. 验证延迟分布符合预期
    """
    # 预热
    for _ in range(10):
        query_fn()
    
    # 正式测试
    latencies = []
    for _ in range(iterations):
        start = time.time()
        query_fn()
        latencies.append((time.time() - start) * 1000)
    
    p50 = percentile(latencies, 50)
    p95 = percentile(latencies, 95)
    p99 = percentile(latencies, 99)
    
    return {"p50": p50, "p95": p95, "p99": p99}
```

### 2.3 内存占用测试

| # | 场景 | 数据规模 | 基准指标 | 优先级 |
|---|------|----------|----------|--------|
| P.11 | 批量写入内存 | 10万条 | 峰值 < 500MB | P0 |
| P.12 | 响应解析内存 | 1万条结果 | 峰值 < 100MB | P0 |
| P.13 | 大聚合结果 | 1000桶 | 峰值 < 50MB | P1 |

**测试方法：**

```python
import tracemalloc

def test_memory_usage(operation_fn):
    """
    内存占用测试
    1. 启动 tracemalloc
    2. 执行操作
    3. 记录峰值内存
    4. 验证内存已释放
    """
    tracemalloc.start()
    operation_fn()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    
    return {"peak_mb": peak / 1024 / 1024}
```

---

## 三、大数据场景测试

### 3.1 深度分页

| # | 场景 | 数据规模 | 验证点 | 优先级 |
|---|------|----------|--------|--------|
| L.1 | 超过10000条分页 | 10万条 | from+size 限制，推荐 search_after | P0 |
| L.2 | PIT + search_after | 100万条 | 正确遍历所有数据 | P1 |
| L.3 | 滚动查询 (scroll) | 100万条 | 正确遍历所有数据 | P1 |

**测试代码示例：**

```python
def test_deep_pagination():
    """测试深度分页限制"""
    # from + size 超过 10000 应失败或使用 search_after
    with pytest.raises(RequestError):
        search.from_(10000).size(10).execute()
    
    # search_after 方案
    search = Search().sort("_id")
    all_docs = []
    pit = client.open_point_in_time(index="test_logs", keep_alive="1m")
    
    while True:
        response = search.execute()
        all_docs.extend(response.hits)
        if not response.hits:
            break
        search = search.extra(pit={"id": pit.id})
        search = search.search_after([response.hits[-1].meta.id])
    
    assert len(all_docs) == 100000
```

### 3.2 大聚合结果

| # | 场景 | 数据规模 | 验证点 | 优先级 |
|---|------|----------|--------|--------|
| L.4 | terms 聚合返回 1000 桶 | 10万条 | 正确解析所有桶 | P0 |
| L.5 | terms 聚合返回 10000 桶 | 100万条 | 正确解析，内存可控 | P1 |
| L.6 | 嵌套聚合 (3层) | 10万条 | 正确解析嵌套结构 | P1 |

### 3.3 大文档处理

| # | 场景 | 文档大小 | 验证点 | 优先级 |
|---|------|----------|--------|--------|
| L.7 | 大文档写入 | 1MB/条 | 正确写入，无截断 | P1 |
| L.8 | 大文档写入 | 10MB/条 | ES 限制检查（默认 100MB） | P2 |
| L.9 | 大文档查询 | 1MB/条 | 正确返回完整文档 | P1 |

---

## 四、压力测试

### 4.1 并发写入

| # | 场景 | 并发数 | 数据规模 | 验证点 | 优先级 |
|---|------|--------|----------|--------|--------|
| S.1 | 并发 bulk 写入 | 5线程 | 每线程1万条 | 无数据丢失 | P1 |
| S.2 | 并发 bulk 写入 | 10线程 | 每线程1万条 | 连接池安全 | P2 |
| S.3 | 并发索引创建 | 5线程 | 各创建1个索引 | 无竞态条件 | P2 |

**测试代码示例：**

```python
import concurrent.futures

def test_concurrent_bulk_write(threads: int, docs_per_thread: int):
    """并发写入测试"""
    def bulk_task(thread_id: int):
        docs = generate_test_docs(docs_per_thread)
        return bulk_tool.bulk_index(f"test_concurrent_{thread_id}", docs)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        futures = [executor.submit(bulk_task, i) for i in range(threads)]
        results = [f.result() for f in futures]
    
    total_success = sum(r.success for r in results)
    expected = threads * docs_per_thread
    assert total_success == expected
```

### 4.2 高并发查询

| # | 场景 | 并发数 | 持续时间 | 验证点 | 优先级 |
|---|------|--------|----------|--------|--------|
| S.4 | 并发查询 | 10线程 | 1分钟 | 无异常，延迟稳定 | P1 |
| S.5 | 并发查询 | 50线程 | 5分钟 | 连接池不耗尽 | P2 |

---

## 五、性能测试报告格式

```
╔══════════════════════════════════════════════════════════════════════╗
║                    ElasticFlow 性能测试报告                          ║
╚══════════════════════════════════════════════════════════════════════╝

一、测试环境
───────────────────────────────────────────────────────────────────────
  ES 集群       : 3节点 × 8C16G
  数据规模      : 100万条 × 1KB
  测试时间      : 2026-03-02 15:30:00

二、批量写入性能
───────────────────────────────────────────────────────────────────────
  数据规模      10万条        100万条       基准值
  ──────────────────────────────────────────────────
  TPS          850/s         320/s         >500/s, >200/s
  总耗时       117.6s        52.1min       -
  成功率       100%          100%          100%
  内存峰值     380MB         1.2GB         <500MB, <2GB

三、查询延迟分布
───────────────────────────────────────────────────────────────────────
  查询类型      P50          P95          P99          基准值
  ──────────────────────────────────────────────────
  简单过滤      12ms         45ms         89ms         <100ms
  范围查询      18ms         62ms         125ms        <150ms
  单聚合        25ms         78ms         165ms        <200ms
  5聚合组合     89ms         245ms        420ms        <500ms

四、并发测试结果
───────────────────────────────────────────────────────────────────────
  场景                并发数   成功率   平均延迟   P99延迟
  ──────────────────────────────────────────────────
  并发写入(5线程)      5       100%     234ms      892ms
  并发查询(10线程)     10      100%     45ms       187ms

五、结论
───────────────────────────────────────────────────────────────────────
  ✅ 批量写入性能：达标
  ✅ 查询延迟：达标
  ⚠️  内存占用：100万条写入时峰值偏高，建议优化分批策略
```

---

## 六、基准指标汇总

| 操作 | 数据规模 | 指标 | 建议阈值 |
|------|----------|------|----------|
| bulk_index | 1万条 | TPS | > 1000/s |
| bulk_index | 10万条 | TPS | > 500/s |
| bulk_index | 100万条 | TPS | > 200/s |
| 简单查询 | - | P99 | < 100ms |
| 范围查询 | - | P99 | < 150ms |
| 单聚合 | - | P99 | < 200ms |
| 复杂聚合 | 5个 | P99 | < 500ms |
| search_after | 100万条 | P99 | < 200ms |
| 内存峰值 | 10万条写入 | Memory | < 500MB |
| 内存峰值 | 100万条写入 | Memory | < 2GB |
