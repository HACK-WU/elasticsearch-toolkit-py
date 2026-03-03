# ElasticFlow 集成测试报告格式规范

> 本文档定义测试过程输出和最终报告的格式规范。

---

## 一、测试过程输出规范

### 1.1 核心原则

测试脚本在执行过程中，**每个场景都必须实时输出详细的执行记录**，使观察者无需阅读代码即可完整了解测试行为。输出应包含：做了什么操作、操作的数据量、预期结果是什么、实际结果是什么。

### 1.2 每个场景的输出格式

```
── 场景 X.Y: 场景名称 ──────────────────────────────
  操作: <具体做了什么，调用了哪个 API/方法，传入了什么参数>
  数据: <涉及的数据量、数据特征描述>
  预期: <期望得到什么结果，具体的断言条件>
  实际: <ES 返回的实际结果，关键字段的实际值>
  结果: ✅ PASS (23.45ms)
```

### 1.3 分场景输出示例

#### 写入类操作

```
── 场景 3.1: 批量索引文档 (test_logs 500条) ─────────
  操作: BulkOperationTool.bulk_index(index="test_logs", docs=500条)
        文档结构: {timestamp, level, service, message, response_time, status_code, ip, tags}
        服务分布: api-gateway / user-service / order-service / payment-service / notification-service
        级别分布: DEBUG / INFO / WARN / ERROR
        时间范围: 最近7天
  预期: BulkResult.success == 500, BulkResult.failed == 0
  实际: success=500, failed=0, batch_count=3, took=0.24s
  结果: ✅ PASS (241.05ms)
```

#### 查询类操作

```
── 场景 4.1: 简单条件过滤 (equal) ──────────────────
  操作: DslQueryBuilder.conditions([{key:"level", method:"eq", value:["ERROR"]}])
        执行查询: POST test_logs/_search
        DSL: {"query":{"bool":{"filter":[{"terms":{"level":["ERROR"]}}]}},"size":50}
  预期: 返回的每条文档 _source.level 均为 "ERROR"
  实际: 命中 total=127, 抽检50条全部为 level=ERROR
  结果: ✅ PASS (12.21ms)
```

#### 聚合类操作

```
── 场景 4.9: 聚合查询 (terms/stats/cardinality/pct) ─
  操作: DslQueryBuilder 添加4种聚合:
        - by_level: terms(field=level, size=10)
        - rt_stats: stats(field=response_time)
        - svc_count: cardinality(field=service)
        - rt_pct: percentiles(field=response_time)
  预期: aggregations 中包含 by_level / rt_stats / svc_count / rt_pct 四个键,
        by_level 至少有1个桶, rt_stats 各指标不为 None
  实际: by_level=4个桶[DEBUG:131,INFO:128,WARN:122,ERROR:119],
        rt_stats={count:500, avg:2487.3, min:10.5, max:4998.7},
        svc_count=5, rt_pct={1.0:52.3, 5.0:260.1, ...}
  结果: ✅ PASS (22.83ms)
```

#### 失败/错误场景

```
── 场景 3.1: 批量索引文档 (test_logs 500条) ─────────
  操作: BulkOperationTool.bulk_index(index="test_logs", docs=500条)
  预期: BulkResult.success == 500, BulkResult.failed == 0
  实际: TypeError: cannot unpack non-iterable int object
        traceback: bulk/tool.py:L87 — for ok, info in bulk(...)
  结果: ❌ FAIL (5.12ms)
  ──────────────────────────────────────────────────
  🐛 BUG 检测到! 停止后续所有测试。
     模块: 批量操作
     原因: elasticsearch-py 8.x 的 bulk() 返回 (int, list) 而非迭代器
     文件: src/elasticflow/bulk/tool.py:L87
```

### 1.4 模块级输出

每个模块开始和结束时输出模块摘要：

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
[模块 3] BulkOperationTool — 批量操作 (6个场景)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  (各场景详细输出...)

[模块 3 小结] 6/6 通过 | 总耗时: 681.14ms
  ✅ 3.1 批量索引文档 (500条→success=500)      241.05ms
  ✅ 3.2 批量创建文档 (50条→success=50)         120.95ms
  ✅ 3.3 批量更新文档 (5条→验证字段变更)         94.56ms
  ✅ 3.4 批量删除文档 (3条→验证已删除)           58.82ms
  ✅ 3.5 批量 UPSERT (2条×2次→create+update)   102.71ms
  ✅ 3.6 流式批量操作 (100条→callback触发)       63.05ms
```

---

## 二、测试报告规范

### 2.1 报告输出方式

测试报告在所有场景执行完毕后输出，同时**保存为独立文件** `integration_test_report.txt`，确保完整可查阅，不受终端缓冲区截断影响。

### 2.2 报告完整结构

```
╔══════════════════════════════════════════════════════════════════════╗
║                    ElasticFlow 集成测试报告                          ║
╚══════════════════════════════════════════════════════════════════════╝

一、测试环境
───────────────────────────────────────────────────────────────────────
  ES 集群       : http://21.91.146.126:9200 (v7.17.10, 3 nodes, green)
  elasticflow   : 0.4.0
  Python        : 3.11.x
  elasticsearch : 8.19.3
  测试时间      : YYYY-MM-DD HH:MM:SS
  总耗时        : xx.xxs

二、总览
───────────────────────────────────────────────────────────────────────
  总场景数: 56
  ✅ 通过: 56 (100.0%)
  ❌ 失败: 0  (0.0%)
  💥 错误: 0  (0.0%)
  ⏭️  跳过: 0  (0.0%)

三、各模块详细结果
───────────────────────────────────────────────────────────────────────

[模块 1] ESClientFactory — 连接管理 (5/5 通过)
┌──────┬──────────────────────────┬────────┬──────────┬────────────────────────────────┐
│ 编号 │ 场景                     │ 结果   │ 耗时     │ 详情                           │
├──────┼──────────────────────────┼────────┼──────────┼────────────────────────────────┤
│ 1.1  │ 正确配置创建客户端        │ ✅ PASS │ 11.21ms │ cluster=es-cluster, ver=7.17.10│
│ 1.2  │ 读写分离回退              │ ✅ PASS │ 15.01ms │ read/write均回退到MASTER        │
│ 1.3  │ 集群健康检查              │ ✅ PASS │ 16.97ms │ status=green, healthy=True      │
│ 1.4  │ 多客户端缓存              │ ✅ PASS │  0.65ms │ c1 is c2 = True                │
│ 1.5  │ 上下文管理器              │ ✅ PASS │ 11.77ms │ with 块正常退出                 │
└──────┴──────────────────────────┴────────┴──────────┴────────────────────────────────┘

  (... 其余模块同上格式 ...)

[模块 3] BulkOperationTool — 批量操作 (6/6 通过)
┌──────┬──────────────────────────────────────┬────────┬──────────┬──────────────────────────────────────────────────┐
│ 编号 │ 场景                                 │ 结果   │ 耗时     │ 详情                                             │
├──────┼──────────────────────────────────────┼────────┼──────────┼──────────────────────────────────────────────────┤
│ 3.1  │ 批量索引文档 (test_logs 500条)        │ ✅ PASS │ 241.05ms│ 写入500条, success=500, failed=0, batches=3      │
│ 3.2  │ 批量创建文档 (test_geo 50条)          │ ✅ PASS │ 120.95ms│ 写入50条, success=50, failed=0                   │
│ 3.3  │ 批量更新文档                          │ ✅ PASS │  94.56ms│ 更新5条, 验证level字段已变更为UPDATED              │
│ 3.4  │ 批量删除文档                          │ ✅ PASS │  58.82ms│ 删除3条UPDATED文档, 验证已不可查                  │
│ 3.5  │ 批量 UPSERT                          │ ✅ PASS │ 102.71ms│ 首次: created=2; 二次: updated=2                 │
│ 3.6  │ 流式批量操作 (test_nested 100条)       │ ✅ PASS │  63.05ms│ 写入100条嵌套订单, callbacks=1次                  │
└──────┴──────────────────────────────────────┴────────┴──────────┴──────────────────────────────────────────────────┘

四、失败/错误场景详情（如有）
───────────────────────────────────────────────────────────────────────
  (无)

  或:

  ❌ 3.1 批量索引文档 (test_logs 500条)
     模块     : 批量操作
     耗时     : 5.12ms
     操作     : BulkOperationTool.bulk_index(index="test_logs", docs=500)
     预期     : BulkResult.success == 500
     实际     : TypeError: cannot unpack non-iterable int object
     错误位置 : src/elasticflow/bulk/tool.py:L87
     堆栈     :
       File "src/elasticflow/bulk/tool.py", line 87, in execute
         for ok, info in bulk(self.es_client, actions, ...)
       TypeError: cannot unpack non-iterable int object
     判定     : 🐛 代码BUG — elasticsearch-py 8.x 兼容性问题

五、性能统计
───────────────────────────────────────────────────────────────────────
  总耗时        : 1.84s
  平均请求耗时  : 30.5ms
  最快请求      : 7.6 parse_percentiles_agg — 0.01ms
  最慢请求      : 2.1 创建索引（含 Mapping） — 268.73ms
  P50           : 12.2ms
  P90           : 81.9ms
  P99           : 268.7ms

  耗时分布:
    < 1ms       : ██████████ 8 个场景
    1ms - 10ms  : ████████ 6 个场景
    10ms - 50ms : ████████████████████████ 28 个场景
    50ms - 100ms: ████████████ 8 个场景
    > 100ms     : ██████████ 6 个场景

六、Bug 记录
───────────────────────────────────────────────────────────────────────
  (无)

七、集群异常记录
───────────────────────────────────────────────────────────────────────
  (无)

八、测试数据清理
───────────────────────────────────────────────────────────────────────
  ✅ 删除索引: test_logs
  ✅ 删除索引: test_geo_locations
  ✅ 删除索引: test_nested_orders
  ✅ 删除模板: test_template_logs
  ✅ 删除 ILM: test_policy_logs
  清理状态: 全部完成
```

### 2.3 报告文件输出

测试脚本执行完毕后，除终端输出外，必须将完整报告写入文件 `integration_test_report.txt`（与测试脚本同目录），内容与终端输出一致，确保：
- 不受终端缓冲区限制，完整保留所有场景记录
- 可作为归档文件保存或传阅
- 文件编码 UTF-8
