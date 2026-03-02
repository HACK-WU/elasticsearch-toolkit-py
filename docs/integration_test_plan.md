# ElasticFlow 集成测试方案

## 一、概述

本文档描述了 ElasticFlow 项目的集成测试方案。通过向真实 Elasticsearch 集群发送 HTTP 请求，验证所有功能模块在真实环境下的正确性与可靠性。

## 二、测试环境

| 项目 | 值 |
|---|---|
| ES 地址 | `http://21.91.146.126:9200` |
| ES 版本 | 7.17.10 (Docker) |
| 集群名称 | es-cluster |
| 集群状态 | green |
| 节点数量 | 3 (es01, es02, es03) |
| 认证方式 | Basic Auth (`elastic / kORpAZR8e3UKDD4r4dKe`) |
| Python | >= 3.11 |
| elasticflow | 0.4.0 |
| 核心依赖 | elasticsearch-dsl>=7,<9 / luqum>=0.11 |

### 2.1 版本兼容性

ES 7.17.10 完全兼容 `elasticsearch-dsl>=7,<9`，ILM、Profile API、geo_point 等功能均默认可用。无额外配置要求。

## 三、测试策略

### 3.1 Bug 阻断机制

**核心原则：发现 Bug 立即停止，修复后才可继续。**

- 每个测试场景执行后，立即校验结果。
- 若某个测试场景失败且判定为代码 Bug（非预期的数据/环境问题），则：
  1. 记录 Bug 详情（模块、场景、错误信息、期望值 vs 实际值）。
  2. **停止后续所有测试**。
  3. 定位并修复 Bug。
  4. **从失败的场景重新开始**继续执行后续测试。
- 若失败原因为测试数据或环境问题（如数据未灌入），归类为测试缺陷而非代码 Bug，修正测试后重试该场景。

### 3.2 ES 集群异常检测机制

**核心原则：集群异常时停止测试，恢复后才可继续。**

- 每个测试模块执行前，检查集群健康状态（`_cluster/health`）。
- 若集群状态为 `red`，或节点数异常，或请求超时/连接拒绝：
  1. 记录异常详情（时间、状态、错误信息）。
  2. **停止后续所有测试**。
  3. 等待集群恢复正常（轮询检查，间隔 10 秒，最多等待 5 分钟）。
  4. 集群恢复后，从中断处继续测试。
  5. 若 5 分钟内未恢复，终止测试并在报告中标注。

### 3.3 执行顺序

模块间存在依赖关系，按以下顺序执行：

```
连接管理 → 索引管理 → 批量操作(灌数据) → DSL查询 → QueryString → 查询转换 → 响应解析 → 时间范围 → 地理查询 → 查询分析
```

## 四、测试数据

### 4.1 索引 `test_logs` — 日志数据（主测试索引）

**Mapping:**

```json
{
  "mappings": {
    "properties": {
      "timestamp":     { "type": "date" },
      "level":         { "type": "keyword" },
      "service":       { "type": "keyword" },
      "message":       { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "response_time": { "type": "float" },
      "status_code":   { "type": "integer" },
      "ip":            { "type": "ip" },
      "tags":          { "type": "keyword" }
    }
  }
}
```

- 数据量：~500 条
- 5 个服务（api-gateway, user-service, order-service, payment-service, notification-service）
- 4 个日志级别（DEBUG, INFO, WARN, ERROR）
- 时间范围：最近 7 天
- response_time：10~5000ms 随机分布
- status_code：200/201/400/404/500

### 4.2 索引 `test_geo_locations` — 地理位置数据

**Mapping:**

```json
{
  "mappings": {
    "properties": {
      "name":        { "type": "keyword" },
      "category":    { "type": "keyword" },
      "location":    { "type": "geo_point" },
      "rating":      { "type": "float" },
      "description": { "type": "text" }
    }
  }
}
```

- 数据量：~50 条
- 模拟北京市区内 POI（餐饮、酒店、景点、商场）
- 坐标范围：纬度 39.8~40.0，经度 116.2~116.5

### 4.3 索引 `test_nested_orders` — 嵌套文档数据

**Mapping:**

```json
{
  "mappings": {
    "properties": {
      "order_id":   { "type": "keyword" },
      "customer":   { "type": "keyword" },
      "total":      { "type": "float" },
      "status":     { "type": "keyword" },
      "created_at": { "type": "date" },
      "items": {
        "type": "nested",
        "properties": {
          "product":  { "type": "keyword" },
          "price":    { "type": "float" },
          "quantity": { "type": "integer" }
        }
      }
    }
  }
}
```

- 数据量：~100 条订单
- 每笔订单含 1~5 个嵌套商品项
- 状态：pending / confirmed / shipped / delivered / cancelled

## 五、测试场景

### 模块 1：ESClientFactory — 连接管理（5 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 1.1 | 正确配置创建客户端 | `get_client()` 返回可用客户端，能执行 `info()` |
| 1.2 | 读写分离回退 | `get_read_client()` / `get_write_client()` 回退到 MASTER |
| 1.3 | 集群健康检查 | `health_check()` 返回 green/yellow，`is_healthy()` 为 True |
| 1.4 | 多客户端缓存 | 多次 `get_client()` 返回同一实例（`is` 判断） |
| 1.5 | 上下文管理器 | `with ESClientFactory(...)` 退出后连接关闭 |

### 模块 2：IndexManager — 索引管理（8 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 2.1 | 创建索引（含 Mapping） | 索引成功创建，mapping 与定义一致 |
| 2.2 | 检查索引是否存在 | 已创建索引返回 True，不存在索引返回 False |
| 2.3 | 获取索引信息 | 返回完整 mapping + settings |
| 2.4 | 创建/查询/删除别名 | 别名 CRUD 完整链路 |
| 2.5 | 创建/获取索引模板 | 模板 CRUD |
| 2.6 | ILM 策略操作 | 创建/查询 ILM 策略 |
| 2.7 | 删除索引 | 索引成功删除 |
| 2.8 | 重复创建索引异常 | 捕获 `IndexAlreadyExistsError` |

### 模块 3：BulkOperationTool — 批量操作（6 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 3.1 | 批量索引文档（test_logs） | BulkResult.success == 文档数 |
| 3.2 | 批量创建文档（test_geo_locations） | 文档正确写入 |
| 3.3 | 批量更新文档 | 指定字段值变更成功 |
| 3.4 | 批量删除文档 | 文档被删除，查询不到 |
| 3.5 | 批量 UPSERT | 新文档插入 + 旧文档更新 |
| 3.6 | 流式批量操作（test_nested_orders） | `bulk_stream()` 配合 progress_callback |

### 模块 4：DslQueryBuilder — DSL 查询构建与执行（10 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 4.1 | 简单条件过滤 (equal) | `level == "ERROR"` 结果全部为 ERROR |
| 4.2 | 范围条件 (gt/lt) | `response_time > 1000` 结果均满足 |
| 4.3 | 包含/排除 (include/exclude) | 多值匹配/排除 |
| 4.4 | 存在/不存在 (exists/nexists) | 字段存在性 |
| 4.5 | ConditionGroup (AND/OR) | 嵌套逻辑组合 |
| 4.6 | NestedCondition | 嵌套文档条件 |
| 4.7 | QueryString 查询 | `level:ERROR AND service:api-gateway` |
| 4.8 | 排序 + 分页 | 降序排列 + 分页参数正确 |
| 4.9 | 聚合查询 | terms/stats/cardinality/percentiles/top_hits |
| 4.10 | 组合查询 | 条件+QueryString+排序+分页+聚合 |

### 模块 5：QueryStringBuilder + Q 对象（4 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 5.1 | QueryStringBuilder 基本构建 | 各操作符生成正确 QueryString |
| 5.2 | 特殊字符转义 | 含 `+ - : /` 的值正确转义 |
| 5.3 | Q 对象组合 | `&` `|` `~` 运算符 |
| 5.4 | Q 对象执行验证 | 构建的查询在 ES 上正确执行 |

### 模块 6：QueryStringTransformer — 查询转换（3 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 6.1 | 字段名映射 | 中文字段名 → ES 字段名 |
| 6.2 | 值翻译 | 枚举值中英文映射 |
| 6.3 | 转换后执行 | 转换后的 QueryString 在 ES 可执行 |

### 模块 7：ResponseParser — 响应解析（8 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 7.1 | parse_hits | 文档列表正确 |
| 7.2 | parse_paged | PagedResponse 结构完整 |
| 7.3 | parse_terms_agg | TermsBucket 列表 |
| 7.4 | parse_stats_agg | StatsResult 数值 |
| 7.5 | parse_cardinality_agg | 去重计数 |
| 7.6 | parse_percentiles_agg | 百分位值 |
| 7.7 | parse_top_hits_agg | Top Hits 子聚合 |
| 7.8 | 元数据方法 | get_total/get_took/get_shards_info |

### 模块 8：TimeRangeQueryTool — 时间范围（4 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 8.1 | 快速时间范围 | `quick_range(LAST_7_DAYS)` 执行正确 |
| 8.2 | 相对时间范围 | `relative_range(30, "m")` |
| 8.3 | 绝对时间范围 | 指定起止时间 |
| 8.4 | 时间字符串解析 | `now-1h` / ISO 8601 / 时间戳 |

### 模块 9：GeoQueryTool — 地理查询（5 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 9.1 | 距离查询 | 北京天安门附近 5km |
| 9.2 | 边界框查询 | 矩形范围内 POI |
| 9.3 | 多边形查询 | 多边形范围内 POI |
| 9.4 | 距离排序 | 按距离升序 |
| 9.5 | 地理聚合 | 距离聚合 / 边界聚合 |

### 模块 10：QueryAnalyzer — 查询分析（4 个场景）

| # | 场景 | 验证点 |
|---|------|--------|
| 10.1 | 简单查询分析 | 返回 QueryAnalysis 结构 |
| 10.2 | 带 Profile 分析 | profile=True 返回分片性能数据 |
| 10.3 | 前导通配符检测 | 触发 LeadingWildcardRule 告警 |
| 10.4 | 复杂查询复杂度评分 | complexity_score 合理 |

## 六、测试过程输出规范

### 6.1 核心原则

测试脚本在执行过程中，**每个场景都必须实时输出详细的执行记录**，使观察者无需阅读代码即可完整了解测试行为。输出应包含：做了什么操作、操作的数据量、预期结果是什么、实际结果是什么。

### 6.2 每个场景的输出格式

每个测试场景执行时，必须按以下结构输出详细信息：

```
── 场景 X.Y: 场景名称 ──────────────────────────────
  操作: <具体做了什么，调用了哪个 API/方法，传入了什么参数>
  数据: <涉及的数据量、数据特征描述>
  预期: <期望得到什么结果，具体的断言条件>
  实际: <ES 返回的实际结果，关键字段的实际值>
  结果: ✅ PASS (23.45ms)
```

### 6.3 分场景输出示例

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

### 6.4 模块级输出

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

## 七、测试报告规范

### 7.1 报告输出方式

测试报告在所有场景执行完毕后输出，同时**保存为独立文件** `integration_test_report.txt`，确保完整可查阅，不受终端缓冲区截断影响。

### 7.2 报告完整结构

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

### 7.3 报告文件输出

测试脚本执行完毕后，除终端输出外，必须将完整报告写入文件 `integration_test_report.txt`（与测试脚本同目录），内容与终端输出一致，确保：
- 不受终端缓冲区限制，完整保留所有场景记录
- 可作为归档文件保存或传阅
- 文件编码 UTF-8

## 八、数据保留与清理策略

### 8.1 核心原则：默认保留，按需清理

测试产生的索引和数据**默认保留**，不在每次测试结束后自动删除。原因：
- 保留数据便于测试后人工检查、排查问题
- 避免重复灌入数据浪费时间，支持增量测试
- 便于在同一份数据上反复调试查询类测试

### 8.2 数据复用机制

测试脚本启动时，先检查测试索引是否已存在且包含数据：
- **索引已存在且数据量符合预期**：跳过数据灌入，直接进入查询类测试
- **索引不存在**：正常创建索引并灌入数据
- **索引存在但数据量不符**：删除后重建

```
启动检查:
  test_logs         — 存在 (497条) ✅ 复用现有数据，跳过灌入
  test_geo_locations — 存在 (50条)  ✅ 复用现有数据，跳过灌入
  test_nested_orders — 不存在       → 创建索引并灌入100条
```

### 8.3 仅清理临时/副作用数据

以下数据在测试过程中产生且可能影响后续运行，测试结束后清理：
- 测试别名：`test_logs_alias` 等
- 测试索引模板：`test_template_*`
- 测试 ILM 策略：`test_policy_*`
- 批量更新/删除产生的脏数据（如 level 被改为 `UPDATED` 的文档）

### 8.4 强制清理模式

提供命令行参数 `--clean` 用于显式清理所有测试数据：

```bash
# 正常测试（保留数据）
python integration_test.py

# 强制清理后重新测试
python integration_test.py --clean
```

`--clean` 模式下，测试开始前删除所有测试索引/模板/策略，从零开始。
