# ElasticFlow 集成测试方案

## 一、概述

本文档描述了 ElasticFlow 项目的集成测试方案。通过向真实 Elasticsearch 集群发送 HTTP 请求，验证所有功能模块在真实环境下的正确性与可靠性。

**测试方案拆分为多个文件，便于维护和扩展：**

| 文件 | 内容 |
|------|------|
| [integration_test_plan.md](./integration_test_plan.md) | 总体方案（环境、策略、数据定义） |
| [test_scenarios/01_connection_scenarios.md](./test_scenarios/01_connection_scenarios.md) | 模块1-2：连接管理 + 索引管理 |
| [test_scenarios/02_query_scenarios.md](./test_scenarios/02_query_scenarios.md) | 模块4-6：DSL/QueryString/转换 |
| [test_scenarios/03_data_scenarios.md](./test_scenarios/03_data_scenarios.md) | 模块3+7：批量操作 + 响应解析 |
| [test_scenarios/04_advanced_scenarios.md](./test_scenarios/04_advanced_scenarios.md) | 模块8-10：时间/地理/分析 |
| [test_scenarios/05_edge_cases.md](./test_scenarios/05_edge_cases.md) | 边界条件 + 异常处理 |
| [performance_test_plan.md](./performance_test_plan.md) | 性能/大数据测试方案 |
| [integration_test_report_format.md](./integration_test_report_format.md) | 报告格式规范 |

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

### 4.4 索引 `test_large_dataset` — 大数据量测试索引

**Mapping:** 同 `test_logs`

- 数据量：10万 / 100万 / 1000万条（按测试阶段递增）
- 用于性能测试和大数据场景验证

## 五、数据保留与清理策略

### 5.1 核心原则：默认保留，按需清理

测试产生的索引和数据**默认保留**，不在每次测试结束后自动删除。原因：
- 保留数据便于测试后人工检查、排查问题
- 避免重复灌入数据浪费时间，支持增量测试
- 便于在同一份数据上反复调试查询类测试

### 5.2 数据复用机制

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

### 5.3 仅清理临时/副作用数据

以下数据在测试过程中产生且可能影响后续运行，测试结束后清理：
- 测试别名：`test_logs_alias` 等
- 测试索引模板：`test_template_*`
- 测试 ILM 策略：`test_policy_*`
- 批量更新/删除产生的脏数据（如 level 被改为 `UPDATED` 的文档）

### 5.4 强制清理模式

提供命令行参数 `--clean` 用于显式清理所有测试数据：

```bash
# 正常测试（保留数据）
python integration_test.py

# 强制清理后重新测试
python integration_test.py --clean
```

`--clean` 模式下，测试开始前删除所有测试索引/模板/策略，从零开始。
