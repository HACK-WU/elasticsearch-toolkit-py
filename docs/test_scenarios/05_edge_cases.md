# 边界条件 + 异常处理 测试场景

> 本文档覆盖当前测试方案中缺失的边界条件和异常处理场景，优先级 P0-P1。

---

## 一、空值/Null 场景

### 1.1 空输入场景

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.1 | bulk | 空文档列表 `bulk_index(index, [])` | 返回 BulkResult(success=0, failed=0) | P0 |
| E.2 | builders | 空条件列表 `conditions([])` | 返回 match_all 或抛出明确异常 | P0 |
| E.3 | builders | 空 QueryString `query_string("")` | 返回 match_all 或抛出明确异常 | P0 |
| E.4 | parsers | 空响应 `parse_hits({})` | 返回空列表，不抛异常 | P0 |
| E.5 | geo | 空坐标点列表 `geo_polygon_query([])` | 抛出 ValueError | P0 |

### 1.2 Null 字段值

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.6 | bulk | 文档含 null 字段值 | ES 允许，正常写入 | P1 |
| E.7 | builders | 条件值为 null `{value: null}` | 正确处理，生成 exists/not_exists | P1 |
| E.8 | parsers | 响应含 null 字段 | 正确解析，返回 None | P1 |

---

## 二、极值场景

### 2.1 分页极值

| # | 场景 | 预期行为 | 优先级 |
|---|------|----------|--------|
| E.9 | page=0 | 自动修正为 page=1 或抛出明确异常 | P0 |
| E.10 | page_size=0 | 返回空结果或抛出明确异常 | P0 |
| E.11 | page_size=10000 | ES 限制，需 search_after 方案 | P1 |
| E.12 | from+size > 10000 | 触发 ES 限制异常或建议 search_after | P1 |

### 2.2 数值极值

| # | 场景 | 预期行为 | 优先级 |
|---|------|----------|--------|
| E.13 | float 最大值 | 正确处理，不溢出 | P2 |
| E.14 | float 最小值（负数） | 正确处理，不溢出 | P2 |
| E.15 | 时间戳=0 | 解析为 1970-01-01 | P2 |
| E.16 | 时间戳=负数 | 解析为 1970 前或抛出异常 | P2 |

### 2.3 字符串极值

| # | 场景 | 预期行为 | 优先级 |
|---|------|----------|--------|
| E.17 | 超长字符串（10KB+） | 正确处理，不截断 | P2 |
| E.18 | 超大数组（10000+元素） | 正确处理，内存合理 | P2 |

---

## 三、特殊字符场景

### 3.1 索引名特殊字符

| # | 场景 | 预期行为 | 优先级 |
|---|------|----------|--------|
| E.19 | 索引名含 `-` (如 `test-logs`) | ES 允许，正常创建 | P1 |
| E.20 | 索引名含 `_` (如 `test_logs`) | ES 允许，正常创建 | P1 |
| E.21 | 索引名含 `.` (如 `test.logs`) | ES 允许，正常创建 | P1 |
| E.22 | 索引名含中文 | ES 允许（不推荐），测试兼容性 | P2 |
| E.23 | 索引名含空格 | ES 拒绝，抛出明确异常 | P0 |
| E.24 | 索引名以 `.` 开头 | ES 保留，抛出明确异常 | P0 |

### 3.2 QueryString 特殊字符

| # | 场景 | 预期行为 | 优先级 |
|---|------|----------|--------|
| E.25 | 含 `+` 号 | 正确转义为 `\+` | P0 |
| E.26 | 含 `-` 号 | 正确转义为 `\-` | P0 |
| E.27 | 含 `:` 号 | 正确转义为 `\:` | P0 |
| E.28 | 含 `/` 号 | 正确转义为 `\/` | P0 |
| E.29 | 含双引号 | 正确转义为 `\"` | P0 |
| E.30 | 含反斜杠 | 正确转义为 `\\` | P1 |

---

## 四、异常处理场景

### 4.1 网络异常

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.31 | connection | 连接超时 | 抛出 ConnectionTimeout，含明确信息 | P0 |
| E.32 | connection | 连接拒绝 | 抛出 ConnectionError，含明确信息 | P0 |
| E.33 | connection | SSL 证书错误 | 抛出 SSLCertVerificationError | P1 |
| E.34 | connection | 认证失败 | 抛出 AuthenticationException | P0 |
| E.35 | connection | 集群不可达 | 抛出 ConnectionError，支持重试 | P0 |

### 4.2 索引异常

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.36 | index_manager | 索引不存在 | `get_index()` 返回 None 或抛出异常 | P0 |
| E.37 | index_manager | 索引已存在 | 抛出 IndexAlreadyExistsError | P0 |
| E.38 | index_manager | 别名不存在删除 | 抛出 NotFoundError 或静默失败 | P1 |
| E.39 | index_manager | Mapping 格式错误 | 抛出 RequestError，含错误详情 | P0 |
| E.40 | index_manager | Settings 格式错误 | 抛出 RequestError，含错误详情 | P0 |

### 4.3 批量操作异常

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.41 | bulk | 部分文档失败 | 返回 BulkResult(failed > 0, errors=[...]) | P0 |
| E.42 | bulk | 版本冲突 | 记录到 errors 列表，不中断批量操作 | P0 |
| E.43 | bulk | 索引不存在（自动创建关闭） | 记录到 errors 列表 | P0 |
| E.44 | bulk | 文档解析错误（非法 JSON） | 记录到 errors 列表 | P1 |
| E.45 | bulk | 内存溢出（超大批量） | 抛出 MemoryError 或分批处理 | P1 |

### 4.4 查询异常

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.46 | builders | 无效操作符 | 抛出 ValueError，含支持的运算符列表 | P0 |
| E.47 | builders | 无效字段名（含特殊字符） | 抛出 ValueError 或自动转义 | P1 |
| E.48 | builders | 聚合名称冲突 | 抛出 ValueError 或覆盖警告 | P1 |
| E.49 | geo | 无效坐标（经度 >180） | 抛出 ValueError | P0 |
| E.50 | geo | 无效坐标（纬度 >90） | 抛出 ValueError | P0 |
| E.51 | geo | 自相交多边形 | ES 返回错误或返回部分结果 | P2 |

### 4.5 解析异常

| # | 模块 | 场景 | 预期行为 | 优先级 |
|---|------|------|----------|--------|
| E.52 | parsers | 空响应 `{}` | 返回空列表/None，不抛异常 | P0 |
| E.53 | parsers | 畸形 JSON | 抛出 JSONDecodeError | P1 |
| E.54 | parsers | 缺失聚合键 | 返回 None 或空列表 | P0 |
| E.55 | parsers | 类型不匹配（预期 number 实际 string） | 返回 None 或抛出 TypeError | P1 |

---

## 五、输出示例

```
── 场景 E.1: 空文档列表 ─────────────────────────────
  操作: BulkOperationTool.bulk_index(index="test_logs", docs=[])
  预期: 返回 BulkResult(success=0, failed=0, errors=[])
  实际: BulkResult(success=0, failed=0, took=0.0s)
  结果: ✅ PASS (0.12ms)

── 场景 E.34: 认证失败 ─────────────────────────────
  操作: ESClientFactory([ClusterConfig(
          hosts=[ES_HOST], username="wrong", password="wrong"
        )]).get_client().info()
  预期: 抛出 AuthenticationException
  实际: AuthenticationException: 401 Unauthorized
  结果: ✅ PASS (105.23ms)

── 场景 E.49: 无效坐标（经度 >180） ───────────────
  操作: GeoPoint(lat=39.9, lon=200.0)
  预期: 抛出 ValueError: 经度必须在 -180 到 180 之间
  实际: ValueError: longitude must be in range [-180, 180], got 200.0
  结果: ✅ PASS (0.03ms)
```

---

## 六、测试数据准备

边界条件测试需要额外的测试数据：

```python
# 极值测试数据
EXTREME_DATA = {
    "max_float": {"value": 1.7976931348623157e+308},
    "min_float": {"value": -1.7976931348623157e+308},
    "long_string": {"value": "a" * 10000},
    "large_array": {"tags": list(range(10000))},
}

# 特殊字符索引名（需单独创建，不与主测试索引混合）
SPECIAL_INDEX_NAMES = [
    "test-with-dash",
    "test.with.dot",
    "test_underscore",
]
```
