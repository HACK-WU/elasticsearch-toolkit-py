# 模块 4-6：DSL查询 + QueryString + 转换 测试场景

## 模块 4：DslQueryBuilder — DSL 查询构建与执行

### 4.1 条件过滤场景（6个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 4.1 | 简单条件过滤 (equal) | `level == "ERROR"` 结果全部为 ERROR | P0 |
| 4.2 | 范围条件 (gt/lt/gte/lte) | `response_time > 1000` 结果均满足 | P0 |
| 4.3 | 包含/排除 (include/exclude) | 多值匹配/排除 | P0 |
| 4.4 | 存在/不存在 (exists/nexists) | 字段存在性判断 | P0 |
| 4.5 | ConditionGroup (AND/OR) | 嵌套逻辑组合 `(A AND B) OR C` | P0 |
| 4.6 | NestedCondition | 嵌套文档条件 `items.product == "iPhone"` | P1 |

### 4.2 查询执行场景（4个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 4.7 | QueryString 查询 | `level:ERROR AND service:api-gateway` | P0 |
| 4.8 | 排序 + 分页 | 降序排列 + 分页参数正确 | P0 |
| 4.9 | 聚合查询 | terms/stats/cardinality/percentiles | P0 |
| 4.10 | 组合查询 | 条件+QueryString+排序+分页+聚合 | P1 |

### 4.3 输出示例

```
── 场景 4.1: 简单条件过滤 (equal) ──────────────────
  操作: DslQueryBuilder.conditions([{key:"level", method:"eq", value:["ERROR"]}])
        执行查询: POST test_logs/_search
        DSL: {"query":{"bool":{"filter":[{"terms":{"level":["ERROR"]}}]}},"size":50}
  预期: 返回的每条文档 _source.level 均为 "ERROR"
  实际: 命中 total=127, 抽检50条全部为 level=ERROR
  结果: ✅ PASS (12.21ms)

── 场景 4.5: ConditionGroup (OR) ───────────────────
  操作: DslQueryBuilder.conditions([
          {type:"group", condition:"or", children:[
            {key:"level", method:"eq", value:["ERROR"]},
            {key:"level", method:"eq", value:["WARN"]}
          ]}
        ])
  预期: 返回 level 为 ERROR 或 WARN 的文档
  实际: total=249, 全部为 ERROR 或 WARN
  结果: ✅ PASS (28.74ms)
```

---

## 模块 5：QueryStringBuilder + Q 对象

### 5.1 构建场景（4个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 5.1 | QueryStringBuilder 基本构建 | 各操作符生成正确 QueryString | P0 |
| 5.2 | 特殊字符转义 | 含 `+ - : /` 的值正确转义 | P0 |
| 5.3 | Q 对象组合 | `&` `|` `~` 运算符正确组合 | P0 |
| 5.4 | Q 对象执行验证 | 构建的查询在 ES 上正确执行 | P0 |

### 5.2 输出示例

```
── 场景 5.2: 特殊字符转义 ──────────────────────────
  操作: escape_query_string("error+timeout: /api/v1")
  预期: 特殊字符 + : / 被转义为 \\+ \\: \\/
  实际: "error\\+timeout\\: \\/api\\/v1"
  结果: ✅ PASS (0.02ms)
```

---

## 模块 6：QueryStringTransformer — 查询转换

### 6.1 转换场景（3个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 6.1 | 字段名映射 | 中文字段名 → ES 字段名 | P0 |
| 6.2 | 值翻译 | 枚举值中英文映射（错误→ERROR） | P0 |
| 6.3 | 转换后执行 | 转换后的 QueryString 在 ES 可执行 | P0 |

### 6.2 输出示例

```
── 场景 6.2: 值翻译 ─────────────────────────────────
  操作: QueryStringTransformer(value_translations={
          "level": [("ERROR", "错误"), ("WARN", "警告")]
        }).transform("日志级别:错误")
  预期: "错误" 被翻译为 "ERROR"
  实际: "level:ERROR"
  结果: ✅ PASS (0.18ms)
```

### 6.3 复杂场景（可选）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 6.4 | 多级字段映射 | 嵌套字段路径转换 | P2 |
| 6.5 | 正则表达式替换 | 复杂值转换规则 | P2 |
