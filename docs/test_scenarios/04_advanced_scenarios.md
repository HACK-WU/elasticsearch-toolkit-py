# 模块 8-10：时间范围 + 地理查询 + 查询分析 测试场景

## 模块 8：TimeRangeQueryTool — 时间范围

### 8.1 时间范围场景（4个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 8.1 | 快速时间范围 | `quick_range(LAST_7_DAYS)` 执行正确 | P0 |
| 8.2 | 相对时间范围 | `relative_range(30, "m")` 返回 DSL | P0 |
| 8.3 | 绝对时间范围 | 指定起止时间，边界正确 | P0 |
| 8.4 | 时间字符串解析 | `now-1h` / ISO 8601 / 时间戳解析 | P0 |

### 8.2 输出示例

```
── 场景 8.1: 快速时间范围 (LAST_7_DAYS) ─────────────
  操作: TimeRangeQueryTool(time_field="timestamp")
          .quick_range(QuickTimeRange.LAST_7_DAYS)
        执行: POST test_logs/_search with range filter
  预期: 返回最近7天的文档
  实际: dsl={"range":{"timestamp":{"gte":"now-7d"}}}, total=500
  结果: ✅ PASS (11.61ms)

── 场景 8.4: 时间字符串解析 ────────────────────────
  操作: TimeRangeQueryTool.parse_time_string("now-1h")
  预期: 返回约1小时前的 datetime 对象（误差<60s）
  实际: parsed=2026-03-02T09:30:00Z, diff=3600s
  结果: ✅ PASS (0.05ms)
```

### 8.3 边界时间场景（可选）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 8.5 | 跨时区时间范围 | UTC/本地时间正确转换 | P2 |
| 8.6 | 未来时间检测 | 警告或拒绝未来时间范围 | P2 |

---

## 模块 9：GeoQueryTool — 地理查询

### 9.1 地理查询场景（5个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 9.1 | 距离查询 | 北京天安门附近 50km POI | P0 |
| 9.2 | 边界框查询 | 矩形范围内 POI | P0 |
| 9.3 | 多边形查询 | 多边形范围内 POI | P1 |
| 9.4 | 距离排序 | 按距离升序排列 | P0 |
| 9.5 | 地理聚合 | 距离聚合 + 边界聚合 | P1 |

### 9.2 输出示例

```
── 场景 9.1: 距离查询 (天安门50km) ─────────────────
  操作: GeoQueryTool(geo_field="location")
          .geo_distance_query(center=GeoPoint(39.9042, 116.3974), 
                              distance=50, unit=KILOMETERS)
  预期: 返回天安门50km范围内的POI
  实际: dsl={"geo_distance":{"location":{...},"distance":"50km"}}, total=50
  结果: ✅ PASS (42.84ms)

── 场景 9.4: 距离排序 ─────────────────────────────
  操作: GeoQueryTool.geo_distance_sort(center=天安门, unit=km, order=asc)
  预期: 结果按距离升序排列
  实际: 第1条距离=0.5km, 第2条=1.2km, ..., 递增验证通过
  结果: ✅ PASS (37.49ms)
```

### 9.3 边界地理场景（可选）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 9.6 | 跨日期变更线边界框 | 经度跨越180°/-180° | P2 |
| 9.7 | 极地坐标处理 | 纬度接近90°的查询 | P2 |

---

## 模块 10：QueryAnalyzer — 查询分析

### 10.1 分析场景（4个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 10.1 | 简单查询分析 | 返回 QueryAnalysis 结构（took_ms, is_slow, suggestions） | P0 |
| 10.2 | 带 Profile 分析 | `profile=True` 返回分片性能数据 | P0 |
| 10.3 | 前导通配符检测 | 触发 LeadingWildcardRule 告警 | P0 |
| 10.4 | 复杂查询复杂度评分 | `query_complexity_score` 合理 | P0 |

### 10.2 输出示例

```
── 场景 10.2: 带 Profile 分析 ──────────────────────
  操作: QueryAnalyzer.analyze(index, {"query":{"match_all":{}}}, profile=True)
  预期: 返回 profile 字段，包含 shards 性能数据
  实际: took=5ms, profile_shards=3, is_slow=False
  结果: ✅ PASS (14.16ms)

── 场景 10.3: 前导通配符检测 ─────────────────────
  操作: QueryAnalyzer.analyze(index, {"query":{"query_string":{
          "query":"message:*error*"
        }}})
  预期: suggestions 包含前导通配符警告
  实际: suggestions=[Suggestion(rule="leading_wildcard", message="...")]
  结果: ✅ PASS (29.54ms)
```

### 10.3 高级分析场景（可选）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 10.5 | 脚本注入检测 | script 查询安全警告 | P1 |
| 10.6 | 高基数字段聚合警告 | 大量唯一值的 terms 聚合警告 | P2 |
| 10.7 | 深度分页警告 | from+size > 10000 的警告 | P1 |
