# QueryAnalyzer（查询分析器）实现计划

## 1. 概述

根据设计文档 `es-query-toolkit-design.md` 第 10.4 节的要求，实现 QueryAnalyzer 查询分析器功能，用于分析查询性能并提供优化建议。

**目标**：
- 分析 ES 查询的性能瓶颈
- 提供查询优化建议
- 支持查询性能剖析（Profile API）
- 识别慢查询

---

## 2. 文件结构设计

✅ **已完成**

遵循项目现有的模块化组织方式（参考 `bulk/` 和 `index_manager/` 目录结构）：

```
src/elasticflow/
├── query_analyzer/                    # ✅ 已完成
│   ├── __init__.py                   # ✅ 已完成
│   ├── models.py                     # ✅ 已完成
│   ├── tool.py                       # ✅ 已完成
│   ├── rules.py                      # ✅ 已完成
│   └── exceptions.py                 # ✅ 已完成
```

---

## 3. 核心类和方法设计

### 3.1 数据模型 (`models.py`)

✅ **已完成**

已实现以下模型:
- QueryOptimizationType (枚举)
- SeverityLevel (枚举)
- QuerySuggestion (数据类)
- ProfileShard (数据类)
- QueryProfile (数据类)
- QueryAnalysis (数据类)
- SlowQueryInfo (数据类)

### 3.2 优化规则引擎 (`rules.py`)

✅ **已完成**

已实现的优化规则:
- OptimizationRule (基类)
- LeadingWildcardRule (前导通配符检测)
- FullTextInFilterContextRule (全文查询在 filter 上下文检测)
- ScriptQueryRule (脚本查询检测)
- DeepNestedQueryRule (深层嵌套查询检测)
- SmallRangeQueryRule (小范围查询检测)
- RegexQueryRule (正则查询检测)
- RuleEngine (规则引擎)

### 3.3 核心工具类 (`tool.py`)

✅ **已完成**

QueryAnalyzer 类已实现以下方法:
- `__init__(es_client, slow_query_threshold_ms, enable_profiling)` - 初始化
- `analyze(index, query, profile)` - 分析查询
- `analyze_without_execution(query)` - 静态分析查询
- `explain_query(index, query, doc_id)` - 解释查询评分
- `validate_query(index, query)` - 验证查询语法
- `get_slow_queries(...)` - 获取慢查询列表
- `calculate_complexity_score(query)` - 计算查询复杂度
- `set_config(...)` - 更新配置
- `register_custom_rule(rule)` - 注册自定义规则

支持性能剖析功能，包括:
- 解析 ES Profile API 返回的数据
- 识别最慢的分片
- 提供时间分解信息
- 支持多级子查询的性能分析

### 3.4 异常定义 (`exceptions.py`)

✅ **已完成**

已实现的异常类:
- QueryAnalyzerError (基类)
- QueryValidationError
- QueryProfileError
- SlowQueryLogNotConfiguredError

---

## 4. 功能清单

### 4.1 核心功能

| 功能 | 方法 | 状态 | 说明 |
|------|------|------|------|
| 查询分析 | `analyze()` | ✅ 已完成 | 执行查询并分析性能 |
| 静态分析 | `analyze_without_execution()` | ✅ 已完成 | 不执行查询的静态分析 |
| 查询验证 | `validate_query()` | ✅ 已完成 | 验证查询语法有效性 |
| 复杂度评分 | `calculate_complexity_score()` | ✅ 已完成 | 计算查询复杂度 |
| 查询解释 | `explain_query()` | ✅ 已完成 | 解释文档评分过程 |
| 慢查询获取 | `get_slow_queries()` | ✅ 已完成 | 从慢查询日志获取慢查询 |
| 配置更新 | `set_config()` | ✅ 已完成 | 更新慢查询阈值和性能剖析设置 |
| 自定义规则 | `register_custom_rule()` | ✅ 已完成 | 注册自定义优化规则 |

### 4.2 优化规则（内置）

| 规则 | 类型 | 严重级别 | 状态 | 说明 |
|------|------|----------|------|------|
| 前导通配符检测 | `AVOID_WILDCARD_START` | CRITICAL | ✅ 已完成 | 检测 `*xxx` 或 `?xxx` 模式 |
| 全文查询优化 | `USE_FILTER_INSTEAD_QUERY` | INFO | ✅ 已完成 | 建议使用 filter 替代 query |
| 脚本查询检测 | `AVOID_SCRIPT_QUERY` | WARNING | ✅ 已完成 | 检测 script 查询的使用 |
| 深层嵌套检测 | `REDUCE_NESTED_DEPTH` | WARNING | ✅ 已完成 | 检测过深的 bool 嵌套 |
| 小范围优化 | `USE_TERMS_QUERY` | INFO | ✅ 已完成 | 建议用 terms 替代小范围 range |
| 正则查询检测 | `AVOID_REGEX_QUERY` | WARNING | ✅ 已完成 | 检测正则表达式查询 |
| 慢查询标记 | `LIMIT_RESULTS` | WARNING | ✅ 已完成 | 标记超过阈值的慢查询 |

### 4.3 性能剖析功能

✅ **已完成**

- ✅ 解析 ES Profile API 返回的数据
- ✅ 识别最慢的分片
- ✅ 提供时间分解（breakdown）信息
- ✅ 支持多级子查询的性能分析

---

## 5. 与现有代码的集成方式

### 5.1 主模块导出 (`src/elasticflow/__init__.py`)

✅ **已完成**

```python
# QueryAnalyzer 相关导出已添加到 __init__.py
from elasticflow.query_analyzer import (
    QueryAnalyzer,
    QueryAnalysis,
    QuerySuggestion,
    QueryOptimizationType,
    SeverityLevel,
    SlowQueryInfo,
    QueryProfile,
)
```

### 5.2 与 DslQueryBuilder 集成示例

```python
from elasticflow import DslQueryBuilder, QueryAnalyzer

# 构建查询
builder = DslQueryBuilder(search_factory=lambda: Search(index="logs"))
search = builder.add_filter(...).build()

# 分析查询
analyzer = QueryAnalyzer(es_client)
analysis = analyzer.analyze_without_execution(search.to_dict())

# 查看优化建议
for suggestion in analysis.suggestions:
    print(f"[{suggestion.severity}] {suggestion.message}")
    if suggestion.suggestion:
        print(f"  建议: {suggestion.suggestion}")
```

### 5.3 依赖关系

```
QueryAnalyzer
    ├── depends on: elasticsearch.Elasticsearch
    ├── uses: RuleEngine (内部)
    └── outputs: QueryAnalysis, QuerySuggestion, etc.
```

---

## 6. 测试策略

### 6.1 单元测试

**测试文件结构**：
- `test_models.py`: 数据模型测试
- `test_rules.py`: 优化规则测试
- `test_tool.py`: 核心工具类测试

**测试覆盖要求**：
- 所有公共方法 100% 覆盖
- 所有优化规则的正向和负向测试
- 边界条件测试

### 6.2 测试用例示例

```python
class TestQueryAnalyzer(unittest.TestCase):
    def setUp(self):
        self.es_client = MagicMock(spec=Elasticsearch)
        self.analyzer = QueryAnalyzer(self.es_client)
    
    def test_detect_leading_wildcard(self):
        """测试检测前导通配符"""
        query = {
            "query": {
                "wildcard": {"message": "*error*"}
            }
        }
        analysis = self.analyzer.analyze_without_execution(query)
        
        self.assertTrue(any(
            s.type == QueryOptimizationType.AVOID_WILDCARD_START
            for s in analysis.suggestions
        ))
    
    def test_slow_query_detection(self):
        """测试慢查询检测"""
        # Mock ES 返回慢查询
        self.es_client.search.return_value = {
            "took": 2000,
            "_shards": {"total": 5, "successful": 5, "failed": 0},
            "hits": {"total": {"value": 100}}
        }
        
        analysis = self.analyzer.analyze("logs", {"query": {"match_all": {}}})
        
        self.assertTrue(analysis.is_slow_query)


class TestOptimizationRules(unittest.TestCase):
    def test_leading_wildcard_rule(self):
        """测试前导通配符规则"""
        rule = LeadingWildcardRule()
        query = {"wildcard": {"field": "*test"}}
        
        suggestions = rule.check(query, {})
        
        self.assertEqual(len(suggestions), 1)
        self.assertEqual(suggestions[0].severity, SeverityLevel.CRITICAL)
```

### 6.3 Mock 策略

- 使用 `unittest.mock.MagicMock` 模拟 ES 客户端
- 准备典型的 ES 响应数据 fixtures
- 测试异常处理路径

---

## 7. 实现步骤

### 第一阶段：基础框架（P0） ✅ 已完成
- ✅ 创建目录结构
- ✅ 实现 `models.py` - 所有数据模型
- ✅ 实现 `exceptions.py` - 异常定义
- ✅ 实现 `rules.py` - 规则引擎基础框架

### 第二阶段：核心功能（P0） ✅ 已完成
- ✅ 实现 `tool.py` - `analyze_without_execution()` 静态分析
- ✅ 实现内置优化规则（前导通配符、全文查询、脚本查询）
- ✅ 实现 `analyze()` - 带执行的分析

### 第三阶段：扩展功能（P1） ✅ 已完成
- ✅ 实现 `validate_query()` - 查询验证
- ✅ 实现 `calculate_complexity_score()` - 复杂度评分
- ✅ 完善剩余优化规则

### 第四阶段：高级功能（P2） ✅ 已完成
- ✅ 实现 `explain_query()` - 查询解释
- ✅ 实现 `get_slow_queries()` - 慢查询获取
- ✅ 性能剖析功能完善

### 第五阶段：集成与文档
- ✅ 更新 `__init__.py` 导出
- ⏳ 编写单元测试 (待完成)
- ⏳ 更新设计文档 (待完成)

---

## 8. 注意事项

1. **API 兼容性**：支持 ES 7.x 和 8.x 版本
2. **性能考虑**：静态分析不应有明显延迟
3. **可扩展性**：规则引擎支持用户自定义规则
4. **错误处理**：所有 ES 调用需要妥善处理异常
5. **日志记录**：关键操作需要记录日志

---

## 9. 预期交付物

- [x] `src/elasticflow/query_analyzer/__init__.py` ✅
- [x] `src/elasticflow/query_analyzer/models.py` ✅
- [x] `src/elasticflow/query_analyzer/exceptions.py` ✅
- [x] `src/elasticflow/query_analyzer/rules.py` ✅
- [x] `src/elasticflow/query_analyzer/tool.py` ✅
- [ ] `tests/test_query_analyzer/__init__.py` ⏳ 待完成
- [ ] `tests/test_query_analyzer/test_models.py` ⏳ 待完成
- [ ] `tests/test_query_analyzer/test_rules.py` ⏳ 待完成
- [ ] `tests/test_query_analyzer/test_tool.py` ⏳ 待完成
- [x] 更新 `src/elasticflow/__init__.py` ✅

---

## 10. 实现总结

### 已完成功能

✅ **核心模块实现**
- 完整的 `query_analyzer` 模块结构
- 所有数据模型
- 所有异常类
- 规则引擎和6个内置优化规则
- 核心分析工具类 `QueryAnalyzer`

✅ **核心功能**
- 查询分析（支持 Profile API）
- 静态查询分析
- 查询验证
- 查询解释
- 慢查询获取
- 复杂度评分
- 自定义规则注册

✅ **性能剖析**
- Profile 数据解析
- 分片性能分析
- 时间分解分析
- 子查询性能追踪

✅ **集成**
- 主模块导出已更新
- 模块可通过 `from elasticflow import QueryAnalyzer` 等方式使用
- 代码质量检查通过

### 待完成功能

⏳ **测试**
- 单元测试框架
- 各模块单元测试
- 集成测试

⏳ **文档**
- 使用文档
- API 文档
- 示例代码
