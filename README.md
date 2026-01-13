# ElasticFlow

[![Python Version](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Test Coverage](https://img.shields.io/badge/coverage-93%25-brightgreen.svg)](tests/)

Elasticsearch Query Building and Transformation Toolkit - 一个用于简化 Elasticsearch 查询构建和转换的 Python 库。

## 🌟 项目背景

本项目在开发过程中参考了[蓝鲸监控平台(bk-monitor)](https://github.com/TencentBlueKing/bk-monitor)的部分设计思路和代码实现。蓝鲸监控作为腾讯开源的监控平台，在 Elasticsearch 的实际应用方面积累了丰富的经验，为本项目提供了宝贵的技术参考。

## ✨ 特性

- **QueryStringBuilder**: 构建 Elasticsearch Query String 查询
  - 支持多种操作符（等于、包含、范围、正则等）
  - 自动处理特殊字符转义
  - 支持链式调用
  - 灵活的多值逻辑组合（AND/OR）

- **DslQueryBuilder**: 构建完整的 ES DSL 查询
  - 结构化条件过滤
  - Query String 查询集成
  - 排序和分页支持
  - 聚合查询支持
  - 字段名映射

- **QueryStringTransformer**: Query String 转换和处理（可选）
  - 字段名映射（中文 → 英文）
  - 值翻译（显示值 → 实际值）
  - 基于语法树的精确转换

## 📦 安装

### 开发安装

```bash
git clone https://github.com/HACK-WU/elasticflow.git
cd elasticflow
uv sync --all-groups
```

## 🚀 快速开始

### 1. 使用 QueryStringBuilder

```python
from elasticflow import QueryStringBuilder, QueryStringOperator

# 创建构建器
builder = QueryStringBuilder()

# 添加过滤条件
builder.add_filter("status", QueryStringOperator.EQUAL, ["error", "warning"])
builder.add_filter("level", QueryStringOperator.GTE, [3])
builder.add_filter("message", QueryStringOperator.INCLUDE, ["timeout"])

# 构建 Query String
query_string = builder.build()
print(query_string)
# 输出: status: ("error" OR "warning") AND level: >=3 AND message: *timeout*
```

### 2. 使用 DslQueryBuilder

```python
from elasticsearch.dsl import Search
from elasticflow import DslQueryBuilder, FieldMapper, QueryField

# 定义字段映射
fields = [
  QueryField(field="status", es_field="doc_status", display="状态"),
  QueryField(field="level", es_field="severity", display="级别"),
]

# 创建构建器
builder = DslQueryBuilder(
  search_factory=lambda: Search(index="logs"),
  field_mapper=FieldMapper(fields),
)

# 构建查询
search = (
  builder
  .conditions([
    {"key": "status", "method": "eq", "value": ["error"]},
    {"key": "level", "method": "gte", "value": [2]},
  ])
  .query_string("message: timeout")
  .ordering(["-create_time"])
  .pagination(page=1, page_size=20)
  .build()
)

# 执行查询
result = search.execute()
```

### 3. 使用 QueryStringTransformer

```python
from elasticflow import QueryStringTransformer

# 创建转换器
transformer = QueryStringTransformer(
  field_mapping={
    "状态": "status",
    "级别": "severity",
  },
  value_translations={
    "severity": [("1", "致命"), ("2", "预警"), ("3", "提醒")],
    "status": [("ABNORMAL", "未恢复"), ("RECOVERED", "已恢复")],
  },
)

# 转换用户输入的中文查询
result = transformer.transform("级别: 致命 AND 状态: 未恢复")
print(result)
# 输出: severity: 1 AND status: ABNORMAL
```

## 📚 详细用法

### QueryStringBuilder

#### 支持的操作符

| 操作符 | 说明 | 示例 |
|--------|------|------|
| `EQUAL` | 精确匹配 | `status: "error"` |
| `NOT_EQUAL` | 不等于 | `NOT status: "error"` |
| `INCLUDE` | 模糊匹配（包含） | `message: *timeout*` |
| `NOT_INCLUDE` | 不包含 | `NOT message: *debug*` |
| `GT` / `GTE` | 大于/大于等于 | `level: >3` 或 `level: >=3` |
| `LT` / `LTE` | 小于/小于等于 | `level: <5` 或 `level: <=5` |
| `BETWEEN` | 范围查询 | `age: [18 TO 60]` |
| `EXISTS` | 字段存在 | `field: *` |
| `NOT_EXISTS` | 字段不存在 | `NOT field: *` |
| `REG` / `NREG` | 正则表达式 | `email: /.*@example\.com/` |

#### 高级功能

**操作符映射** - 兼容外部系统的操作符名称：

```python
operator_mapping = {
    "eq": QueryStringOperator.EQUAL,
    "neq": QueryStringOperator.NOT_EQUAL,
    "contains": QueryStringOperator.INCLUDE,
}

builder = QueryStringBuilder(operator_mapping=operator_mapping)
builder.add_filter("status", "eq", ["error"])  # 使用自定义操作符名
```

**多值逻辑关系**：

```python
from elasticflow import GroupRelation

# OR 关系（默认）
builder.add_filter("status", QueryStringOperator.EQUAL, ["error", "warning"])
# 输出: status: ("error" OR "warning")

# AND 关系
builder.add_filter("tag", QueryStringOperator.EQUAL, ["tag1", "tag2"], group_relation=GroupRelation.AND)
# 输出: tag: ("tag1" AND "tag2")
```

**通配符保留**：

```python
# 保留用户输入的通配符
builder.add_filter("message", QueryStringOperator.INCLUDE, ["err*or"], is_wildcard=True)
# 输出: message: *err*or*
```

### DslQueryBuilder

#### 条件方法支持

| Method | ES Query 类型 | 说明 |
|--------|--------------|------|
| `eq` | terms | 精确匹配 |
| `neq` | ~terms | 不等于 |
| `include` | wildcard | 模糊匹配 `*value*` |
| `exclude` | ~wildcard | 排除匹配 |
| `gt/gte/lt/lte` | range | 范围查询 |
| `exists/nexists` | exists | 字段存在/不存在 |

#### 自定义条件解析器

```python
from elasticflow import ConditionParser, ConditionItem
from elasticsearch.dsl import Q


class CustomConditionParser(ConditionParser):
  def parse(self, condition: ConditionItem):
    # 处理特殊字段
    if condition.key == "tags":
      return Q("nested", path="tags", query=Q("term", **{"tags.name": condition.value}))

    # 其他使用默认解析
    return DefaultConditionParser().parse(condition)


builder = DslQueryBuilder(
  search_factory=lambda: Search(index="docs"),
  condition_parser=CustomConditionParser(),
)
```

#### 聚合支持

```python
search = (
    builder
    .conditions([{"key": "status", "method": "eq", "value": ["error"]}])
    .add_aggregation("status_count", "terms", field="status", size=10)
    .add_aggregation("avg_response_time", "avg", field="response_time")
    .build()
)

result = search.execute()
print(result.aggregations.status_count.buckets)
```

### QueryStringTransformer

#### 字段映射

```python
transformer = QueryStringTransformer(
    field_mapping={
        "消息": "message",
        "状态": "status",
        "创建时间": "create_time",
    }
)

result = transformer.transform("消息: error AND 状态: active")
# 输出: message: error AND status: active
```

#### 值翻译

**有指定字段的值翻译**：

```python
transformer = QueryStringTransformer(
    value_translations={
        "severity": [("1", "致命"), ("2", "预警"), ("3", "提醒")],
    }
)

result = transformer.transform("severity: 致命")
# 输出: severity: 1
```

**无指定字段的值翻译**（自动生成 OR 表达式）：

```python
result = transformer.transform("致命")
# 输出: "致命" OR (severity: 1)
```

## 🔧 配置示例

### Django 项目集成

```python
# settings.py 或单独的配置文件
from elasticflow import QueryField

ALERT_FIELDS = [
  QueryField(field="status", es_field="status", display="状态"),
  QueryField(field="severity", es_field="severity", display="级别"),
  QueryField(field="alert_name", es_field="alert_name.raw", es_field_for_agg="alert_name.raw"),
]

VALUE_TRANSLATIONS = {
  "severity": [("1", "致命"), ("2", "预警"), ("3", "提醒")],
  "status": [("ABNORMAL", "未恢复"), ("RECOVERED", "已恢复")],
}
```

```python
# views.py
from elasticflow import DslQueryBuilder, FieldMapper, QueryStringTransformer
from .settings import ALERT_FIELDS, VALUE_TRANSLATIONS


def search_alerts(request):
  # 创建转换器和构建器
  transformer = QueryStringTransformer(value_translations=VALUE_TRANSLATIONS)
  builder = DslQueryBuilder(
    search_factory=lambda: AlertDocument.search(),
    field_mapper=FieldMapper(ALERT_FIELDS),
    query_string_transformer=transformer.transform,
  )

  # 构建查询
  search = (
    builder
    .conditions(request.data.get("conditions", []))
    .query_string(request.data.get("query_string", ""))
    .ordering(request.data.get("ordering", ["-create_time"]))
    .pagination(
      page=request.data.get("page", 1),
      page_size=request.data.get("page_size", 20)
    )
    .build()
  )

  result = search.execute()
  return Response({"data": [hit.to_dict() for hit in result]})
```

## 🧪 测试

```bash
# 运行所有测试
pytest

# 运行测试并显示覆盖率
pytest --cov=src/elasticflow --cov-report=term-missing

# 运行特定测试文件
pytest tests/test_query_string_builder.py -v
```

## 📖 API 文档

### 主要类

- **QueryStringBuilder**: Query String 构建器
- **DslQueryBuilder**: DSL 查询构建器
- **QueryStringTransformer**: Query String 转换器
- **QueryField**: 字段配置类
- **FieldMapper**: 字段映射器
- **ConditionParser**: 条件解析器（抽象基类）
- **DefaultConditionParser**: 默认条件解析器

### 枚举类

- **QueryStringOperator**: Query String 操作符
- **LogicOperator**: 逻辑操作符（AND/OR）
- **GroupRelation**: 多值关系（or/and）

### 异常类

- **EsQueryToolkitError**: 基础异常类
- **QueryStringParseError**: Query String 解析异常
- **ConditionParseError**: 条件解析异常
- **UnsupportedOperatorError**: 不支持的操作符异常

## 🤝 贡献

欢迎贡献代码、报告问题或提出新功能建议！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 开启 Pull Request

## 📝 版本历史

### v0.2.0 (2026-01-13)

- ✅ 添加 QueryStringTransformer（Query String 转换器）
- ✅ 支持字段名映射和值翻译
- ✅ 完整的单元测试覆盖（93%）

### v0.1.0 (2026-01-13)

- ✅ QueryStringBuilder 实现
- ✅ DslQueryBuilder 实现
- ✅ 核心模块和操作符定义

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 🔗 相关链接

- [Elasticsearch 官方文档](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [elasticsearch-dsl-py](https://github.com/elastic/elasticsearch-dsl-py)
- [luqum - Lucene Query Parser](https://github.com/jurismarches/luqum)

## 💡 常见问题


### Q: 如何处理嵌套字段查询？

**A**: 自定义 ConditionParser 处理嵌套字段：

```python
class NestedFieldParser(ConditionParser):
    def parse(self, condition):
        if condition.key.startswith("nested."):
            return Q("nested", path="nested", query=...)
        return DefaultConditionParser().parse(condition)
```

### Q: 字段映射不生效？

**A**: 确保在创建 DslQueryBuilder 时传入了 FieldMapper：

```python
builder = DslQueryBuilder(
    search_factory=lambda: Search(index="..."),
    field_mapper=FieldMapper(fields=[...]),  # 必须传入
)
```

### Q: 如何调试生成的 DSL？

**A**: 使用 `to_dict()` 方法查看生成的 DSL：

```python
dsl = builder.to_dict()
import json
print(json.dumps(dsl, indent=2))
```

---

**如有问题或建议，欢迎提 Issue！** 🎉

## 🙏 致谢

本项目在开发过程中参考了以下开源项目的设计思路和部分代码实现，特此感谢：

### 蓝鲸监控平台

- 参考了 Elasticsearch 客户端管理模块的配置设计
- 参考了 DSL 查询编译器的构建模式

**说明**：上述参考内容均经过了架构重构和功能增强，并已移除所有框架依赖，确保了项目的独立性和可移植性。

感谢所有为开源社区贡献力量的开发者们！
