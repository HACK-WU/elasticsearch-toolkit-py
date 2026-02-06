"""ES Query Toolkit - Elasticsearch Query Building and Transformation Toolkit.

这是一个用于简化 Elasticsearch 查询构建和转换的 Python 库。

主要功能:
    - QueryStringBuilder: 构建 Query String 查询
    - DslQueryBuilder: 构建完整的 ES DSL 查询
    - QueryStringTransformer: 转换和处理 Query String
    - ResponseParser: 解析 ES 查询结果

使用示例:
    from elasticflow import QueryStringBuilder, QueryStringOperator

    builder = QueryStringBuilder()
    builder.add_filter("status", QueryStringOperator.EQUAL, ["error"])
    query_string = builder.build()
"""

__version__ = "0.4.0"

# 导出构建器
from elasticflow.builders import DslQueryBuilder, SubAggregation, QueryStringBuilder

# 导出核心组件
from elasticflow.core import (
    ConditionItem,
    ConditionParser,
    DefaultConditionParser,
    FieldMapper,
    GroupRelation,
    LogicOperator,
    Q,
    QueryField,
    QueryStringOperator,
    escape_query_string,
)

# 导出异常
from elasticflow.exceptions import (
    ConditionParseError,
    EsQueryToolkitError,
    QueryStringParseError,
    UnsupportedOperatorError,
)

# 导出转换器
from elasticflow.transformers import QueryStringTransformer

# 导出解析器
from elasticflow.parsers import (
    ResponseParser,
    DataCleaner,
    NullHandling,
    PagedResponse,
    HighlightedHit,
    TermsBucket,
    StatsResult,
    PercentilesResult,
    CardinalityResult,
    SuggestionItem,
)

# 导出批量操作工具
from elasticflow.bulk import (
    BulkAction,
    BulkErrorItem,
    BulkOperation,
    BulkOperationTool,
    BulkResult,
)

# 导出索引管理器
from elasticflow.index_manager import (
    IndexManager,
    IndexInfo,
    AliasInfo,
    IndexTemplateInfo,
    ILMPolicyInfo,
    ILMPhase,
    ILMIndexStatus,
    RolloverInfo,
    IndexSettings,
    MappingProperty,
    IndexMappings,
)

# 导出查询分析器
from elasticflow.query_analyzer import (
    QueryAnalyzer,
    QueryAnalysis,
    QuerySuggestion,
    QueryOptimizationType,
    QueryProfile,
    ProfileShard,
    SlowQueryInfo,
    SeverityLevel,
    RuleEngine,
    OptimizationRule,
)

__all__ = [
    # 版本
    "__version__",
    # 构建器
    "QueryStringBuilder",
    "DslQueryBuilder",
    "SubAggregation",
    # 操作符和枚举
    "QueryStringOperator",
    "LogicOperator",
    "GroupRelation",
    # 核心组件
    "QueryField",
    "FieldMapper",
    "ConditionItem",
    "ConditionParser",
    "DefaultConditionParser",
    "Q",
    "escape_query_string",
    # 异常
    "EsQueryToolkitError",
    "QueryStringParseError",
    "ConditionParseError",
    "UnsupportedOperatorError",
    # 转换器
    "QueryStringTransformer",
    # 解析器
    "ResponseParser",
    "DataCleaner",
    "NullHandling",
    "PagedResponse",
    "HighlightedHit",
    "TermsBucket",
    "StatsResult",
    "PercentilesResult",
    "CardinalityResult",
    "SuggestionItem",
    # 批量操作工具
    "BulkAction",
    "BulkErrorItem",
    "BulkOperation",
    "BulkOperationTool",
    "BulkResult",
    # 索引管理器
    "IndexManager",
    "IndexInfo",
    "AliasInfo",
    "IndexTemplateInfo",
    "ILMPolicyInfo",
    "ILMPhase",
    "ILMIndexStatus",
    "RolloverInfo",
    "IndexSettings",
    "MappingProperty",
    "IndexMappings",
    # 查询分析器
    "QueryAnalyzer",
    "QueryAnalysis",
    "QuerySuggestion",
    "QueryOptimizationType",
    "QueryProfile",
    "ProfileShard",
    "SlowQueryInfo",
    "SeverityLevel",
    "RuleEngine",
    "OptimizationRule",
]
