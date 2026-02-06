"""
QueryAnalyzer 模块 - 查询分析器

该模块提供了查询性能分析、优化建议和慢查询检测功能。

主要组件:
- QueryAnalyzer: 核心查询分析器类
- RuleEngine: 优化规则引擎
- OptimizationRule: 优化规则基类

主要功能:
- 查询性能分析（支持 Profile API）
- 静态查询分析（不执行查询）
- 查询优化建议
- 慢查询检测
- 查询复杂度评分
- 查询验证和解释
"""

from .tool import QueryAnalyzer
from .models import (
    QueryAnalysis,
    QuerySuggestion,
    QueryOptimizationType,
    QueryProfile,
    ProfileShard,
    SlowQueryInfo,
    SeverityLevel,
)
from .rules import (
    RuleEngine,
    OptimizationRule,
    LeadingWildcardRule,
    FullTextInFilterContextRule,
    ScriptQueryRule,
    DeepNestedQueryRule,
    SmallRangeQueryRule,
    RegexQueryRule,
)
from .exceptions import (
    QueryAnalyzerError,
    QueryValidationError,
    QueryProfileError,
    SlowQueryLogNotConfiguredError,
)

__all__ = [
    # 核心类
    "QueryAnalyzer",
    "RuleEngine",
    "OptimizationRule",
    # 数据模型
    "QueryAnalysis",
    "QuerySuggestion",
    "QueryOptimizationType",
    "QueryProfile",
    "ProfileShard",
    "SlowQueryInfo",
    "SeverityLevel",
    # 内置规则
    "LeadingWildcardRule",
    "FullTextInFilterContextRule",
    "ScriptQueryRule",
    "DeepNestedQueryRule",
    "SmallRangeQueryRule",
    "RegexQueryRule",
    # 异常
    "QueryAnalyzerError",
    "QueryValidationError",
    "QueryProfileError",
    "SlowQueryLogNotConfiguredError",
]

__version__ = "1.0.0"
