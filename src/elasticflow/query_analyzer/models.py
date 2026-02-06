from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class QueryOptimizationType(str, Enum):
    """优化建议类型"""

    USE_FILTER_INSTEAD_QUERY = "use_filter_instead_query"
    ADD_INDEX_FIELD = "add_index_field"
    AVOID_WILDCARD_START = "avoid_wildcard_start"
    AVOID_REGEX_QUERY = "avoid_regex_query"
    LIMIT_RESULTS = "limit_results"
    USE_TERMS_QUERY = "use_terms_query"
    AVOID_SCRIPT_QUERY = "avoid_script_query"
    USE_DOC_VALUES = "use_doc_values"
    OPTIMIZE_AGGREGATION = "optimize_aggregation"
    REDUCE_NESTED_DEPTH = "reduce_nested_depth"


class SeverityLevel(str, Enum):
    """严重级别"""

    CRITICAL = "critical"  # 严重问题，必须修复
    WARNING = "warning"  # 警告，建议修复
    INFO = "info"  # 信息提示，可选优化


@dataclass
class QuerySuggestion:
    """查询优化建议"""

    type: QueryOptimizationType
    severity: SeverityLevel
    message: str
    affected_field: str | None = None
    suggestion: str | None = None
    estimated_impact: str | None = None  # 预估影响


@dataclass
class ProfileShard:
    """分片性能剖析数据"""

    shard_id: str
    node_id: str
    total_time_ns: int
    breakdown: dict[str, int]
    children: list["ProfileShard"] | None = None


@dataclass
class QueryProfile:
    """查询性能剖析结果"""

    shards: list[ProfileShard]
    total_time_ms: float
    slowest_shard: ProfileShard | None = None


@dataclass
class QueryAnalysis:
    """查询分析结果"""

    query: dict
    total_shards: int
    successful_shards: int
    failed_shards: int
    took_ms: float
    is_slow_query: bool
    suggestions: list[QuerySuggestion]
    profile: QueryProfile | None = None
    query_complexity_score: int = 0  # 查询复杂度评分


@dataclass
class SlowQueryInfo:
    """慢查询信息"""

    query: dict
    index: str
    took_ms: float
    timestamp: datetime | None = None
    source: str | None = None
