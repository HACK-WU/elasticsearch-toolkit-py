"""结果解析器模块.

提供 ES 查询结果的解析功能.
"""

from elasticflow.parsers.response import ResponseParser
from elasticflow.parsers.types import (
    CardinalityResult,
    DataCleaner,
    HighlightedHit,
    NullHandling,
    PagedResponse,
    PercentilesResult,
    StatsResult,
    SuggestionItem,
    TermsBucket,
)

__all__ = [
    "ResponseParser",
    "PagedResponse",
    "HighlightedHit",
    "TermsBucket",
    "StatsResult",
    "PercentilesResult",
    "CardinalityResult",
    "SuggestionItem",
    "DataCleaner",
    "NullHandling",
]
