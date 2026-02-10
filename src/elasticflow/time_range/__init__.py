"""
TimeRangeQueryTool 模块 - 时间范围查询工具

该模块提供了简化时间范围查询构建的功能，支持多种时间范围选择方式。

主要组件:
- TimeRangeQueryTool: 核心时间范围查询工具类
- TimeRange: 时间范围数据类
- TimeRangeType: 时间范围类型枚举
- QuickTimeRange: 快速时间范围选项枚举

主要功能:
- 快速时间范围选择（如最近1小时、最近24小时、今天、昨天等）
- 相对时间范围（如最近30分钟、最近7天）
- 绝对时间范围（指定起止时间）
- 时间字符串解析（支持 ISO 8601、时间戳、相对时间表达式）
- 生成 ES DSL range 查询

示例用法:
    >>> from elasticflow.time_range import TimeRangeQueryTool, QuickTimeRange
    >>> time_tool = TimeRangeQueryTool(time_field="@timestamp")
    >>> time_range = time_tool.quick_range(QuickTimeRange.LAST_24_HOURS)
    >>> dsl = time_range.to_dsl()
"""

from .models import (
    TimeRangeType,
    QuickTimeRange,
    TimeRange,
)
from .tool import TimeRangeQueryTool
from .exceptions import (
    TimeRangeError,
    TimeRangeParseError,
    InvalidTimeRangeError,
)

__all__ = [
    # 核心工具类
    "TimeRangeQueryTool",
    # 数据模型
    "TimeRangeType",
    "QuickTimeRange",
    "TimeRange",
    # 异常
    "TimeRangeError",
    "TimeRangeParseError",
    "InvalidTimeRangeError",
]

__version__ = "0.4.0"
