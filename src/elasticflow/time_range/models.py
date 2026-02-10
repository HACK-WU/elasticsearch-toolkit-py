"""时间范围查询工具数据模型定义模块."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any


class TimeRangeType(str, Enum):
    """时间范围类型."""

    RELATIVE = "relative"  # 相对时间（如 "最近1小时"）
    ABSOLUTE = "absolute"  # 绝对时间（指定起止时间）
    QUICK = "quick"  # 快速选择（如 "today", "yesterday"）


class QuickTimeRange(str, Enum):
    """快速时间范围选项."""

    LAST_15_MINUTES = "15m"
    LAST_30_MINUTES = "30m"
    LAST_1_HOUR = "1h"
    LAST_3_HOURS = "3h"
    LAST_6_HOURS = "6h"
    LAST_12_HOURS = "12h"
    LAST_24_HOURS = "24h"
    LAST_7_DAYS = "7d"
    LAST_30_DAYS = "30d"
    TODAY = "today"
    YESTERDAY = "yesterday"
    THIS_WEEK = "this_week"
    LAST_WEEK = "last_week"
    THIS_MONTH = "this_month"
    LAST_MONTH = "last_month"


# 相对时间单位常量
RELATIVE_TIME_UNITS: dict[str, str] = {
    "s": "seconds",
    "m": "minutes",
    "h": "hours",
    "d": "days",
    "w": "weeks",
}

# 快速时间范围到 timedelta 参数的映射（仅简单偏移类型）
_QUICK_RANGE_DELTA_MAP: dict[QuickTimeRange, dict[str, int]] = {
    QuickTimeRange.LAST_15_MINUTES: {"minutes": 15},
    QuickTimeRange.LAST_30_MINUTES: {"minutes": 30},
    QuickTimeRange.LAST_1_HOUR: {"hours": 1},
    QuickTimeRange.LAST_3_HOURS: {"hours": 3},
    QuickTimeRange.LAST_6_HOURS: {"hours": 6},
    QuickTimeRange.LAST_12_HOURS: {"hours": 12},
    QuickTimeRange.LAST_24_HOURS: {"hours": 24},
    QuickTimeRange.LAST_7_DAYS: {"days": 7},
    QuickTimeRange.LAST_30_DAYS: {"days": 30},
}


@dataclass
class TimeRange:
    """时间范围数据类.

    Attributes:
        start: 开始时间
        end: 结束时间
        field: 时间字段名，默认为 "@timestamp"
        range_type: 时间范围类型
    """

    start: datetime
    end: datetime
    field: str = "@timestamp"
    range_type: TimeRangeType = TimeRangeType.ABSOLUTE

    def to_dsl(self) -> dict[str, Any]:
        """转换为 ES DSL range 查询.

        Returns:
            ES DSL range 查询字典
        """
        return {
            "range": {
                self.field: {
                    "gte": self.start.isoformat(),
                    "lte": self.end.isoformat(),
                    "format": "strict_date_optional_time||epoch_millis",
                }
            }
        }

    def to_filter(self) -> dict[str, Any]:
        """转换为 ES DSL filter 子句（不含外层 range key）.

        Returns:
            filter 子句字典，可直接用于 bool 查询的 filter 数组
        """
        return {
            self.field: {
                "gte": self.start.isoformat(),
                "lte": self.end.isoformat(),
                "format": "strict_date_optional_time||epoch_millis",
            }
        }

    @property
    def duration_seconds(self) -> float:
        """返回时间范围的持续时间（秒）."""
        return (self.end - self.start).total_seconds()

    def __repr__(self) -> str:
        return (
            f"TimeRange(start={self.start.isoformat()}, "
            f"end={self.end.isoformat()}, "
            f"field='{self.field}', "
            f"range_type={self.range_type.value})"
        )
