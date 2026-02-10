"""时间范围查询工具核心实现模块."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, UTC
from typing import Any
from collections.abc import Callable

from .exceptions import InvalidTimeRangeError, TimeRangeParseError
from .models import (
    RELATIVE_TIME_UNITS,
    QuickTimeRange,
    TimeRange,
    TimeRangeType,
    _QUICK_RANGE_DELTA_MAP,
)


class TimeRangeQueryTool:
    """时间范围查询工具.

    提供便捷的时间范围构建功能，支持快速选择、相对时间、绝对时间和时间字符串解析。
    生成的 TimeRange 对象可直接转换为 ES DSL range 查询。

    Args:
        time_field: 时间字段名，默认为 "@timestamp"
        use_utc: 是否使用 UTC 时间（默认 True）。
                 - True: now/相对时间/时间戳/ISO解析结果将统一为 UTC tz-aware datetime
                 - False: 默认使用本地时间（datetime.now()），不强制附加时区
        now_func: 自定义获取当前时间的函数，主要用于测试。
                  默认为 None。
                  - use_utc=True 时默认使用 datetime.utcnow()
                  - use_utc=False 时默认使用 datetime.now()

    示例:
        >>> tool = TimeRangeQueryTool(time_field="@timestamp")
        >>> # 快速选择最近24小时
        >>> tr = tool.quick_range(QuickTimeRange.LAST_24_HOURS)
        >>> print(tr.to_dsl())
        >>> # 相对时间：最近30分钟
        >>> tr = tool.relative_range(30, "m")
        >>> # 绝对时间
        >>> tr = tool.absolute_range(start_time, end_time)
        >>> # 解析时间字符串
        >>> dt = tool.parse_time_string("now-1h")
    """

    # 匹配相对时间表达式的正则：now-<数字><单位>
    _RELATIVE_TIME_PATTERN = re.compile(r"^now-(\d+)([smhdw])$")

    # 支持的 ISO 8601 格式列表
    _ISO_FORMATS = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]

    def __init__(
        self,
        time_field: str = "@timestamp",
        use_utc: bool = True,
        now_func: Callable | None = None,
    ) -> None:
        self.time_field = time_field
        self.use_utc = use_utc
        self._now_func = now_func

    def _normalize_datetime(self, dt: datetime) -> datetime:
        """根据 use_utc 规范化 datetime.

        - use_utc=True：
          - naive datetime 视为 UTC，并附加 tzinfo=UTC
          - tz-aware datetime 转换为 UTC
        - use_utc=False：原样返回
        """
        if not self.use_utc:
            # 默认使用本地时区
            return dt.astimezone()

        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)

    def _now(self) -> datetime:
        """获取当前时间."""
        if self._now_func is not None:
            return self._normalize_datetime(self._now_func())

        if self.use_utc:
            return datetime.now(tz=UTC)

        return datetime.now()

    # ------------------------------------------------------------------
    # 快速时间范围
    # ------------------------------------------------------------------

    def quick_range(self, quick_range: QuickTimeRange) -> TimeRange:
        """根据快速时间范围选项构建 TimeRange.

        Args:
            quick_range: 快速时间范围选项

        Returns:
            TimeRange 对象

        Raises:
            ValueError: 不支持的快速时间范围选项
        """
        now = self._now()

        # 简单偏移类型：直接从映射表取 timedelta 参数
        if quick_range in _QUICK_RANGE_DELTA_MAP:
            delta_kwargs = _QUICK_RANGE_DELTA_MAP[quick_range]
            start = now - timedelta(**delta_kwargs)
            return TimeRange(
                start=start,
                end=now,
                field=self.time_field,
                range_type=TimeRangeType.QUICK,
            )

        # 需要特殊计算的类型
        handler = self._QUICK_RANGE_HANDLERS.get(quick_range)
        if handler is None:
            raise ValueError(f"不支持的快速时间范围选项: {quick_range}")
        return handler(self, now)

    def _handle_today(self, now: datetime) -> TimeRange:
        """处理 TODAY 选项."""
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        return TimeRange(
            start=start,
            end=now,
            field=self.time_field,
            range_type=TimeRangeType.QUICK,
        )

    def _handle_yesterday(self, now: datetime) -> TimeRange:
        """处理 YESTERDAY 选项."""
        yesterday = now - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
        end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
        return TimeRange(
            start=start,
            end=end,
            field=self.time_field,
            range_type=TimeRangeType.QUICK,
        )

    def _handle_this_week(self, now: datetime) -> TimeRange:
        """处理 THIS_WEEK 选项（周一为一周起始）."""
        start = now - timedelta(days=now.weekday())
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        return TimeRange(
            start=start,
            end=now,
            field=self.time_field,
            range_type=TimeRangeType.QUICK,
        )

    def _handle_last_week(self, now: datetime) -> TimeRange:
        """处理 LAST_WEEK 选项."""
        start = now - timedelta(days=now.weekday() + 7)
        start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(
            days=6, hours=23, minutes=59, seconds=59, microseconds=999999
        )
        return TimeRange(
            start=start,
            end=end,
            field=self.time_field,
            range_type=TimeRangeType.QUICK,
        )

    def _handle_this_month(self, now: datetime) -> TimeRange:
        """处理 THIS_MONTH 选项."""
        start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return TimeRange(
            start=start,
            end=now,
            field=self.time_field,
            range_type=TimeRangeType.QUICK,
        )

    def _handle_last_month(self, now: datetime) -> TimeRange:
        """处理 LAST_MONTH 选项."""
        # 上个月最后一天 = 本月1号 - 1天
        last_day_of_prev_month = now.replace(day=1) - timedelta(days=1)
        start = last_day_of_prev_month.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        end = last_day_of_prev_month.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )
        return TimeRange(
            start=start,
            end=end,
            field=self.time_field,
            range_type=TimeRangeType.QUICK,
        )

    # 特殊类型处理器映射
    _QUICK_RANGE_HANDLERS: dict[
        QuickTimeRange, Callable[[TimeRangeQueryTool, datetime], TimeRange]
    ] = {
        QuickTimeRange.TODAY: _handle_today,
        QuickTimeRange.YESTERDAY: _handle_yesterday,
        QuickTimeRange.THIS_WEEK: _handle_this_week,
        QuickTimeRange.LAST_WEEK: _handle_last_week,
        QuickTimeRange.THIS_MONTH: _handle_this_month,
        QuickTimeRange.LAST_MONTH: _handle_last_month,
    }

    # ------------------------------------------------------------------
    # 相对时间范围
    # ------------------------------------------------------------------

    def relative_range(self, value: int, unit: str = "m") -> TimeRange:
        """根据相对时间构建 TimeRange.

        Args:
            value: 时间值（正整数）
            unit: 时间单位（s=秒, m=分钟, h=小时, d=天, w=周）

        Returns:
            TimeRange 对象

        Raises:
            ValueError: 不支持的时间单位或无效的时间值
        """
        if unit not in RELATIVE_TIME_UNITS:
            supported = ", ".join(f"{k}={v}" for k, v in RELATIVE_TIME_UNITS.items())
            raise ValueError(f"不支持的时间单位: '{unit}'，支持的单位: {supported}")

        if value <= 0:
            raise ValueError(f"时间值必须为正整数，当前值: {value}")

        now = self._now()
        delta_kwargs = {RELATIVE_TIME_UNITS[unit]: value}
        start = now - timedelta(**delta_kwargs)

        return TimeRange(
            start=start,
            end=now,
            field=self.time_field,
            range_type=TimeRangeType.RELATIVE,
        )

    # ------------------------------------------------------------------
    # 绝对时间范围
    # ------------------------------------------------------------------

    def absolute_range(
        self,
        start: datetime,
        end: datetime,
    ) -> TimeRange:
        """根据绝对时间构建 TimeRange.

        Args:
            start: 开始时间
            end: 结束时间

        Returns:
            TimeRange 对象

        Raises:
            InvalidTimeRangeError: 开始时间大于结束时间
        """
        if start > end:
            raise InvalidTimeRangeError(
                f"开始时间不能晚于结束时间: start={start.isoformat()}, end={end.isoformat()}"
            )

        return TimeRange(
            start=self._normalize_datetime(start),
            end=self._normalize_datetime(end),
            field=self.time_field,
            range_type=TimeRangeType.ABSOLUTE,
        )

    # ------------------------------------------------------------------
    # 时间字符串解析
    # ------------------------------------------------------------------

    def parse_time_string(self, time_str: str) -> datetime:
        """解析时间字符串为 datetime 对象.

        支持以下格式:
        - 相对时间表达式: "now-1h", "now-30m", "now-7d"
        - "now": 当前时间
        - 时间戳（秒级或毫秒级）: "1704067200", "1704067200000"
        - ISO 8601 格式: "2024-01-01T00:00:00Z", "2024-01-01"

        Args:
            time_str: 时间字符串

        Returns:
            datetime 对象

        Raises:
            TimeRangeParseError: 无法解析时间字符串
        """
        if not time_str or not isinstance(time_str, str):
            raise TimeRangeParseError(f"无效的时间字符串: {time_str!r}")

        time_str = time_str.strip()
        now = self._now()

        # "now" 关键字
        if time_str.lower() == "now":
            return now

        # 相对时间表达式: now-<value><unit>
        match = self._RELATIVE_TIME_PATTERN.match(time_str.lower())
        if match:
            value = int(match.group(1))
            unit = match.group(2)
            delta_kwargs = {RELATIVE_TIME_UNITS[unit]: value}
            return now - timedelta(**delta_kwargs)

        # 纯数字 -> 时间戳
        if time_str.isdigit():
            return self._parse_timestamp(int(time_str))

        # ISO 8601 / 常见日期时间格式
        return self._parse_datetime_string(time_str)

    def parse_range_string(
        self,
        start_str: str,
        end_str: str,
    ) -> TimeRange:
        """解析时间字符串对，构建 TimeRange.

        Args:
            start_str: 开始时间字符串
            end_str: 结束时间字符串

        Returns:
            TimeRange 对象

        Raises:
            TimeRangeParseError: 无法解析时间字符串
            InvalidTimeRangeError: 开始时间大于结束时间
        """
        start = self.parse_time_string(start_str)
        end = self.parse_time_string(end_str)
        return self.absolute_range(start, end)

    # ------------------------------------------------------------------
    # 便捷 DSL 生成方法
    # ------------------------------------------------------------------

    def quick_range_dsl(self, quick_range: QuickTimeRange) -> dict[str, Any]:
        """快速生成时间范围 DSL（快捷方法）.

        Args:
            quick_range: 快速时间范围选项

        Returns:
            ES DSL range 查询字典
        """
        return self.quick_range(quick_range).to_dsl()

    def relative_range_dsl(self, value: int, unit: str = "m") -> dict[str, Any]:
        """快速生成相对时间范围 DSL（快捷方法）.

        Args:
            value: 时间值
            unit: 时间单位

        Returns:
            ES DSL range 查询字典
        """
        return self.relative_range(value, unit).to_dsl()

    # ------------------------------------------------------------------
    # 私有方法
    # ------------------------------------------------------------------

    def _parse_timestamp(self, timestamp: int) -> datetime:
        """解析时间戳.

        自动区分秒级和毫秒级时间戳。

        Args:
            timestamp: 时间戳

        Returns:
            datetime 对象

        Raises:
            TimeRangeParseError: 无效的时间戳
        """
        try:
            # 毫秒级时间戳（> 1e12 认为是毫秒）
            if timestamp > 1e12:
                timestamp = timestamp / 1000

            if self.use_utc:
                return datetime.fromtimestamp(timestamp, tz=UTC)
            return datetime.fromtimestamp(timestamp)
        except (ValueError, OSError, OverflowError) as e:
            raise TimeRangeParseError(f"无效的时间戳: {timestamp}，错误: {e}") from e

    def _parse_datetime_string(self, time_str: str) -> datetime:
        """尝试多种格式解析日期时间字符串.

        Args:
            time_str: 日期时间字符串

        Returns:
            datetime 对象

        Raises:
            TimeRangeParseError: 无法解析时间字符串
        """
        for fmt in self._ISO_FORMATS:
            try:
                dt = datetime.strptime(time_str, fmt)

                # 明确的 Z 后缀：无论 use_utc 是否开启，都应标记为 UTC
                if time_str.endswith("Z") and dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)

                return self._normalize_datetime(dt)
            except ValueError:
                continue

        raise TimeRangeParseError(
            f"无法解析时间字符串: '{time_str}'，"
            f"支持的格式: ISO 8601 (如 '2024-01-01T00:00:00Z')、"
            f"日期 (如 '2024-01-01')、时间戳、相对时间 (如 'now-1h')"
        )
