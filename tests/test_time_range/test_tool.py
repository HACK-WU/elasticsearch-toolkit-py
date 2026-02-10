"""时间范围查询工具单元测试."""

import unittest
from datetime import datetime, timedelta, UTC

from elasticflow.time_range import (
    TimeRangeQueryTool,
    TimeRange,
    TimeRangeType,
    QuickTimeRange,
)
from elasticflow.time_range.exceptions import (
    TimeRangeParseError,
    InvalidTimeRangeError,
)


class TestTimeRange(unittest.TestCase):
    """TimeRange 数据类单元测试."""

    def test_to_dsl(self):
        """测试转换为 DSL."""
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 31, 23, 59, 59)
        tr = TimeRange(start=start, end=end, field="@timestamp")
        dsl = tr.to_dsl()

        self.assertIn("range", dsl)
        self.assertIn("@timestamp", dsl["range"])
        self.assertEqual(dsl["range"]["@timestamp"]["gte"], start.isoformat())
        self.assertEqual(dsl["range"]["@timestamp"]["lte"], end.isoformat())
        self.assertEqual(
            dsl["range"]["@timestamp"]["format"],
            "strict_date_optional_time||epoch_millis",
        )

    def test_to_filter(self):
        """测试转换为 filter 子句."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        tr = TimeRange(start=start, end=end, field="timestamp")
        f = tr.to_filter()

        self.assertIn("timestamp", f)
        self.assertEqual(f["timestamp"]["gte"], start.isoformat())
        self.assertEqual(f["timestamp"]["lte"], end.isoformat())

    def test_custom_field(self):
        """测试自定义时间字段."""
        tr = TimeRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
            field="create_time",
        )
        dsl = tr.to_dsl()
        self.assertIn("create_time", dsl["range"])

    def test_duration_seconds(self):
        """测试时间范围持续时间计算."""
        start = datetime(2024, 1, 1, 0, 0, 0)
        end = datetime(2024, 1, 1, 1, 0, 0)
        tr = TimeRange(start=start, end=end)
        self.assertEqual(tr.duration_seconds, 3600.0)

    def test_repr(self):
        """测试字符串表示."""
        tr = TimeRange(
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
            field="@timestamp",
            range_type=TimeRangeType.ABSOLUTE,
        )
        repr_str = repr(tr)
        self.assertIn("TimeRange", repr_str)
        self.assertIn("@timestamp", repr_str)
        self.assertIn("absolute", repr_str)


class TestTimeRangeQueryToolInit(unittest.TestCase):
    """TimeRangeQueryTool 初始化测试."""

    def test_default_field(self):
        """测试默认时间字段."""
        tool = TimeRangeQueryTool()
        self.assertEqual(tool.time_field, "@timestamp")

    def test_custom_field(self):
        """测试自定义时间字段."""
        tool = TimeRangeQueryTool(time_field="created_at")
        self.assertEqual(tool.time_field, "created_at")

    def test_custom_now_func(self):
        """测试自定义 now 函数."""
        fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(now_func=lambda: fixed_now)
        self.assertEqual(tool._now(), fixed_now)


class TestQuickRange(unittest.TestCase):
    """快速时间范围测试."""

    def setUp(self):
        """设置测试环境，使用固定时间."""
        # 固定时间：2024-06-15 周六 12:00:00 UTC
        self.fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        self.tool = TimeRangeQueryTool(
            time_field="@timestamp",
            now_func=lambda: self.fixed_now,
        )

    def test_last_15_minutes(self):
        """测试最近15分钟."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_15_MINUTES)
        expected_start = self.fixed_now - timedelta(minutes=15)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)
        self.assertEqual(tr.range_type, TimeRangeType.QUICK)

    def test_last_30_minutes(self):
        """测试最近30分钟."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_30_MINUTES)
        expected_start = self.fixed_now - timedelta(minutes=30)
        self.assertEqual(tr.start, expected_start)

    def test_last_1_hour(self):
        """测试最近1小时."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_1_HOUR)
        expected_start = self.fixed_now - timedelta(hours=1)
        self.assertEqual(tr.start, expected_start)

    def test_last_3_hours(self):
        """测试最近3小时."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_3_HOURS)
        expected_start = self.fixed_now - timedelta(hours=3)
        self.assertEqual(tr.start, expected_start)

    def test_last_6_hours(self):
        """测试最近6小时."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_6_HOURS)
        expected_start = self.fixed_now - timedelta(hours=6)
        self.assertEqual(tr.start, expected_start)

    def test_last_12_hours(self):
        """测试最近12小时."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_12_HOURS)
        expected_start = self.fixed_now - timedelta(hours=12)
        self.assertEqual(tr.start, expected_start)

    def test_last_24_hours(self):
        """测试最近24小时."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_24_HOURS)
        expected_start = self.fixed_now - timedelta(hours=24)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)

    def test_last_7_days(self):
        """测试最近7天."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_7_DAYS)
        expected_start = self.fixed_now - timedelta(days=7)
        self.assertEqual(tr.start, expected_start)

    def test_last_30_days(self):
        """测试最近30天."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_30_DAYS)
        expected_start = self.fixed_now - timedelta(days=30)
        self.assertEqual(tr.start, expected_start)

    def test_today(self):
        """测试今天."""
        tr = self.tool.quick_range(QuickTimeRange.TODAY)
        expected_start = datetime(2024, 6, 15, 0, 0, 0, tzinfo=UTC)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)

    def test_yesterday(self):
        """测试昨天."""
        tr = self.tool.quick_range(QuickTimeRange.YESTERDAY)
        expected_start = datetime(2024, 6, 14, 0, 0, 0, tzinfo=UTC)
        expected_end = datetime(2024, 6, 14, 23, 59, 59, 999999, tzinfo=UTC)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, expected_end)

    def test_this_week(self):
        """测试本周（2024-06-15 是周六，weekday=5，周一为起始）."""
        tr = self.tool.quick_range(QuickTimeRange.THIS_WEEK)
        # 周六 weekday=5，本周一 = 6月15日 - 5天 = 6月10日
        expected_start = datetime(2024, 6, 10, 0, 0, 0, tzinfo=UTC)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)

    def test_last_week(self):
        """测试上周."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_WEEK)
        # 上周一 = 6月10日 - 7天 = 6月3日
        expected_start = datetime(2024, 6, 3, 0, 0, 0, tzinfo=UTC)
        # 上周日 = 6月3日 + 6天23:59:59.999999 = 6月9日
        expected_end = datetime(2024, 6, 9, 23, 59, 59, 999999, tzinfo=UTC)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, expected_end)

    def test_this_month(self):
        """测试本月."""
        tr = self.tool.quick_range(QuickTimeRange.THIS_MONTH)
        expected_start = datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)

    def test_last_month(self):
        """测试上月."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_MONTH)
        expected_start = datetime(2024, 5, 1, 0, 0, 0, tzinfo=UTC)
        expected_end = datetime(2024, 5, 31, 23, 59, 59, 999999, tzinfo=UTC)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, expected_end)

    def test_quick_range_dsl_type(self):
        """测试快速范围返回的 range_type 为 QUICK."""
        tr = self.tool.quick_range(QuickTimeRange.LAST_1_HOUR)
        self.assertEqual(tr.range_type, TimeRangeType.QUICK)

    def test_quick_range_uses_custom_field(self):
        """测试快速范围使用自定义字段."""
        tool = TimeRangeQueryTool(
            time_field="created_at",
            now_func=lambda: self.fixed_now,
        )
        tr = tool.quick_range(QuickTimeRange.LAST_1_HOUR)
        self.assertEqual(tr.field, "created_at")
        dsl = tr.to_dsl()
        self.assertIn("created_at", dsl["range"])


class TestRelativeRange(unittest.TestCase):
    """相对时间范围测试."""

    def setUp(self):
        """设置测试环境."""
        self.fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        self.tool = TimeRangeQueryTool(
            now_func=lambda: self.fixed_now,
        )

    def test_seconds(self):
        """测试秒级相对时间."""
        tr = self.tool.relative_range(30, "s")
        expected_start = self.fixed_now - timedelta(seconds=30)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)

    def test_minutes(self):
        """测试分钟级相对时间."""
        tr = self.tool.relative_range(15, "m")
        expected_start = self.fixed_now - timedelta(minutes=15)
        self.assertEqual(tr.start, expected_start)

    def test_hours(self):
        """测试小时级相对时间."""
        tr = self.tool.relative_range(2, "h")
        expected_start = self.fixed_now - timedelta(hours=2)
        self.assertEqual(tr.start, expected_start)

    def test_days(self):
        """测试天级相对时间."""
        tr = self.tool.relative_range(7, "d")
        expected_start = self.fixed_now - timedelta(days=7)
        self.assertEqual(tr.start, expected_start)

    def test_weeks(self):
        """测试周级相对时间."""
        tr = self.tool.relative_range(2, "w")
        expected_start = self.fixed_now - timedelta(weeks=2)
        self.assertEqual(tr.start, expected_start)

    def test_default_unit_is_minutes(self):
        """测试默认单位为分钟."""
        tr = self.tool.relative_range(10)
        expected_start = self.fixed_now - timedelta(minutes=10)
        self.assertEqual(tr.start, expected_start)

    def test_range_type_is_relative(self):
        """测试返回的 range_type 为 RELATIVE."""
        tr = self.tool.relative_range(30, "m")
        self.assertEqual(tr.range_type, TimeRangeType.RELATIVE)

    def test_invalid_unit(self):
        """测试不支持的时间单位."""
        with self.assertRaises(ValueError) as ctx:
            self.tool.relative_range(10, "x")
        self.assertIn("不支持的时间单位", str(ctx.exception))

    def test_zero_value(self):
        """测试时间值为零."""
        with self.assertRaises(ValueError) as ctx:
            self.tool.relative_range(0, "m")
        self.assertIn("正整数", str(ctx.exception))

    def test_negative_value(self):
        """测试时间值为负数."""
        with self.assertRaises(ValueError):
            self.tool.relative_range(-5, "h")


class TestAbsoluteRange(unittest.TestCase):
    """绝对时间范围测试."""

    def setUp(self):
        """设置测试环境."""
        self.tool = TimeRangeQueryTool()

    def test_valid_range(self):
        """测试有效的绝对时间范围."""
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        tr = self.tool.absolute_range(start, end)

        self.assertEqual(tr.start, start)
        self.assertEqual(tr.end, end)
        self.assertEqual(tr.range_type, TimeRangeType.ABSOLUTE)

    def test_same_start_end(self):
        """测试起止时间相同."""
        same_time = datetime(2024, 1, 1, 12, 0, 0)
        tr = self.tool.absolute_range(same_time, same_time)
        self.assertEqual(tr.start, same_time)
        self.assertEqual(tr.end, same_time)

    def test_start_after_end(self):
        """测试开始时间晚于结束时间应抛异常."""
        start = datetime(2024, 2, 1)
        end = datetime(2024, 1, 1)
        with self.assertRaises(InvalidTimeRangeError) as ctx:
            self.tool.absolute_range(start, end)
        self.assertIn("开始时间不能晚于结束时间", str(ctx.exception))


class TestParseTimeString(unittest.TestCase):
    """时间字符串解析测试."""

    def setUp(self):
        """设置测试环境."""
        self.fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        self.tool = TimeRangeQueryTool(
            now_func=lambda: self.fixed_now,
        )

    def test_now(self):
        """测试 'now' 关键字."""
        result = self.tool.parse_time_string("now")
        self.assertEqual(result, self.fixed_now)

    def test_now_case_insensitive(self):
        """测试 'NOW' 大小写不敏感."""
        result = self.tool.parse_time_string("NOW")
        self.assertEqual(result, self.fixed_now)

    def test_relative_hours(self):
        """测试相对时间表达式 - 小时."""
        result = self.tool.parse_time_string("now-1h")
        expected = self.fixed_now - timedelta(hours=1)
        self.assertEqual(result, expected)

    def test_relative_minutes(self):
        """测试相对时间表达式 - 分钟."""
        result = self.tool.parse_time_string("now-30m")
        expected = self.fixed_now - timedelta(minutes=30)
        self.assertEqual(result, expected)

    def test_relative_seconds(self):
        """测试相对时间表达式 - 秒."""
        result = self.tool.parse_time_string("now-45s")
        expected = self.fixed_now - timedelta(seconds=45)
        self.assertEqual(result, expected)

    def test_relative_days(self):
        """测试相对时间表达式 - 天."""
        result = self.tool.parse_time_string("now-7d")
        expected = self.fixed_now - timedelta(days=7)
        self.assertEqual(result, expected)

    def test_relative_weeks(self):
        """测试相对时间表达式 - 周."""
        result = self.tool.parse_time_string("now-2w")
        expected = self.fixed_now - timedelta(weeks=2)
        self.assertEqual(result, expected)

    def test_timestamp_seconds(self):
        """测试秒级时间戳."""
        ts = 1704067200  # 2024-01-01T00:00:00Z
        result = self.tool.parse_time_string(str(ts))
        expected = datetime.fromtimestamp(ts, tz=UTC)
        self.assertEqual(result, expected)

    def test_timestamp_milliseconds(self):
        """测试毫秒级时间戳."""
        ts_ms = 1704067200000
        result = self.tool.parse_time_string(str(ts_ms))
        expected = datetime.fromtimestamp(ts_ms / 1000, tz=UTC)
        self.assertEqual(result, expected)

    def test_iso_8601_with_z(self):
        """测试 ISO 8601 格式带 Z."""
        result = self.tool.parse_time_string("2024-01-01T00:00:00Z")
        expected = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test_iso_8601_without_z(self):
        """测试 ISO 8601 格式不带 Z."""
        result = self.tool.parse_time_string("2024-01-01T12:30:45")
        expected = datetime(2024, 1, 1, 12, 30, 45, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test_iso_8601_with_microseconds(self):
        """测试 ISO 8601 格式带微秒."""
        result = self.tool.parse_time_string("2024-01-01T12:30:45.123456")
        expected = datetime(2024, 1, 1, 12, 30, 45, 123456, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test_date_only(self):
        """测试纯日期格式."""
        result = self.tool.parse_time_string("2024-01-01")
        expected = datetime(2024, 1, 1, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test_datetime_with_space(self):
        """测试空格分隔的日期时间."""
        result = self.tool.parse_time_string("2024-01-01 12:30:45")
        expected = datetime(2024, 1, 1, 12, 30, 45, tzinfo=UTC)
        self.assertEqual(result, expected)

    def test_empty_string(self):
        """测试空字符串."""
        with self.assertRaises(TimeRangeParseError):
            self.tool.parse_time_string("")

    def test_none_value(self):
        """测试 None 值."""
        with self.assertRaises(TimeRangeParseError):
            self.tool.parse_time_string(None)

    def test_invalid_string(self):
        """测试无效字符串."""
        with self.assertRaises(TimeRangeParseError):
            self.tool.parse_time_string("not-a-date")

    def test_whitespace_handling(self):
        """测试前后空格处理."""
        result = self.tool.parse_time_string("  now  ")
        self.assertEqual(result, self.fixed_now)


class TestParseRangeString(unittest.TestCase):
    """时间范围字符串对解析测试."""

    def setUp(self):
        """设置测试环境."""
        self.fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        self.tool = TimeRangeQueryTool(
            now_func=lambda: self.fixed_now,
        )

    def test_parse_range_with_relative_time(self):
        """测试使用相对时间表达式构建范围."""
        tr = self.tool.parse_range_string("now-24h", "now")
        expected_start = self.fixed_now - timedelta(hours=24)
        self.assertEqual(tr.start, expected_start)
        self.assertEqual(tr.end, self.fixed_now)

    def test_parse_range_with_absolute_time(self):
        """测试使用绝对时间字符串构建范围."""
        tr = self.tool.parse_range_string("2024-01-01", "2024-01-31")
        self.assertEqual(tr.start, datetime(2024, 1, 1, tzinfo=UTC))
        self.assertEqual(tr.end, datetime(2024, 1, 31, tzinfo=UTC))

    def test_parse_range_invalid_order(self):
        """测试开始时间晚于结束时间."""
        with self.assertRaises(InvalidTimeRangeError):
            self.tool.parse_range_string("2024-02-01", "2024-01-01")


class TestConvenienceDslMethods(unittest.TestCase):
    """便捷 DSL 生成方法测试."""

    def setUp(self):
        """设置测试环境."""
        self.fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        self.tool = TimeRangeQueryTool(
            now_func=lambda: self.fixed_now,
        )

    def test_quick_range_dsl(self):
        """测试 quick_range_dsl 快捷方法."""
        dsl = self.tool.quick_range_dsl(QuickTimeRange.LAST_1_HOUR)
        self.assertIn("range", dsl)
        self.assertIn("@timestamp", dsl["range"])

    def test_relative_range_dsl(self):
        """测试 relative_range_dsl 快捷方法."""
        dsl = self.tool.relative_range_dsl(30, "m")
        self.assertIn("range", dsl)
        expected_start = self.fixed_now - timedelta(minutes=30)
        self.assertEqual(
            dsl["range"]["@timestamp"]["gte"],
            expected_start.isoformat(),
        )


class TestEdgeCases(unittest.TestCase):
    """边界情况测试."""

    def test_last_month_january(self):
        """测试1月份时获取上月（12月）."""
        fixed_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(now_func=lambda: fixed_now)
        tr = tool.quick_range(QuickTimeRange.LAST_MONTH)
        self.assertEqual(tr.start, datetime(2023, 12, 1, 0, 0, 0, tzinfo=UTC))
        self.assertEqual(tr.end, datetime(2023, 12, 31, 23, 59, 59, 999999, tzinfo=UTC))

    def test_last_month_march_leap_year(self):
        """测试闰年3月获取上月（2月有29天）."""
        fixed_now = datetime(2024, 3, 15, 12, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(now_func=lambda: fixed_now)
        tr = tool.quick_range(QuickTimeRange.LAST_MONTH)
        self.assertEqual(tr.start, datetime(2024, 2, 1, 0, 0, 0, tzinfo=UTC))
        self.assertEqual(tr.end, datetime(2024, 2, 29, 23, 59, 59, 999999, tzinfo=UTC))

    def test_this_week_on_monday(self):
        """测试周一时获取本周（起始即为当天）."""
        # 2024-06-10 是周一
        fixed_now = datetime(2024, 6, 10, 8, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(now_func=lambda: fixed_now)
        tr = tool.quick_range(QuickTimeRange.THIS_WEEK)
        self.assertEqual(tr.start, datetime(2024, 6, 10, 0, 0, 0, tzinfo=UTC))

    def test_today_at_midnight(self):
        """测试零点时获取今天."""
        fixed_now = datetime(2024, 6, 15, 0, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(now_func=lambda: fixed_now)
        tr = tool.quick_range(QuickTimeRange.TODAY)
        self.assertEqual(tr.start, fixed_now)
        self.assertEqual(tr.end, fixed_now)

    def test_large_relative_range(self):
        """测试较大的相对时间范围."""
        fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(now_func=lambda: fixed_now)
        tr = tool.relative_range(365, "d")
        expected_start = fixed_now - timedelta(days=365)
        self.assertEqual(tr.start, expected_start)


class TestIntegration(unittest.TestCase):
    """集成测试 - 模拟实际使用场景."""

    def test_import_from_main_package(self):
        """测试从主包导入."""
        from elasticflow import TimeRangeQueryTool, TimeRange, QuickTimeRange

        tool = TimeRangeQueryTool()
        tr = tool.quick_range(QuickTimeRange.LAST_1_HOUR)
        self.assertIsInstance(tr, TimeRange)

    def test_full_workflow(self):
        """测试完整工作流：创建工具 -> 构建范围 -> 生成 DSL."""
        fixed_now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        tool = TimeRangeQueryTool(
            time_field="timestamp",
            now_func=lambda: fixed_now,
        )

        # 快速范围
        tr = tool.quick_range(QuickTimeRange.LAST_24_HOURS)
        dsl = tr.to_dsl()
        self.assertIn("range", dsl)
        self.assertIn("timestamp", dsl["range"])

        # 相对范围
        tr = tool.relative_range(30, "m")
        dsl = tr.to_dsl()
        self.assertEqual(
            dsl["range"]["timestamp"]["gte"],
            (fixed_now - timedelta(minutes=30)).isoformat(),
        )

        # 绝对范围
        start = datetime(2024, 1, 1, tzinfo=UTC)
        end = datetime(2024, 6, 30, tzinfo=UTC)
        tr = tool.absolute_range(start, end)
        dsl = tr.to_dsl()
        self.assertEqual(dsl["range"]["timestamp"]["gte"], start.isoformat())
        self.assertEqual(dsl["range"]["timestamp"]["lte"], end.isoformat())

        # 字符串解析范围
        tr = tool.parse_range_string("now-7d", "now")
        self.assertEqual(tr.start, fixed_now - timedelta(days=7))
        self.assertEqual(tr.end, fixed_now)


if __name__ == "__main__":
    unittest.main()
