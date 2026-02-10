"""策略工具函数单元测试."""

import pytest

from elasticflow.index_manager.policies.utils import (
    parse_time_to_seconds,
    validate_size_format,
    validate_time_format,
)


class TestValidateTimeFormat:
    """validate_time_format 函数测试."""

    @pytest.mark.parametrize(
        "value",
        [
            "30d",
            "1d",
            "7d",
            "1h",
            "1m",
            "1s",
            "1w",
            "1M",
            "1y",
            "0ms",
            "500ms",
            "100s",
            "24h",
        ],
    )
    def test_valid_time_formats(self, value: str) -> None:
        """测试合法的 ES 时间格式."""
        assert validate_time_format(value) is True

    @pytest.mark.parametrize(
        "value",
        ["", "abc", "30", "d", "30x", "30 d", "-1d", "1.5d", "30D", "30W"],
    )
    def test_invalid_time_formats(self, value: str) -> None:
        """测试不合法的时间格式."""
        assert validate_time_format(value) is False

    def test_non_string_input(self) -> None:
        """测试非字符串输入."""
        assert validate_time_format(None) is False  # type: ignore
        assert validate_time_format(123) is False  # type: ignore
        assert validate_time_format([]) is False  # type: ignore

    def test_boundary_zero_values(self) -> None:
        """测试零值边界."""
        assert validate_time_format("0d") is True
        assert validate_time_format("0s") is True
        assert validate_time_format("0ms") is True

    def test_large_numbers(self) -> None:
        """测试大数值."""
        assert validate_time_format("999999d") is True
        assert validate_time_format("1000000h") is True


class TestValidateSizeFormat:
    """validate_size_format 函数测试."""

    @pytest.mark.parametrize(
        "value",
        ["10GB", "500MB", "1TB", "100b", "50kb", "10gb", "500mb", "1tb", "1PB", "0b"],
    )
    def test_valid_size_formats(self, value: str) -> None:
        """测试合法的 ES 大小格式."""
        assert validate_size_format(value) is True

    @pytest.mark.parametrize(
        "value",
        ["", "abc", "10", "GB", "10 GB", "-10GB", "1.5GB", "10GiB", "10MB "],
    )
    def test_invalid_size_formats(self, value: str) -> None:
        """测试不合法的大小格式."""
        assert validate_size_format(value) is False

    def test_non_string_input(self) -> None:
        """测试非字符串输入."""
        assert validate_size_format(None) is False  # type: ignore
        assert validate_size_format(123) is False  # type: ignore

    def test_case_insensitive(self) -> None:
        """测试大小写不敏感."""
        assert validate_size_format("10gb") is True
        assert validate_size_format("10GB") is True
        assert validate_size_format("10Gb") is True

    def test_large_numbers(self) -> None:
        """测试大数值."""
        assert validate_size_format("999999GB") is True


class TestParseTimeToSeconds:
    """parse_time_to_seconds 函数测试."""

    def test_seconds(self) -> None:
        """测试秒转换."""
        assert parse_time_to_seconds("1s") == 1
        assert parse_time_to_seconds("60s") == 60
        assert parse_time_to_seconds("0s") == 0

    def test_minutes(self) -> None:
        """测试分钟转换."""
        assert parse_time_to_seconds("1m") == 60
        assert parse_time_to_seconds("5m") == 300

    def test_hours(self) -> None:
        """测试小时转换."""
        assert parse_time_to_seconds("1h") == 3600
        assert parse_time_to_seconds("24h") == 86400

    def test_days(self) -> None:
        """测试天数转换."""
        assert parse_time_to_seconds("1d") == 86400
        assert parse_time_to_seconds("30d") == 2592000

    def test_weeks(self) -> None:
        """测试周转换."""
        assert parse_time_to_seconds("1w") == 604800
        assert parse_time_to_seconds("2w") == 1209600

    def test_months(self) -> None:
        """测试月份转换（按30天）."""
        assert parse_time_to_seconds("1M") == 2592000
        assert parse_time_to_seconds("3M") == 7776000

    def test_years(self) -> None:
        """测试年份转换（按365天）."""
        assert parse_time_to_seconds("1y") == 31536000

    def test_milliseconds(self) -> None:
        """测试毫秒转换（取整为秒）."""
        assert parse_time_to_seconds("0ms") == 0
        assert parse_time_to_seconds("500ms") == 0
        assert parse_time_to_seconds("1000ms") == 1
        assert parse_time_to_seconds("1500ms") == 1
        assert parse_time_to_seconds("2000ms") == 2

    def test_invalid_format_raises_error(self) -> None:
        """测试不合法格式抛出 ValueError."""
        with pytest.raises(ValueError, match="不合法的 ES 时间格式"):
            parse_time_to_seconds("abc")
        with pytest.raises(ValueError, match="不合法的 ES 时间格式"):
            parse_time_to_seconds("")
        with pytest.raises(ValueError, match="不合法的 ES 时间格式"):
            parse_time_to_seconds("30")

    def test_zero_values(self) -> None:
        """测试零值."""
        assert parse_time_to_seconds("0d") == 0
        assert parse_time_to_seconds("0h") == 0
        assert parse_time_to_seconds("0m") == 0
