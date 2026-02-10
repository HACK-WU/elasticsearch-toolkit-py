"""索引管理策略工具函数模块.

提供 ES 时间格式和大小格式的校验与解析功能。
"""

import re


# ES 时间格式正则：数字 + 时间单位（ms, s, m, h, d, w, M, y）
_TIME_PATTERN = re.compile(r"^(\d+)(ms|s|m|h|d|w|M|y)$")

# ES 大小格式正则：数字 + 大小单位（b, kb, mb, gb, tb, pb）不区分大小写
_SIZE_PATTERN = re.compile(r"^(\d+)(b|kb|mb|gb|tb|pb)$", re.IGNORECASE)

# 时间单位到秒的转换映射
_TIME_UNIT_TO_SECONDS: dict[str, int] = {
    "ms": 0,  # 毫秒转秒时取整为0（不足1秒）
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "M": 2592000,  # 按30天算
    "y": 31536000,  # 按365天算
}


def validate_time_format(value: str) -> bool:
    """校验值是否符合 ES 时间格式.

    支持的时间单位：ms（毫秒）、s（秒）、m（分钟）、h（小时）、
    d（天）、w（周）、M（月）、y（年）。

    Args:
        value: 待校验的时间格式字符串，如 "30d", "1h", "7d", "1M", "0ms"

    Returns:
        True 表示格式合法，False 表示格式不合法

    Examples:
        >>> validate_time_format("30d")
        True
        >>> validate_time_format("1h")
        True
        >>> validate_time_format("abc")
        False
        >>> validate_time_format("")
        False
    """
    if not isinstance(value, str) or not value:
        return False
    return _TIME_PATTERN.match(value) is not None


def validate_size_format(value: str) -> bool:
    """校验值是否符合 ES 大小格式.

    支持的大小单位（不区分大小写）：b（字节）、kb（千字节）、
    mb（兆字节）、gb（吉字节）、tb（太字节）、pb（拍字节）。

    Args:
        value: 待校验的大小格式字符串，如 "10GB", "500MB", "1TB"

    Returns:
        True 表示格式合法，False 表示格式不合法

    Examples:
        >>> validate_size_format("10GB")
        True
        >>> validate_size_format("500mb")
        True
        >>> validate_size_format("abc")
        False
        >>> validate_size_format("")
        False
    """
    if not isinstance(value, str) or not value:
        return False
    return _SIZE_PATTERN.match(value) is not None


def parse_time_to_seconds(value: str) -> int:
    """将 ES 时间格式转换为秒数.

    用于比较索引创建时间与保留时间。毫秒（ms）精度会被取整。

    Args:
        value: ES 时间格式字符串，如 "30d", "1h", "7d"

    Returns:
        对应的秒数

    Raises:
        ValueError: 当时间格式不合法时抛出

    Examples:
        >>> parse_time_to_seconds("1d")
        86400
        >>> parse_time_to_seconds("1h")
        3600
        >>> parse_time_to_seconds("500ms")
        0
    """
    if not validate_time_format(value):
        raise ValueError(f"不合法的 ES 时间格式: {value!r}")

    match = _TIME_PATTERN.match(value)
    # validate_time_format 已确保 match 不为 None
    assert match is not None
    amount = int(match.group(1))
    unit = match.group(2)

    if unit == "ms":
        # 毫秒转秒，取整
        return amount // 1000

    return amount * _TIME_UNIT_TO_SECONDS[unit]
