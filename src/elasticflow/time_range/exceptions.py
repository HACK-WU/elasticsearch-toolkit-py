"""时间范围查询工具异常定义模块."""

from elasticflow.exceptions import EsQueryToolkitError


class TimeRangeError(EsQueryToolkitError):
    """时间范围查询工具基础异常."""

    pass


class TimeRangeParseError(TimeRangeError):
    """时间字符串解析异常."""

    pass


class InvalidTimeRangeError(TimeRangeError):
    """无效的时间范围异常（如 start > end）."""

    pass
