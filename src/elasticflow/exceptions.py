"""ES Query Toolkit 异常定义模块."""


class EsQueryToolkitError(Exception):
    """ES Query Toolkit 基础异常类."""

    pass


class QueryStringParseError(EsQueryToolkitError):
    """Query String 解析异常."""

    pass


class ConditionParseError(EsQueryToolkitError):
    """条件解析异常."""

    pass


class UnsupportedOperatorError(EsQueryToolkitError):
    """不支持的操作符异常."""

    pass
