from ..exceptions import EsQueryToolkitError


class QueryAnalyzerError(EsQueryToolkitError):
    """查询分析器基础异常"""

    pass


class QueryValidationError(QueryAnalyzerError):
    """查询验证异常"""

    pass


class QueryProfileError(QueryAnalyzerError):
    """查询性能剖析异常"""

    pass


class SlowQueryLogNotConfiguredError(QueryAnalyzerError):
    """慢查询日志未配置异常"""

    pass
