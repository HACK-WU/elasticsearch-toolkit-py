"""批量操作工具异常定义模块."""

from ..exceptions import EsQueryToolkitError


class BulkOperationError(EsQueryToolkitError):
    """批量操作基础异常类."""

    pass


class BulkProcessingError(BulkOperationError):
    """批量处理过程中的异常."""

    pass


class BulkRetryExhaustedError(BulkOperationError):
    """批量操作重试次数耗尽异常."""

    pass


class BulkValidationError(BulkOperationError):
    """批量操作验证异常."""

    pass
