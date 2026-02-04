"""批量操作工具模块.

该模块提供了高效的 Elasticsearch 批量操作功能，包括：
- 批量索引、创建、更新、删除
- UPSERT 操作（存在则更新，不存在则创建）
- 自动分批处理
- 失败重试机制
- 流式处理支持（适用于超大数据量）

示例用法:
    >>> from elasticflow.bulk import BulkOperationTool, BulkAction
    >>> bulk_tool = BulkOperationTool(es_client)
    >>> documents = [{"name": "Alice"}, {"name": "Bob"}]
    >>> result = bulk_tool.bulk_index("users", documents)
    >>> print(f"成功: {result.success}, 失败: {result.failed}")
"""

from .models import (
    BulkAction,
    BulkErrorItem,
    BulkOperation,
    BulkResult,
)
from .tool import BulkOperationTool
from .exceptions import (
    BulkOperationError,
    BulkProcessingError,
    BulkRetryExhaustedError,
    BulkValidationError,
)

__all__ = [
    "BulkAction",
    "BulkErrorItem",
    "BulkOperation",
    "BulkResult",
    "BulkOperationTool",
    "BulkOperationError",
    "BulkProcessingError",
    "BulkRetryExhaustedError",
    "BulkValidationError",
]
