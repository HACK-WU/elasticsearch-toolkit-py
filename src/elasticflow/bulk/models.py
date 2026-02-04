"""批量操作工具数据模型定义模块."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class BulkAction(Enum):
    """批量操作类型枚举."""

    INDEX = "index"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    UPSERT = "upsert"


@dataclass
class BulkOperation:
    """批量操作项数据类.

    Attributes:
        action: 操作类型
        index_name: 索引名称
        doc_id: 文档ID（可选，对于INDEX操作如果不指定则自动生成）
        source: 文档源数据（用于INDEX、CREATE、UPDATE操作）
        routing: 路由信息（可选）
        retry_on_conflict: 冲突重试次数（用于UPDATE操作）
        metadata: 额外的元数据
    """

    action: BulkAction
    index_name: str
    doc_id: str | None = None
    source: dict[str, Any] | None = None
    routing: str | None = None
    retry_on_conflict: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BulkErrorItem:
    """批量操作错误项数据类.

    Attributes:
        index_name: 索引名称
        doc_id: 文档ID
        error_type: 错误类型
        error_reason: 错误原因
        status: HTTP状态码
        caused_by: 根本原因
        operation: 失败的操作类型
    """

    index_name: str
    doc_id: str | None
    error_type: str
    error_reason: str
    status: int
    caused_by: str | None = None
    operation: BulkAction | None = None


@dataclass
class BulkResult:
    """批量操作结果数据类.

    Attributes:
        total: 总操作数
        success: 成功数
        failed: 失败数
        created: 创建数（用于UPSERT操作）
        updated: 更新数（用于UPSERT操作）
        deleted: 删除数
        errors: 错误详情列表
        took: 总耗时（秒）
        batch_count: 批次数
        warnings: 警告信息列表
    """

    total: int = 0
    success: int = 0
    failed: int = 0
    created: int = 0
    updated: int = 0
    deleted: int = 0
    errors: list[BulkErrorItem] = field(default_factory=list)
    took: float = 0.0
    batch_count: int = 0
    warnings: list[str] = field(default_factory=list)

    def is_success(self) -> bool:
        """判断操作是否全部成功."""
        return self.failed == 0

    def add_error(
        self,
        index_name: str,
        doc_id: str | None,
        error_type: str,
        error_reason: str,
        status: int,
        caused_by: str | None = None,
        operation: BulkAction | None = None,
    ) -> None:
        """添加错误项."""
        error_item = BulkErrorItem(
            index_name=index_name,
            doc_id=doc_id,
            error_type=error_type,
            error_reason=error_reason,
            status=status,
            caused_by=caused_by,
            operation=operation,
        )
        self.errors.append(error_item)

    def add_warning(self, warning: str) -> None:
        """添加警告信息."""
        self.warnings.append(warning)

    def get_error_summary(self) -> str:
        """获取错误摘要."""
        if not self.errors:
            return "No errors"
        summary = f"Total errors: {len(self.errors)}\n"
        for i, error in enumerate(self.errors[:10], 1):  # 只显示前10个错误
            summary += (
                f"{i}. [{error.operation.value if error.operation else 'unknown'}] "
                f"Index: {error.index_name}, DocID: {error.doc_id}, "
                f"Status: {error.status}, Reason: {error.error_reason}\n"
            )
        if len(self.errors) > 10:
            summary += f"... and {len(self.errors) - 10} more errors\n"
        return summary
