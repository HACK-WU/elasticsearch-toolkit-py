"""索引管理策略数据模型定义模块.

提供各种索引管理策略的 dataclass 模型定义，包括：
- TimeBasedRolloverPolicy: 基于时间的滚动策略
- SizeBasedRolloverPolicy: 基于大小的滚动策略
- LifecyclePhase: 生命周期阶段
- IndexLifecyclePolicy: 索引生命周期管理策略
- ShrinkPolicy: 索引压缩策略
- ArchivePolicy: 索引归档策略
- CleanupPolicy: 索引清理策略
"""

from dataclasses import dataclass, field
from typing import Any
from collections.abc import Callable

from .exceptions import PolicyValidationError
from .utils import parse_time_to_seconds, validate_size_format, validate_time_format


@dataclass
class TimeBasedRolloverPolicy:
    """基于时间的滚动策略模型.

    通过配置时间间隔和保留时长来自动滚动日志索引，
    自动清理超过保留时间的过期索引。

    Attributes:
        interval: 滚动间隔，ES 时间格式（如 "1d", "1w", "1M"）
        max_age: 最大保留时间，ES 时间格式（如 "30d"）
        alias: 索引别名（必需，不可为空）
        index_pattern: 索引命名模式（如 "logs-{now/d}-000001"）

    Raises:
        PolicyValidationError: 当参数校验失败时抛出

    Examples:
        >>> policy = TimeBasedRolloverPolicy(
        ...     interval="1d",
        ...     max_age="30d",
        ...     alias="logs",
        ...     index_pattern="logs-*",
        ... )
    """

    interval: str
    max_age: str
    alias: str
    index_pattern: str = ""

    def __post_init__(self) -> None:
        """校验策略参数合法性."""
        if not self.alias:
            raise PolicyValidationError("alias 不能为空")
        if not validate_time_format(self.interval):
            raise PolicyValidationError(
                f"interval 格式不合法: {self.interval!r}，"
                "应为 ES 时间格式（如 '1d', '1w', '1M'）"
            )
        if not validate_time_format(self.max_age):
            raise PolicyValidationError(
                f"max_age 格式不合法: {self.max_age!r}，"
                "应为 ES 时间格式（如 '30d', '7d'）"
            )


@dataclass
class SizeBasedRolloverPolicy:
    """基于大小的滚动策略模型.

    通过配置索引大小和文档数上限来自动滚动索引，
    防止单索引超过磁盘限制并优化查询性能。

    Attributes:
        max_size: 最大索引大小，ES 大小格式（如 "10GB", "500MB"）
        max_docs: 最大文档数（必须大于 0）
        alias: 索引别名（必需，不可为空）
        index_prefix: 索引前缀（如 "logs"）
        max_age: 可选的最大保留时间，ES 时间格式

    Raises:
        PolicyValidationError: 当参数校验失败时抛出

    Examples:
        >>> policy = SizeBasedRolloverPolicy(
        ...     max_size="10GB",
        ...     max_docs=1000000,
        ...     alias="logs",
        ...     index_prefix="logs",
        ... )
    """

    max_size: str
    max_docs: int
    alias: str
    index_prefix: str = ""
    max_age: str | None = None

    def __post_init__(self) -> None:
        """校验策略参数合法性."""
        if not self.alias:
            raise PolicyValidationError("alias 不能为空")
        if not validate_size_format(self.max_size):
            raise PolicyValidationError(
                f"max_size 格式不合法: {self.max_size!r}，"
                "应为 ES 大小格式（如 '10GB', '500MB'）"
            )
        if self.max_docs <= 0:
            raise PolicyValidationError(f"max_docs 必须大于 0，当前值: {self.max_docs}")
        if self.max_age is not None and not validate_time_format(self.max_age):
            raise PolicyValidationError(
                f"max_age 格式不合法: {self.max_age!r}，"
                "应为 ES 时间格式（如 '30d', '7d'）"
            )


@dataclass
class LifecyclePhase:
    """生命周期阶段数据模型.

    定义 ILM 策略中某一阶段的配置，如热阶段、温阶段、冷阶段、删除阶段。

    Attributes:
        name: 阶段名称（如 "hot", "warm", "cold", "delete"）
        min_age: 进入该阶段的最小时间，ES 时间格式（如 "0ms", "30d"）
        actions: 阶段动作配置字典

    Raises:
        PolicyValidationError: 当 min_age 格式不合法时抛出

    Examples:
        >>> phase = LifecyclePhase(
        ...     name="hot",
        ...     min_age="0ms",
        ...     actions={"rollover": {"max_size": "50GB"}},
        ... )
    """

    name: str
    min_age: str = "0ms"
    actions: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """校验阶段参数合法性."""
        if not validate_time_format(self.min_age):
            raise PolicyValidationError(
                f"min_age 格式不合法: {self.min_age!r}，"
                "应为 ES 时间格式（如 '0ms', '30d'）"
            )


@dataclass
class IndexLifecyclePolicy:
    """索引生命周期管理策略模型.

    定义热温冷删除各阶段的配置来管理索引全生命周期，
    实现自动化存储成本优化。

    Attributes:
        name: 策略名称
        hot_phase: 热阶段配置（必需）
        warm_phase: 温阶段配置（可选）
        cold_phase: 冷阶段配置（可选）
        delete_phase: 删除阶段配置（可选）

    Raises:
        PolicyValidationError: 当 hot_phase 为 None 时抛出

    Examples:
        >>> policy = IndexLifecyclePolicy(
        ...     name="logs_lifecycle",
        ...     hot_phase=LifecyclePhase(name="hot", min_age="0ms"),
        ...     delete_phase=LifecyclePhase(name="delete", min_age="30d"),
        ... )
    """

    name: str
    hot_phase: LifecyclePhase | None = None
    warm_phase: LifecyclePhase | None = None
    cold_phase: LifecyclePhase | None = None
    delete_phase: LifecyclePhase | None = None

    def __post_init__(self) -> None:
        """校验策略参数合法性."""
        if self.hot_phase is None:
            raise PolicyValidationError("hot_phase 为必需参数，不能为 None")


@dataclass
class ShrinkPolicy:
    """索引压缩策略模型.

    通过减少索引分片数来降低资源占用和查询开销。

    Attributes:
        source_index: 源索引名
        target_index: 目标索引名
        target_shards: 目标分片数（必须 ≥ 1）
        force_merge: 是否在压缩前执行段合并（默认 True）
        copy_settings: 是否复制源索引设置（默认 True）

    Raises:
        PolicyValidationError: 当参数校验失败时抛出

    Examples:
        >>> policy = ShrinkPolicy(
        ...     source_index="logs-000001",
        ...     target_index="shrink-logs-000001",
        ...     target_shards=1,
        ... )
    """

    source_index: str
    target_index: str
    target_shards: int
    force_merge: bool = True
    copy_settings: bool = True

    def __post_init__(self) -> None:
        """校验策略参数合法性."""
        if self.target_shards < 1:
            raise PolicyValidationError(
                f"target_shards 必须 ≥ 1，当前值: {self.target_shards}"
            )
        if self.source_index == self.target_index:
            raise PolicyValidationError(
                f"source_index 与 target_index 不能相同: {self.source_index!r}"
            )


@dataclass
class ArchivePolicy:
    """索引归档策略模型.

    将不再频繁查询的索引进行归档处理，降低存储成本同时保留历史数据。

    Attributes:
        source_index: 源索引名
        archive_index: 归档索引名
        compress: 是否对归档索引执行段合并压缩（默认 True）
        reduce_replicas: 归档后副本数（默认 0，不能为负数）
        delete_source: 是否删除源索引（默认 True）

    Raises:
        PolicyValidationError: 当参数校验失败时抛出

    Examples:
        >>> policy = ArchivePolicy(
        ...     source_index="logs-2024-01",
        ...     archive_index="archive-logs-2024-01",
        ... )
    """

    source_index: str
    archive_index: str
    compress: bool = True
    reduce_replicas: int = 0
    delete_source: bool = True

    def __post_init__(self) -> None:
        """校验策略参数合法性."""
        if self.source_index == self.archive_index:
            raise PolicyValidationError(
                f"source_index 与 archive_index 不能相同: {self.source_index!r}"
            )
        if self.reduce_replicas < 0:
            raise PolicyValidationError(
                f"reduce_replicas 不能为负数，当前值: {self.reduce_replicas}"
            )


@dataclass
class CleanupPolicy:
    """索引清理策略模型.

    定期自动清理过期或无用的索引，释放存储空间。

    Attributes:
        index_pattern: 索引匹配模式（如 "logs-*"）
        max_age: 最大保留时间，ES 时间格式（如 "30d"）
        min_age: 最小保留时间，ES 时间格式（可选，如 "7d"）
        dry_run: 试运行模式，为 True 时仅返回待清理列表不实际删除（默认 False）
        filter_func: 自定义过滤函数，接受索引信息字典并返回布尔值（可选）

    Raises:
        PolicyValidationError: 当参数校验失败时抛出

    Examples:
        >>> policy = CleanupPolicy(
        ...     index_pattern="logs-*",
        ...     max_age="30d",
        ... )
    """

    index_pattern: str
    max_age: str
    min_age: str | None = None
    dry_run: bool = False
    filter_func: Callable[[dict[str, Any]], bool] | None = None

    def __post_init__(self) -> None:
        """校验策略参数合法性."""
        if not validate_time_format(self.max_age):
            raise PolicyValidationError(
                f"max_age 格式不合法: {self.max_age!r}，"
                "应为 ES 时间格式（如 '30d', '7d'）"
            )
        if self.min_age is not None:
            if not validate_time_format(self.min_age):
                raise PolicyValidationError(
                    f"min_age 格式不合法: {self.min_age!r}，"
                    "应为 ES 时间格式（如 '7d', '1d'）"
                )
            # 校验 min_age ≤ max_age
            min_seconds = parse_time_to_seconds(self.min_age)
            max_seconds = parse_time_to_seconds(self.max_age)
            if min_seconds > max_seconds:
                raise PolicyValidationError(
                    f"min_age ({self.min_age}) 不能大于 max_age ({self.max_age})"
                )
