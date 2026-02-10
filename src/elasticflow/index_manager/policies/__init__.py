"""索引管理策略子模块.

提供内置的索引管理策略，包括：
- 基于时间的滚动策略（TimeBasedRolloverPolicy）
- 基于大小的滚动策略（SizeBasedRolloverPolicy）
- 索引生命周期管理策略（IndexLifecyclePolicy）
- 索引压缩策略（ShrinkPolicy）
- 索引归档策略（ArchivePolicy）
- 索引清理策略（CleanupPolicy）
- 策略管理器（IndexPolicyManager）
"""

from .exceptions import (
    PolicyError,
    PolicyExecutionError,
    PolicyNotFoundError,
    PolicyValidationError,
)
from .manager import IndexPolicyManager
from .models import (
    ArchivePolicy,
    CleanupPolicy,
    IndexLifecyclePolicy,
    LifecyclePhase,
    ShrinkPolicy,
    SizeBasedRolloverPolicy,
    TimeBasedRolloverPolicy,
)
from .utils import parse_time_to_seconds, validate_size_format, validate_time_format

__all__ = [
    # 策略管理器
    "IndexPolicyManager",
    # 策略模型
    "TimeBasedRolloverPolicy",
    "SizeBasedRolloverPolicy",
    "LifecyclePhase",
    "IndexLifecyclePolicy",
    "ShrinkPolicy",
    "ArchivePolicy",
    "CleanupPolicy",
    # 异常类
    "PolicyError",
    "PolicyValidationError",
    "PolicyExecutionError",
    "PolicyNotFoundError",
    # 工具函数
    "validate_time_format",
    "validate_size_format",
    "parse_time_to_seconds",
]
