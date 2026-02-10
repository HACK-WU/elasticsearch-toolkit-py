"""索引策略管理器模块.

提供统一的策略管理器 IndexPolicyManager，用于注册、应用和管理所有索引策略。
"""

import logging
import time
from typing import Any

from ..tool import IndexManager
from .exceptions import (
    PolicyExecutionError,
    PolicyNotFoundError,
    PolicyValidationError,
)
from .models import (
    ArchivePolicy,
    CleanupPolicy,
    IndexLifecyclePolicy,
    LifecyclePhase,
    ShrinkPolicy,
    SizeBasedRolloverPolicy,
    TimeBasedRolloverPolicy,
)
from .utils import parse_time_to_seconds

logger = logging.getLogger(__name__)

# 所有支持的策略类型联合类型
PolicyType = (
    TimeBasedRolloverPolicy
    | SizeBasedRolloverPolicy
    | IndexLifecyclePolicy
    | ShrinkPolicy
    | ArchivePolicy
    | CleanupPolicy
)


class IndexPolicyManager:
    """索引策略管理器.

    统一管理所有索引策略的注册、应用和调度。基于 IndexManager 已有的底层 API
    进行组合编排，不直接操作 Elasticsearch 客户端。

    Args:
        index_manager: IndexManager 实例

    Examples:
        >>> manager = IndexPolicyManager(index_manager)
        >>> manager.register_policy(
        ...     "cleanup_logs",
        ...     CleanupPolicy(
        ...         index_pattern="logs-*",
        ...         max_age="30d",
        ...     ),
        ... )
        >>> result = manager.apply_policy("cleanup_logs")
    """

    def __init__(self, index_manager: IndexManager) -> None:
        """初始化策略管理器.

        Args:
            index_manager: IndexManager 实例
        """
        self._index_manager = index_manager
        self._policies: dict[str, PolicyType] = {}
        logger.info("初始化索引策略管理器")

    def register_policy(self, name: str, policy: PolicyType) -> "IndexPolicyManager":
        """注册策略.

        将策略对象存储到内部字典中。如果同名策略已存在，则覆盖。

        Args:
            name: 策略名称
            policy: 策略对象

        Returns:
            自身实例，支持链式调用

        Examples:
            >>> manager.register_policy("p1", policy1).register_policy("p2", policy2)
        """
        self._policies[name] = policy
        logger.info(f"注册策略: {name} (类型: {type(policy).__name__})")
        return self

    def list_policies(self) -> list[str]:
        """列出所有已注册策略的名称.

        Returns:
            策略名称列表
        """
        return list(self._policies.keys())

    def remove_policy(self, name: str) -> "IndexPolicyManager":
        """移除指定策略.

        Args:
            name: 策略名称

        Returns:
            自身实例，支持链式调用

        Raises:
            PolicyNotFoundError: 当策略名称不存在时抛出
        """
        if name not in self._policies:
            raise PolicyNotFoundError(f"策略 '{name}' 不存在")
        del self._policies[name]
        logger.info(f"移除策略: {name}")
        return self

    def apply_policy(self, name: str) -> dict[str, Any]:
        """应用指定策略.

        根据策略类型自动分发到对应的执行方法。

        Args:
            name: 策略名称

        Returns:
            执行结果字典

        Raises:
            PolicyNotFoundError: 当策略名称不存在时抛出
            PolicyValidationError: 当策略类型不支持时抛出
            PolicyExecutionError: 当策略执行失败时抛出
        """
        if name not in self._policies:
            raise PolicyNotFoundError(f"策略 '{name}' 不存在")

        policy = self._policies[name]
        logger.info(f"开始应用策略: {name} (类型: {type(policy).__name__})")

        if isinstance(policy, TimeBasedRolloverPolicy):
            return self._apply_time_rollover(policy)
        elif isinstance(policy, SizeBasedRolloverPolicy):
            return self._apply_size_rollover(policy)
        elif isinstance(policy, IndexLifecyclePolicy):
            return self._apply_lifecycle(policy)
        elif isinstance(policy, ShrinkPolicy):
            return self._apply_shrink(policy)
        elif isinstance(policy, ArchivePolicy):
            return self._apply_archive(policy)
        elif isinstance(policy, CleanupPolicy):
            return self._apply_cleanup(policy)
        else:
            raise PolicyValidationError(f"不支持的策略类型: {type(policy).__name__}")

    def apply_all_policies(self) -> dict[str, dict[str, Any]]:
        """依次执行所有已注册的策略.

        每个策略的结果或错误信息都收集到返回字典中。

        Returns:
            以策略名称为键，执行结果或错误信息为值的字典
        """
        results: dict[str, dict[str, Any]] = {}
        for name in list(self._policies.keys()):
            try:
                results[name] = self.apply_policy(name)
            except Exception as e:
                results[name] = {
                    "success": False,
                    "error": str(e),
                    "error_type": type(e).__name__,
                }
                logger.error(f"策略 '{name}' 执行失败: {e}")
        return results

    # ========== 内部执行方法 ==========

    def _apply_time_rollover(self, policy: TimeBasedRolloverPolicy) -> dict[str, Any]:
        """执行基于时间的滚动策略.

        1. 调用 rollover_index() 并根据 interval 转换为 max_age 滚动条件
        2. 检查超过 max_age 的旧索引并调用 delete_index() 清理

        Args:
            policy: 时间滚动策略对象

        Returns:
            执行结果字典
        """
        try:
            # 构建滚动条件：将 interval 作为 max_age 条件
            conditions = {"max_age": policy.interval}
            rollover_result = self._index_manager.rollover_index(
                alias=policy.alias,
                conditions=conditions,
            )

            # 清理超过 max_age 的旧索引
            deleted_indices: list[str] = []
            if policy.index_pattern:
                max_age_seconds = parse_time_to_seconds(policy.max_age)
                current_time = int(time.time() * 1000)  # 毫秒时间戳
                indices = self._index_manager.list_indices(pattern=policy.index_pattern)
                for index_info in indices:
                    if index_info.creation_date > 0:
                        age_seconds = (current_time - index_info.creation_date) / 1000
                        if age_seconds > max_age_seconds:
                            if self._index_manager.delete_index(index_info.name):
                                deleted_indices.append(index_info.name)

            return {
                "success": True,
                "rolled_over": rollover_result.rolled_over,
                "old_index": rollover_result.old_index,
                "new_index": rollover_result.new_index,
                "deleted_indices": deleted_indices,
            }
        except Exception as e:
            raise PolicyExecutionError(f"执行时间滚动策略失败: {e}") from e

    def _apply_size_rollover(self, policy: SizeBasedRolloverPolicy) -> dict[str, Any]:
        """执行基于大小的滚动策略.

        构建包含 max_size 和 max_docs 的滚动条件调用 rollover_index()。
        如果同时配置了 max_age，则在条件中一并包含。

        Args:
            policy: 大小滚动策略对象

        Returns:
            执行结果字典
        """
        try:
            conditions: dict[str, Any] = {
                "max_size": policy.max_size,
                "max_docs": policy.max_docs,
            }
            if policy.max_age:
                conditions["max_age"] = policy.max_age

            rollover_result = self._index_manager.rollover_index(
                alias=policy.alias,
                conditions=conditions,
            )

            return {
                "success": True,
                "rolled_over": rollover_result.rolled_over,
                "old_index": rollover_result.old_index,
                "new_index": rollover_result.new_index,
                "conditions": conditions,
            }
        except Exception as e:
            raise PolicyExecutionError(f"执行大小滚动策略失败: {e}") from e

    def _apply_lifecycle(self, policy: IndexLifecyclePolicy) -> dict[str, Any]:
        """执行索引生命周期管理策略.

        将阶段配置转换为 ES ILM 格式，调用 create_ilm_policy() 创建策略。

        Args:
            policy: 生命周期策略对象

        Returns:
            执行结果字典
        """
        try:
            phases: dict[str, dict[str, Any]] = {}

            # 转换各阶段配置为 ES ILM 格式
            phase_mapping: list[tuple[str, LifecyclePhase | None]] = [
                ("hot", policy.hot_phase),
                ("warm", policy.warm_phase),
                ("cold", policy.cold_phase),
                ("delete", policy.delete_phase),
            ]

            for phase_name, phase in phase_mapping:
                if phase is not None:
                    phases[phase_name] = {
                        "min_age": phase.min_age,
                        "actions": phase.actions,
                    }

            created = self._index_manager.create_ilm_policy(
                policy_name=policy.name,
                phases=phases,
            )

            return {
                "success": created,
                "policy_name": policy.name,
                "phases": list(phases.keys()),
            }
        except Exception as e:
            raise PolicyExecutionError(f"执行生命周期策略失败: {e}") from e

    def _apply_shrink(self, policy: ShrinkPolicy) -> dict[str, Any]:
        """执行索引压缩策略.

        1. 设置源索引为只读（blocks.write=True）
        2. 如果 force_merge 为 True，先执行段合并
        3. 调用 shrink_index() 压缩分片

        Args:
            policy: 压缩策略对象

        Returns:
            执行结果字典
        """
        try:
            # 设置源索引为只读
            self._index_manager.put_settings(
                index_name=policy.source_index,
                settings={"index": {"blocks": {"write": True}}},
            )

            # 可选的段合并
            if policy.force_merge:
                self._index_manager.force_merge(
                    index_name=policy.source_index,
                    max_num_segments=1,
                )

            # 执行压缩
            shrink_settings = None
            if policy.copy_settings:
                shrink_settings = {"index.blocks.write": None}

            shrunk = self._index_manager.shrink_index(
                source_index=policy.source_index,
                target_index=policy.target_index,
                number_of_shards=policy.target_shards,
                settings=shrink_settings,
            )

            return {
                "success": shrunk,
                "source_index": policy.source_index,
                "target_index": policy.target_index,
                "target_shards": policy.target_shards,
            }
        except Exception as e:
            raise PolicyExecutionError(f"执行压缩策略失败: {e}") from e

    def _apply_archive(self, policy: ArchivePolicy) -> dict[str, Any]:
        """执行索引归档策略.

        1. reindex: 将源索引数据复制到归档索引
        2. put_settings: 设置归档索引的副本数
        3. force_merge（可选）: 对归档索引进行段合并压缩
        4. delete_index（可选）: 删除源索引

        任一步骤失败抛出 PolicyExecutionError。

        Args:
            policy: 归档策略对象

        Returns:
            执行结果字典
        """
        steps_completed: list[str] = []
        try:
            # 步骤1: reindex
            self._index_manager.reindex(
                source_index=policy.source_index,
                dest_index=policy.archive_index,
            )
            steps_completed.append("reindex")

            # 步骤2: 设置副本数
            self._index_manager.put_settings(
                index_name=policy.archive_index,
                settings={"index": {"number_of_replicas": policy.reduce_replicas}},
            )
            steps_completed.append("put_settings")

            # 步骤3: 可选的段合并压缩
            if policy.compress:
                self._index_manager.force_merge(
                    index_name=policy.archive_index,
                    max_num_segments=1,
                )
                steps_completed.append("force_merge")

            # 步骤4: 可选删除源索引
            if policy.delete_source:
                self._index_manager.delete_index(policy.source_index)
                steps_completed.append("delete_source")

            return {
                "success": True,
                "source_index": policy.source_index,
                "archive_index": policy.archive_index,
                "steps_completed": steps_completed,
            }
        except Exception as e:
            raise PolicyExecutionError(
                f"执行归档策略失败（已完成步骤: {steps_completed}）: {e}"
            ) from e

    def _apply_cleanup(self, policy: CleanupPolicy) -> dict[str, Any]:
        """执行索引清理策略.

        1. list_indices: 获取匹配 index_pattern 的所有索引
        2. 根据 creation_date 与 max_age/min_age 比较判断是否过期
        3. 应用自定义过滤函数（如有）
        4. dry_run 模式仅返回待清理列表，否则实际执行删除

        Args:
            policy: 清理策略对象

        Returns:
            执行结果字典
        """
        try:
            indices = self._index_manager.list_indices(pattern=policy.index_pattern)
            max_age_seconds = parse_time_to_seconds(policy.max_age)
            min_age_seconds = (
                parse_time_to_seconds(policy.min_age) if policy.min_age else 0
            )
            current_time = int(time.time() * 1000)  # 毫秒时间戳

            to_delete: list[str] = []
            skipped: list[str] = []
            errors: list[dict[str, str]] = []

            for index_info in indices:
                # 计算索引年龄
                if index_info.creation_date <= 0:
                    skipped.append(index_info.name)
                    continue

                age_seconds = (current_time - index_info.creation_date) / 1000

                # 检查是否超过最大保留时间且满足最小保留时间
                if age_seconds > max_age_seconds and age_seconds >= min_age_seconds:
                    # 应用自定义过滤函数
                    if policy.filter_func is not None:
                        index_dict = {
                            "name": index_info.name,
                            "creation_date": index_info.creation_date,
                            "docs_count": index_info.docs_count,
                            "store_size": index_info.store_size,
                            "health": index_info.health,
                            "status": index_info.status,
                        }
                        if not policy.filter_func(index_dict):
                            skipped.append(index_info.name)
                            continue
                    to_delete.append(index_info.name)
                else:
                    skipped.append(index_info.name)

            # dry_run 模式仅返回待清理列表
            deleted_indices: list[str] = []
            if not policy.dry_run:
                for index_name in to_delete:
                    try:
                        if self._index_manager.delete_index(index_name):
                            deleted_indices.append(index_name)
                        else:
                            errors.append(
                                {
                                    "index": index_name,
                                    "error": "删除返回 False",
                                }
                            )
                    except Exception as e:
                        errors.append(
                            {
                                "index": index_name,
                                "error": str(e),
                            }
                        )
            else:
                deleted_indices = []

            return {
                "success": True,
                "dry_run": policy.dry_run,
                "deleted_count": len(deleted_indices),
                "deleted_indices": deleted_indices,
                "candidates": to_delete if policy.dry_run else [],
                "skipped_indices": skipped,
                "errors": errors,
            }
        except Exception as e:
            raise PolicyExecutionError(f"执行清理策略失败: {e}") from e
