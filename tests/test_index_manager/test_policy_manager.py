"""策略管理器单元测试."""

import time
from unittest.mock import MagicMock

import pytest

from elasticflow.index_manager.models import IndexInfo, RolloverInfo
from elasticflow.index_manager.policies.exceptions import (
    PolicyExecutionError,
    PolicyNotFoundError,
    PolicyValidationError,
)
from elasticflow.index_manager.policies.manager import IndexPolicyManager
from elasticflow.index_manager.policies.models import (
    ArchivePolicy,
    CleanupPolicy,
    IndexLifecyclePolicy,
    LifecyclePhase,
    ShrinkPolicy,
    SizeBasedRolloverPolicy,
    TimeBasedRolloverPolicy,
)


@pytest.fixture
def mock_index_manager() -> MagicMock:
    """创建模拟的 IndexManager 实例."""
    manager = MagicMock()
    return manager


@pytest.fixture
def policy_manager(mock_index_manager: MagicMock) -> IndexPolicyManager:
    """创建 IndexPolicyManager 实例."""
    return IndexPolicyManager(mock_index_manager)


# ==================== 注册与管理 ====================


class TestPolicyRegistration:
    """策略注册与管理测试."""

    def test_register_policy(self, policy_manager: IndexPolicyManager) -> None:
        """测试注册策略."""
        policy = TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="logs")
        result = policy_manager.register_policy("test", policy)
        assert result is policy_manager  # 链式调用
        assert "test" in policy_manager.list_policies()

    def test_register_policy_overwrite(
        self, policy_manager: IndexPolicyManager
    ) -> None:
        """测试同名策略覆盖."""
        policy1 = TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="logs")
        policy2 = TimeBasedRolloverPolicy(interval="7d", max_age="90d", alias="logs")
        policy_manager.register_policy("test", policy1)
        policy_manager.register_policy("test", policy2)
        assert len(policy_manager.list_policies()) == 1

    def test_list_policies_empty(self, policy_manager: IndexPolicyManager) -> None:
        """测试空列表."""
        assert policy_manager.list_policies() == []

    def test_list_policies_multiple(self, policy_manager: IndexPolicyManager) -> None:
        """测试多策略列表."""
        policy_manager.register_policy(
            "p1", TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="a")
        )
        policy_manager.register_policy(
            "p2",
            SizeBasedRolloverPolicy(max_size="10GB", max_docs=100, alias="b"),
        )
        assert set(policy_manager.list_policies()) == {"p1", "p2"}

    def test_remove_policy(self, policy_manager: IndexPolicyManager) -> None:
        """测试移除策略."""
        policy_manager.register_policy(
            "test", TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="a")
        )
        result = policy_manager.remove_policy("test")
        assert result is policy_manager  # 链式调用
        assert "test" not in policy_manager.list_policies()

    def test_remove_nonexistent_policy_raises_error(
        self, policy_manager: IndexPolicyManager
    ) -> None:
        """测试移除不存在的策略抛出异常."""
        with pytest.raises(PolicyNotFoundError, match="策略 'missing' 不存在"):
            policy_manager.remove_policy("missing")

    def test_chain_operations(self, policy_manager: IndexPolicyManager) -> None:
        """测试链式操作."""
        p1 = TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="a")
        p2 = SizeBasedRolloverPolicy(max_size="10GB", max_docs=100, alias="b")
        policy_manager.register_policy("p1", p1).register_policy("p2", p2)
        assert len(policy_manager.list_policies()) == 2
        policy_manager.remove_policy("p1").remove_policy("p2")
        assert len(policy_manager.list_policies()) == 0


# ==================== apply_policy 分发 ====================


class TestApplyPolicyDispatch:
    """apply_policy 分发逻辑测试."""

    def test_apply_nonexistent_policy_raises_error(
        self, policy_manager: IndexPolicyManager
    ) -> None:
        """测试应用不存在的策略抛出 PolicyNotFoundError."""
        with pytest.raises(PolicyNotFoundError, match="策略 'missing' 不存在"):
            policy_manager.apply_policy("missing")

    def test_apply_unsupported_policy_type_raises_error(
        self, policy_manager: IndexPolicyManager
    ) -> None:
        """测试不支持的策略类型抛出 PolicyValidationError."""
        # 注册一个非策略类型对象
        policy_manager._policies["bad"] = "not a policy"  # type: ignore
        with pytest.raises(PolicyValidationError, match="不支持的策略类型"):
            policy_manager.apply_policy("bad")


# ==================== TimeBasedRolloverPolicy 执行 ====================


class TestApplyTimeRollover:
    """时间滚动策略执行测试."""

    def test_basic_rollover(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试基本时间滚动."""
        policy = TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="logs")
        policy_manager.register_policy("test", policy)

        mock_index_manager.rollover_index.return_value = RolloverInfo(
            alias="logs",
            old_index="logs-000001",
            new_index="logs-000002",
            rolled_over=True,
        )
        mock_index_manager.list_indices.return_value = []

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["rolled_over"] is True
        assert result["old_index"] == "logs-000001"
        assert result["new_index"] == "logs-000002"

        mock_index_manager.rollover_index.assert_called_once_with(
            alias="logs", conditions={"max_age": "1d"}
        )

    def test_rollover_with_cleanup(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试时间滚动并清理过期索引."""
        policy = TimeBasedRolloverPolicy(
            interval="1d", max_age="30d", alias="logs", index_pattern="logs-*"
        )
        policy_manager.register_policy("test", policy)

        mock_index_manager.rollover_index.return_value = RolloverInfo(
            alias="logs",
            old_index="logs-000001",
            new_index="logs-000002",
            rolled_over=True,
        )

        # 创建一个过期索引（创建时间为60天前）
        current_ms = int(time.time() * 1000)
        old_creation = current_ms - (60 * 86400 * 1000)  # 60天前
        new_creation = current_ms - (5 * 86400 * 1000)  # 5天前

        mock_index_manager.list_indices.return_value = [
            IndexInfo(name="logs-000001", creation_date=old_creation),
            IndexInfo(name="logs-000002", creation_date=new_creation),
        ]
        mock_index_manager.delete_index.return_value = True

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert "logs-000001" in result["deleted_indices"]
        assert "logs-000002" not in result["deleted_indices"]

    def test_rollover_failure_raises_error(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试滚动失败抛出 PolicyExecutionError."""
        policy = TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="logs")
        policy_manager.register_policy("test", policy)
        mock_index_manager.rollover_index.side_effect = Exception("连接超时")

        with pytest.raises(PolicyExecutionError, match="执行时间滚动策略失败"):
            policy_manager.apply_policy("test")


# ==================== SizeBasedRolloverPolicy 执行 ====================


class TestApplySizeRollover:
    """大小滚动策略执行测试."""

    def test_basic_size_rollover(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试基本大小滚动."""
        policy = SizeBasedRolloverPolicy(
            max_size="10GB", max_docs=1000000, alias="logs"
        )
        policy_manager.register_policy("test", policy)

        mock_index_manager.rollover_index.return_value = RolloverInfo(
            alias="logs",
            old_index="logs-000001",
            new_index="logs-000002",
            rolled_over=True,
        )

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["conditions"] == {"max_size": "10GB", "max_docs": 1000000}

        mock_index_manager.rollover_index.assert_called_once_with(
            alias="logs",
            conditions={"max_size": "10GB", "max_docs": 1000000},
        )

    def test_size_rollover_with_max_age(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试大小滚动附带 max_age 条件."""
        policy = SizeBasedRolloverPolicy(
            max_size="10GB", max_docs=100, alias="logs", max_age="7d"
        )
        policy_manager.register_policy("test", policy)

        mock_index_manager.rollover_index.return_value = RolloverInfo(
            alias="logs",
            old_index="logs-000001",
            rolled_over=False,
        )

        result = policy_manager.apply_policy("test")
        assert result["conditions"]["max_age"] == "7d"

    def test_size_rollover_failure(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试大小滚动失败."""
        policy = SizeBasedRolloverPolicy(max_size="10GB", max_docs=100, alias="logs")
        policy_manager.register_policy("test", policy)
        mock_index_manager.rollover_index.side_effect = Exception("失败")

        with pytest.raises(PolicyExecutionError, match="执行大小滚动策略失败"):
            policy_manager.apply_policy("test")


# ==================== IndexLifecyclePolicy 执行 ====================


class TestApplyLifecycle:
    """生命周期策略执行测试."""

    def test_basic_lifecycle(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试基本生命周期策略."""
        hot = LifecyclePhase(
            name="hot",
            min_age="0ms",
            actions={"rollover": {"max_size": "50GB"}},
        )
        delete = LifecyclePhase(
            name="delete",
            min_age="30d",
            actions={"delete": {}},
        )
        policy = IndexLifecyclePolicy(
            name="logs_lifecycle",
            hot_phase=hot,
            delete_phase=delete,
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.create_ilm_policy.return_value = True

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["policy_name"] == "logs_lifecycle"
        assert set(result["phases"]) == {"hot", "delete"}

        mock_index_manager.create_ilm_policy.assert_called_once_with(
            policy_name="logs_lifecycle",
            phases={
                "hot": {
                    "min_age": "0ms",
                    "actions": {"rollover": {"max_size": "50GB"}},
                },
                "delete": {"min_age": "30d", "actions": {"delete": {}}},
            },
        )

    def test_full_lifecycle(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试完整四阶段生命周期."""
        policy = IndexLifecyclePolicy(
            name="full",
            hot_phase=LifecyclePhase(name="hot", min_age="0ms"),
            warm_phase=LifecyclePhase(name="warm", min_age="7d"),
            cold_phase=LifecyclePhase(name="cold", min_age="30d"),
            delete_phase=LifecyclePhase(name="delete", min_age="90d"),
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.create_ilm_policy.return_value = True

        result = policy_manager.apply_policy("test")
        assert set(result["phases"]) == {"hot", "warm", "cold", "delete"}

    def test_lifecycle_failure(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试生命周期策略失败."""
        policy = IndexLifecyclePolicy(
            name="fail",
            hot_phase=LifecyclePhase(name="hot"),
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.create_ilm_policy.side_effect = Exception("ILM 创建失败")

        with pytest.raises(PolicyExecutionError, match="执行生命周期策略失败"):
            policy_manager.apply_policy("test")


# ==================== ShrinkPolicy 执行 ====================


class TestApplyShrink:
    """压缩策略执行测试."""

    def test_basic_shrink(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试基本压缩策略."""
        policy = ShrinkPolicy(
            source_index="logs-000001",
            target_index="shrink-logs",
            target_shards=1,
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.shrink_index.return_value = True

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["source_index"] == "logs-000001"
        assert result["target_index"] == "shrink-logs"
        assert result["target_shards"] == 1

        # 验证调用顺序：put_settings → force_merge → shrink_index
        mock_index_manager.put_settings.assert_called_once_with(
            index_name="logs-000001",
            settings={"index": {"blocks": {"write": True}}},
        )
        mock_index_manager.force_merge.assert_called_once_with(
            index_name="logs-000001", max_num_segments=1
        )
        mock_index_manager.shrink_index.assert_called_once()

    def test_shrink_without_force_merge(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试不执行段合并的压缩."""
        policy = ShrinkPolicy(
            source_index="a",
            target_index="b",
            target_shards=1,
            force_merge=False,
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.shrink_index.return_value = True

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        mock_index_manager.force_merge.assert_not_called()

    def test_shrink_failure(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试压缩策略失败."""
        policy = ShrinkPolicy(source_index="a", target_index="b", target_shards=1)
        policy_manager.register_policy("test", policy)
        mock_index_manager.put_settings.side_effect = Exception("设置失败")

        with pytest.raises(PolicyExecutionError, match="执行压缩策略失败"):
            policy_manager.apply_policy("test")


# ==================== ArchivePolicy 执行 ====================


class TestApplyArchive:
    """归档策略执行测试."""

    def test_full_archive(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试完整归档流程."""
        policy = ArchivePolicy(
            source_index="logs-old",
            archive_index="archive-logs",
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.reindex.return_value = {"total": 100}

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["source_index"] == "logs-old"
        assert result["archive_index"] == "archive-logs"
        assert "reindex" in result["steps_completed"]
        assert "put_settings" in result["steps_completed"]
        assert "force_merge" in result["steps_completed"]
        assert "delete_source" in result["steps_completed"]

        mock_index_manager.reindex.assert_called_once_with(
            source_index="logs-old", dest_index="archive-logs"
        )
        mock_index_manager.put_settings.assert_called_once_with(
            index_name="archive-logs",
            settings={"index": {"number_of_replicas": 0}},
        )
        mock_index_manager.force_merge.assert_called_once_with(
            index_name="archive-logs", max_num_segments=1
        )
        mock_index_manager.delete_index.assert_called_once_with("logs-old")

    def test_archive_without_compress_and_delete(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试不压缩不删源索引的归档."""
        policy = ArchivePolicy(
            source_index="a",
            archive_index="b",
            compress=False,
            delete_source=False,
        )
        policy_manager.register_policy("test", policy)
        mock_index_manager.reindex.return_value = {"total": 50}

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert "force_merge" not in result["steps_completed"]
        assert "delete_source" not in result["steps_completed"]
        mock_index_manager.force_merge.assert_not_called()
        mock_index_manager.delete_index.assert_not_called()

    def test_archive_reindex_failure(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试归档时 reindex 失败."""
        policy = ArchivePolicy(source_index="a", archive_index="b")
        policy_manager.register_policy("test", policy)
        mock_index_manager.reindex.side_effect = Exception("reindex 失败")

        with pytest.raises(PolicyExecutionError, match="执行归档策略失败"):
            policy_manager.apply_policy("test")

    def test_archive_partial_failure(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试归档中间步骤失败，异常中包含已完成步骤."""
        policy = ArchivePolicy(source_index="a", archive_index="b")
        policy_manager.register_policy("test", policy)
        mock_index_manager.reindex.return_value = {"total": 100}
        mock_index_manager.put_settings.side_effect = Exception("设置失败")

        with pytest.raises(PolicyExecutionError, match="已完成步骤.*reindex"):
            policy_manager.apply_policy("test")


# ==================== CleanupPolicy 执行 ====================


class TestApplyCleanup:
    """清理策略执行测试."""

    def test_basic_cleanup(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试基本清理."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d")
        policy_manager.register_policy("test", policy)

        current_ms = int(time.time() * 1000)
        mock_index_manager.list_indices.return_value = [
            IndexInfo(
                name="logs-old",
                creation_date=current_ms - (60 * 86400 * 1000),  # 60天前
            ),
            IndexInfo(
                name="logs-new",
                creation_date=current_ms - (5 * 86400 * 1000),  # 5天前
            ),
        ]
        mock_index_manager.delete_index.return_value = True

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["deleted_count"] == 1
        assert "logs-old" in result["deleted_indices"]
        assert "logs-new" in result["skipped_indices"]

    def test_dry_run_mode(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试 dry_run 模式不实际删除."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d", dry_run=True)
        policy_manager.register_policy("test", policy)

        current_ms = int(time.time() * 1000)
        mock_index_manager.list_indices.return_value = [
            IndexInfo(
                name="logs-old",
                creation_date=current_ms - (60 * 86400 * 1000),
            ),
        ]

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["dry_run"] is True
        assert result["deleted_count"] == 0
        assert result["deleted_indices"] == []
        assert "logs-old" in result["candidates"]
        mock_index_manager.delete_index.assert_not_called()

    def test_cleanup_with_filter_func(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试自定义过滤函数."""

        def only_empty(info: dict) -> bool:
            return info.get("docs_count", 0) == 0

        policy = CleanupPolicy(
            index_pattern="logs-*", max_age="30d", filter_func=only_empty
        )
        policy_manager.register_policy("test", policy)

        current_ms = int(time.time() * 1000)
        mock_index_manager.list_indices.return_value = [
            IndexInfo(
                name="logs-empty",
                creation_date=current_ms - (60 * 86400 * 1000),
                docs_count=0,
            ),
            IndexInfo(
                name="logs-notempty",
                creation_date=current_ms - (60 * 86400 * 1000),
                docs_count=100,
            ),
        ]
        mock_index_manager.delete_index.return_value = True

        result = policy_manager.apply_policy("test")
        assert result["deleted_count"] == 1
        assert "logs-empty" in result["deleted_indices"]
        assert "logs-notempty" in result["skipped_indices"]

    def test_cleanup_skips_no_creation_date(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试跳过无创建时间的索引."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d")
        policy_manager.register_policy("test", policy)

        mock_index_manager.list_indices.return_value = [
            IndexInfo(name="logs-no-date", creation_date=0),
        ]

        result = policy_manager.apply_policy("test")
        assert result["deleted_count"] == 0
        assert "logs-no-date" in result["skipped_indices"]

    def test_cleanup_delete_failure_captured(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试删除失败时错误被捕获."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d")
        policy_manager.register_policy("test", policy)

        current_ms = int(time.time() * 1000)
        mock_index_manager.list_indices.return_value = [
            IndexInfo(
                name="logs-old",
                creation_date=current_ms - (60 * 86400 * 1000),
            ),
        ]
        mock_index_manager.delete_index.side_effect = Exception("删除失败")

        result = policy_manager.apply_policy("test")
        assert result["success"] is True
        assert result["deleted_count"] == 0
        assert len(result["errors"]) == 1
        assert result["errors"][0]["index"] == "logs-old"

    def test_cleanup_with_min_age(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试带 min_age 的清理策略."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d", min_age="7d")
        policy_manager.register_policy("test", policy)

        current_ms = int(time.time() * 1000)
        mock_index_manager.list_indices.return_value = [
            IndexInfo(
                name="logs-very-old",
                creation_date=current_ms - (60 * 86400 * 1000),  # 60天
            ),
            IndexInfo(
                name="logs-recent",
                creation_date=current_ms - (3 * 86400 * 1000),  # 3天
            ),
        ]
        mock_index_manager.delete_index.return_value = True

        result = policy_manager.apply_policy("test")
        assert "logs-very-old" in result["deleted_indices"]
        assert "logs-recent" in result["skipped_indices"]


# ==================== apply_all_policies ====================


class TestApplyAllPolicies:
    """apply_all_policies 批量执行测试."""

    def test_apply_all_success(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试全部策略成功执行."""
        policy_manager.register_policy(
            "p1",
            ShrinkPolicy(source_index="a", target_index="b", target_shards=1),
        )
        policy_manager.register_policy(
            "p2",
            ShrinkPolicy(source_index="c", target_index="d", target_shards=1),
        )
        mock_index_manager.shrink_index.return_value = True

        results = policy_manager.apply_all_policies()
        assert "p1" in results
        assert "p2" in results
        assert results["p1"]["success"] is True
        assert results["p2"]["success"] is True

    def test_apply_all_with_failure(
        self,
        policy_manager: IndexPolicyManager,
        mock_index_manager: MagicMock,
    ) -> None:
        """测试部分策略失败时不影响其他策略."""
        policy_manager.register_policy(
            "good",
            ShrinkPolicy(source_index="a", target_index="b", target_shards=1),
        )
        mock_index_manager.shrink_index.return_value = True

        # 注册一个会导致执行失败的策略
        policy_manager._policies["bad"] = "not a policy"  # type: ignore

        results = policy_manager.apply_all_policies()
        assert results["good"]["success"] is True
        assert results["bad"]["success"] is False
        assert "error" in results["bad"]

    def test_apply_all_empty(self, policy_manager: IndexPolicyManager) -> None:
        """测试无策略时返回空字典."""
        results = policy_manager.apply_all_policies()
        assert results == {}
