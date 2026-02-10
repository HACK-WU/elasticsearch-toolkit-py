"""策略模型单元测试."""

import pytest

from elasticflow.index_manager.policies.exceptions import PolicyValidationError
from elasticflow.index_manager.policies.models import (
    ArchivePolicy,
    CleanupPolicy,
    IndexLifecyclePolicy,
    LifecyclePhase,
    ShrinkPolicy,
    SizeBasedRolloverPolicy,
    TimeBasedRolloverPolicy,
)


# ==================== TimeBasedRolloverPolicy ====================


class TestTimeBasedRolloverPolicy:
    """TimeBasedRolloverPolicy 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        policy = TimeBasedRolloverPolicy(
            interval="1d", max_age="30d", alias="logs", index_pattern="logs-*"
        )
        assert policy.interval == "1d"
        assert policy.max_age == "30d"
        assert policy.alias == "logs"
        assert policy.index_pattern == "logs-*"

    def test_default_index_pattern(self) -> None:
        """测试默认 index_pattern 为空字符串."""
        policy = TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="logs")
        assert policy.index_pattern == ""

    def test_invalid_interval_raises_error(self) -> None:
        """测试不合法 interval 抛出异常."""
        with pytest.raises(PolicyValidationError, match="interval 格式不合法"):
            TimeBasedRolloverPolicy(interval="bad", max_age="30d", alias="logs")

    def test_invalid_max_age_raises_error(self) -> None:
        """测试不合法 max_age 抛出异常."""
        with pytest.raises(PolicyValidationError, match="max_age 格式不合法"):
            TimeBasedRolloverPolicy(interval="1d", max_age="bad", alias="logs")

    def test_empty_alias_raises_error(self) -> None:
        """测试空 alias 抛出异常."""
        with pytest.raises(PolicyValidationError, match="alias 不能为空"):
            TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias="")

    def test_none_alias_raises_error(self) -> None:
        """测试 None alias 抛出异常."""
        with pytest.raises(PolicyValidationError, match="alias 不能为空"):
            TimeBasedRolloverPolicy(interval="1d", max_age="30d", alias=None)  # type: ignore

    def test_various_valid_intervals(self) -> None:
        """测试各种合法间隔格式."""
        for interval in ["1h", "7d", "1w", "1M", "1y", "500ms"]:
            policy = TimeBasedRolloverPolicy(
                interval=interval, max_age="30d", alias="logs"
            )
            assert policy.interval == interval


# ==================== SizeBasedRolloverPolicy ====================


class TestSizeBasedRolloverPolicy:
    """SizeBasedRolloverPolicy 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        policy = SizeBasedRolloverPolicy(
            max_size="10GB", max_docs=1000000, alias="logs", index_prefix="logs"
        )
        assert policy.max_size == "10GB"
        assert policy.max_docs == 1000000
        assert policy.alias == "logs"
        assert policy.index_prefix == "logs"
        assert policy.max_age is None

    def test_with_max_age(self) -> None:
        """测试带 max_age 参数创建."""
        policy = SizeBasedRolloverPolicy(
            max_size="10GB", max_docs=100, alias="logs", max_age="7d"
        )
        assert policy.max_age == "7d"

    def test_invalid_max_size_raises_error(self) -> None:
        """测试不合法 max_size 抛出异常."""
        with pytest.raises(PolicyValidationError, match="max_size 格式不合法"):
            SizeBasedRolloverPolicy(max_size="bad", max_docs=100, alias="logs")

    def test_zero_max_docs_raises_error(self) -> None:
        """测试零值 max_docs 抛出异常."""
        with pytest.raises(PolicyValidationError, match="max_docs 必须大于 0"):
            SizeBasedRolloverPolicy(max_size="10GB", max_docs=0, alias="logs")

    def test_negative_max_docs_raises_error(self) -> None:
        """测试负数 max_docs 抛出异常."""
        with pytest.raises(PolicyValidationError, match="max_docs 必须大于 0"):
            SizeBasedRolloverPolicy(max_size="10GB", max_docs=-1, alias="logs")

    def test_empty_alias_raises_error(self) -> None:
        """测试空 alias 抛出异常."""
        with pytest.raises(PolicyValidationError, match="alias 不能为空"):
            SizeBasedRolloverPolicy(max_size="10GB", max_docs=100, alias="")

    def test_invalid_max_age_raises_error(self) -> None:
        """测试不合法 max_age 抛出异常."""
        with pytest.raises(PolicyValidationError, match="max_age 格式不合法"):
            SizeBasedRolloverPolicy(
                max_size="10GB", max_docs=100, alias="logs", max_age="bad"
            )

    def test_valid_max_age_none(self) -> None:
        """测试 max_age 为 None 不校验."""
        policy = SizeBasedRolloverPolicy(
            max_size="10GB", max_docs=100, alias="logs", max_age=None
        )
        assert policy.max_age is None


# ==================== LifecyclePhase ====================


class TestLifecyclePhase:
    """LifecyclePhase 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        phase = LifecyclePhase(
            name="hot", min_age="0ms", actions={"rollover": {"max_size": "50GB"}}
        )
        assert phase.name == "hot"
        assert phase.min_age == "0ms"
        assert phase.actions == {"rollover": {"max_size": "50GB"}}

    def test_default_values(self) -> None:
        """测试默认值."""
        phase = LifecyclePhase(name="hot")
        assert phase.min_age == "0ms"
        assert phase.actions == {}

    def test_invalid_min_age_raises_error(self) -> None:
        """测试不合法 min_age 抛出异常."""
        with pytest.raises(PolicyValidationError, match="min_age 格式不合法"):
            LifecyclePhase(name="hot", min_age="bad")

    def test_various_valid_phases(self) -> None:
        """测试各阶段名称."""
        for name in ["hot", "warm", "cold", "delete"]:
            phase = LifecyclePhase(name=name, min_age="30d")
            assert phase.name == name


# ==================== IndexLifecyclePolicy ====================


class TestIndexLifecyclePolicy:
    """IndexLifecyclePolicy 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        hot = LifecyclePhase(name="hot", min_age="0ms")
        policy = IndexLifecyclePolicy(name="test_policy", hot_phase=hot)
        assert policy.name == "test_policy"
        assert policy.hot_phase == hot
        assert policy.warm_phase is None
        assert policy.cold_phase is None
        assert policy.delete_phase is None

    def test_full_lifecycle(self) -> None:
        """测试完整生命周期配置."""
        hot = LifecyclePhase(name="hot", min_age="0ms")
        warm = LifecyclePhase(name="warm", min_age="7d")
        cold = LifecyclePhase(name="cold", min_age="30d")
        delete = LifecyclePhase(name="delete", min_age="90d")
        policy = IndexLifecyclePolicy(
            name="full",
            hot_phase=hot,
            warm_phase=warm,
            cold_phase=cold,
            delete_phase=delete,
        )
        assert policy.warm_phase == warm
        assert policy.cold_phase == cold
        assert policy.delete_phase == delete

    def test_none_hot_phase_raises_error(self) -> None:
        """测试 hot_phase 为 None 抛出异常."""
        with pytest.raises(PolicyValidationError, match="hot_phase 为必需参数"):
            IndexLifecyclePolicy(name="test", hot_phase=None)

    def test_default_hot_phase_is_none(self) -> None:
        """测试默认 hot_phase 为 None 也会抛出异常."""
        with pytest.raises(PolicyValidationError):
            IndexLifecyclePolicy(name="test")


# ==================== ShrinkPolicy ====================


class TestShrinkPolicy:
    """ShrinkPolicy 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        policy = ShrinkPolicy(
            source_index="logs-000001",
            target_index="shrink-logs",
            target_shards=1,
        )
        assert policy.source_index == "logs-000001"
        assert policy.target_index == "shrink-logs"
        assert policy.target_shards == 1
        assert policy.force_merge is True
        assert policy.copy_settings is True

    def test_custom_options(self) -> None:
        """测试自定义选项."""
        policy = ShrinkPolicy(
            source_index="a",
            target_index="b",
            target_shards=2,
            force_merge=False,
            copy_settings=False,
        )
        assert policy.force_merge is False
        assert policy.copy_settings is False

    def test_zero_shards_raises_error(self) -> None:
        """测试零分片数抛出异常."""
        with pytest.raises(PolicyValidationError, match="target_shards 必须 ≥ 1"):
            ShrinkPolicy(source_index="a", target_index="b", target_shards=0)

    def test_negative_shards_raises_error(self) -> None:
        """测试负分片数抛出异常."""
        with pytest.raises(PolicyValidationError, match="target_shards 必须 ≥ 1"):
            ShrinkPolicy(source_index="a", target_index="b", target_shards=-1)

    def test_same_source_target_raises_error(self) -> None:
        """测试源和目标相同抛出异常."""
        with pytest.raises(
            PolicyValidationError, match="source_index 与 target_index 不能相同"
        ):
            ShrinkPolicy(source_index="logs", target_index="logs", target_shards=1)

    def test_boundary_one_shard(self) -> None:
        """测试边界值1分片."""
        policy = ShrinkPolicy(source_index="a", target_index="b", target_shards=1)
        assert policy.target_shards == 1


# ==================== ArchivePolicy ====================


class TestArchivePolicy:
    """ArchivePolicy 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        policy = ArchivePolicy(
            source_index="logs-2024-01",
            archive_index="archive-logs-2024-01",
        )
        assert policy.source_index == "logs-2024-01"
        assert policy.archive_index == "archive-logs-2024-01"
        assert policy.compress is True
        assert policy.reduce_replicas == 0
        assert policy.delete_source is True

    def test_custom_options(self) -> None:
        """测试自定义选项."""
        policy = ArchivePolicy(
            source_index="a",
            archive_index="b",
            compress=False,
            reduce_replicas=2,
            delete_source=False,
        )
        assert policy.compress is False
        assert policy.reduce_replicas == 2
        assert policy.delete_source is False

    def test_same_source_archive_raises_error(self) -> None:
        """测试源和归档相同抛出异常."""
        with pytest.raises(
            PolicyValidationError, match="source_index 与 archive_index 不能相同"
        ):
            ArchivePolicy(source_index="logs", archive_index="logs")

    def test_negative_replicas_raises_error(self) -> None:
        """测试负副本数抛出异常."""
        with pytest.raises(PolicyValidationError, match="reduce_replicas 不能为负数"):
            ArchivePolicy(source_index="a", archive_index="b", reduce_replicas=-1)

    def test_zero_replicas_valid(self) -> None:
        """测试零副本数合法."""
        policy = ArchivePolicy(source_index="a", archive_index="b", reduce_replicas=0)
        assert policy.reduce_replicas == 0


# ==================== CleanupPolicy ====================


class TestCleanupPolicy:
    """CleanupPolicy 模型测试."""

    def test_valid_creation(self) -> None:
        """测试合法参数创建."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d")
        assert policy.index_pattern == "logs-*"
        assert policy.max_age == "30d"
        assert policy.min_age is None
        assert policy.dry_run is False
        assert policy.filter_func is None

    def test_with_min_age(self) -> None:
        """测试带 min_age 参数创建."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d", min_age="7d")
        assert policy.min_age == "7d"

    def test_with_dry_run(self) -> None:
        """测试 dry_run 模式."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d", dry_run=True)
        assert policy.dry_run is True

    def test_with_filter_func(self) -> None:
        """测试自定义过滤函数."""

        def my_filter(info: dict) -> bool:
            return info.get("docs_count", 0) > 0

        policy = CleanupPolicy(
            index_pattern="logs-*", max_age="30d", filter_func=my_filter
        )
        assert policy.filter_func is my_filter

    def test_invalid_max_age_raises_error(self) -> None:
        """测试不合法 max_age 抛出异常."""
        with pytest.raises(PolicyValidationError, match="max_age 格式不合法"):
            CleanupPolicy(index_pattern="logs-*", max_age="bad")

    def test_invalid_min_age_raises_error(self) -> None:
        """测试不合法 min_age 抛出异常."""
        with pytest.raises(PolicyValidationError, match="min_age 格式不合法"):
            CleanupPolicy(index_pattern="logs-*", max_age="30d", min_age="bad")

    def test_min_age_greater_than_max_age_raises_error(self) -> None:
        """测试 min_age > max_age 抛出异常."""
        with pytest.raises(PolicyValidationError, match="min_age.*不能大于.*max_age"):
            CleanupPolicy(index_pattern="logs-*", max_age="7d", min_age="30d")

    def test_min_age_equals_max_age_valid(self) -> None:
        """测试 min_age == max_age 合法."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d", min_age="30d")
        assert policy.min_age == "30d"

    def test_min_age_less_than_max_age_valid(self) -> None:
        """测试 min_age < max_age 合法."""
        policy = CleanupPolicy(index_pattern="logs-*", max_age="30d", min_age="1d")
        assert policy.min_age == "1d"
