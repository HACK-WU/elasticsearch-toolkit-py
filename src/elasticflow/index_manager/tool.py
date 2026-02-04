"""索引管理器核心工具类."""

import logging
from typing import Any
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError

from .models import (
    AliasInfo,
    ILMPolicyInfo,
    ILMPhase,
    ILMIndexStatus,
    IndexInfo,
    IndexMappings,
    IndexSettings,
    IndexTemplateInfo,
    RolloverInfo,
)
from .exceptions import (
    IndexManagerError,
    IndexNotFoundError,
    IndexAlreadyExistsError,
    AliasNotFoundError,
    TemplateNotFoundError,
    ILMNotFoundError,
    RolloverError,
)

logger = logging.getLogger(__name__)


def _validate_index_name(index_name: str, allow_wildcards: bool = False) -> bool:
    """验证索引名称是否符合 Elasticsearch 规范.

    Args:
        index_name: 索引名称
        allow_wildcards: 是否允许通配符（用于查询场景）

    Returns:
        是否有效

    Note:
        Elasticsearch 索引名称限制：
        - 不能以 . 或 _ 开头
        - 不能包含 , # / \\ * ? " < > | 空格
        - 不能是 . 或 ..
        - 长度不能超过 255 字节
    """
    if not index_name or not isinstance(index_name, str):
        return False

    # 检查长度
    if len(index_name.encode("utf-8")) > 255:
        return False

    # 检查不能以 . 或 _ 开头
    if index_name.startswith(".") or index_name.startswith("_"):
        return False

    # 检查是否为 . 或 ..
    if index_name in (".", ".."):
        return False

    # 定义无效字符（基础集合，不含通配符）
    invalid_chars = {",", "#", "/", "\\", '"', "<", ">", "|", " ", "\t", "\n", "\r"}

    # 如果不允许通配符，将通配符也加入无效字符集
    if not allow_wildcards:
        invalid_chars.update({"*", "?"})

    if any(char in invalid_chars for char in index_name):
        return False

    return True


class IndexManager:
    """索引管理器核心类.

    提供企业级的索引管理功能，包括：
    - 索引创建与删除
    - 滚动索引策略
    - 别名管理
    - 索引模板管理
    - 索引生命周期管理（ILM）

    Args:
        es_client: Elasticsearch 客户端实例
    """

    def __init__(self, es_client: Elasticsearch):
        if es_client is None:
            raise ValueError("es_client 不能为 None")
        self.es_client = es_client
        logger.info("初始化索引管理器")

    def create_index(
        self,
        index_name: str,
        mappings: IndexMappings | None = None,
        settings: IndexSettings | None = None,
    ) -> bool:
        """创建索引.

        Args:
            index_name: 索引名称
            mappings: 索引映射配置
            settings: 索引设置配置，支持 number_of_shards、number_of_replicas、refresh_interval 等

        Returns:
            是否成功创建索引

        Raises:
            IndexAlreadyExistsError: 索引已存在时抛出
            ValueError: 索引名称不符合 Elasticsearch 规范时抛出

        Example:
            >>> manager = IndexManager(es_client)
            >>> mappings = {
            ...     "properties": {
            ...         "name": {"type": "keyword"},
            ...         "age": {"type": "integer"},
            ...     }
            ... }
            >>> manager.create_index("users", mappings=mappings)
        """
        # 验证索引名称
        if not _validate_index_name(index_name):
            raise ValueError(f"索引名称 '{index_name}' 不符合 Elasticsearch 规范")

        body: dict[str, Any] = {}
        if mappings:
            body["mappings"] = mappings
        if settings:
            body["settings"] = settings

        try:
            response = self.es_client.indices.create(index=index_name, body=body)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 创建成功")
                return True
            return False

        except RequestError as e:
            if e.status_code == 400 and "resource_already_exists_exception" in str(e):
                raise IndexAlreadyExistsError(f"索引 '{index_name}' 已存在") from e
            raise IndexManagerError(f"创建索引 '{index_name}' 失败: {str(e)}") from e
        except Exception as e:
            raise IndexManagerError(f"创建索引 '{index_name}' 失败: {str(e)}") from e

    def delete_index(self, index_name: str) -> bool:
        """删除索引.

        Args:
            index_name: 索引名称（支持通配符）

        Returns:
            是否成功删除索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.delete_index("users")
        """
        # 检查是否包含通配符，警告可能批量删除
        if "*" in index_name or "?" in index_name:
            logger.warning(f"索引名称 '{index_name}' 包含通配符，可能会删除多个索引！")

        try:
            if not self.index_exists(index_name):
                logger.warning(f"索引 '{index_name}' 不存在，无法删除")
                return False

            response = self.es_client.indices.delete(index=index_name)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 删除成功")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"删除索引 '{index_name}' 失败: {str(e)}") from e

    def index_exists(self, index_name: str) -> bool:
        """检查索引是否存在.

        Args:
            index_name: 索引名称（支持通配符）

        Returns:
            索引是否存在

        Example:
            >>> manager = IndexManager(es_client)
            >>> exists = manager.index_exists("users")
            >>> print(exists)
        """
        try:
            return self.es_client.indices.exists(index=index_name)
        except Exception as e:
            logger.warning(f"检查索引 '{index_name}' 是否存在时出错: {str(e)}")
            return False

    def get_index(self, index_name: str) -> IndexInfo | None:
        """获取索引信息.

        Args:
            index_name: 索引名称

        Returns:
            索引信息对象，如果索引不存在则返回 None

        Example:
            >>> manager = IndexManager(es_client)
            >>> info = manager.get_index("users")
            >>> print(f"文档数: {info.docs_count}")
        """
        try:
            if not self.index_exists(index_name):
                return None

            response = self.es_client.indices.get(index=index_name)
            stats = self.es_client.indices.stats(index=index_name)

            # 获取索引统计信息
            indices_stats = stats.get("indices", {})
            index_stats = list(indices_stats.values())[0] if indices_stats else {}

            # 获取索引设置和映射
            index_data = list(response.values())[0]

            # 提取别名
            aliases = list(index_data.get("aliases", {}).keys())

            # 提取健康状态
            health = ""
            status = ""
            try:
                cat_response = self.es_client.cat.indices(
                    index=index_name, format="json"
                )
                if cat_response:
                    health = cat_response[0].get("health", "")
                    status = cat_response[0].get("status", "")
            except Exception as e:
                logger.warning(f"获取索引 '{index_name}' 健康状态失败: {str(e)}")
                health = ""
                status = ""

            # 提取索引创建时间
            creation_date = int(
                index_data.get("settings", {}).get("index", {}).get("creation_date", 0)
            )

            return IndexInfo(
                name=index_name,
                aliases=aliases,
                mappings=index_data.get("mappings", {}),
                settings=index_data.get("settings", {}),
                docs_count=index_stats.get("primaries", {})
                .get("docs", {})
                .get("count", 0),
                store_size=index_stats.get("primaries", {})
                .get("store", {})
                .get("size_in_bytes", 0),
                health=health,
                status=status,
                creation_date=creation_date,
            )

        except NotFoundError:
            return None
        except Exception as e:
            logger.warning(f"获取索引 '{index_name}' 信息失败: {str(e)}")
            return None

    def get_index_or_raise(self, index_name: str) -> IndexInfo:
        """获取索引信息，如果不存在则抛出异常.

        与 get_index 方法不同，此方法在索引不存在时抛出 IndexNotFoundError 异常，
        而不是返回 None。适用于需要确保索引存在的场景。

        Args:
            index_name: 索引名称

        Returns:
            索引信息对象

        Raises:
            IndexNotFoundError: 当索引不存在时抛出

        Example:
            >>> manager = IndexManager(es_client)
            >>> try:
            ...     info = manager.get_index_or_raise("users")
            ...     print(f"文档数: {info.docs_count}")
            ... except IndexNotFoundError:
            ...     print("索引不存在")
        """
        info = self.get_index(index_name)
        if info is None:
            raise IndexNotFoundError(f"索引 '{index_name}' 不存在")
        return info

    def list_indices(
        self,
        pattern: str = "*",
        health: str | None = None,
        status: str | None = None,
    ) -> list[IndexInfo]:
        """列出所有索引，支持过滤条件.

        Args:
            pattern: 索引匹配模式，默认为 "*"
            health: 健康状态过滤（"green", "yellow", "red"），默认不过滤
            status: 索引状态过滤（"open", "close"），默认不过滤

        Returns:
            索引信息列表

        Example:
            >>> manager = IndexManager(es_client)
            >>> # 列出所有 logs-* 索引
            >>> indices = manager.list_indices("logs-*")
            >>> # 仅列出健康状态为 green 的索引
            >>> healthy_indices = manager.list_indices("*", health="green")
            >>> # 仅列出打开状态的索引
            >>> open_indices = manager.list_indices("*", status="open")
            >>> for info in indices:
            ...     print(f"{info.name}: {info.docs_count} 文档")
        """
        indices: list[IndexInfo] = []

        try:
            # 批量获取索引信息和统计信息
            response = self.es_client.indices.get(index=pattern)
            stats = self.es_client.indices.stats(index=pattern)
            indices_stats = stats.get("indices", {})

            # 批量获取健康状态信息
            health_status_map: dict[str, dict[str, str]] = {}
            try:
                cat_response = self.es_client.cat.indices(index=pattern, format="json")
                for cat_info in cat_response:
                    idx_name = cat_info.get("index", "")
                    if idx_name:
                        health_status_map[idx_name] = {
                            "health": cat_info.get("health", ""),
                            "status": cat_info.get("status", ""),
                        }
            except Exception as e:
                logger.warning(f"获取索引健康状态失败: {str(e)}，将使用空值")

            for index_name, index_data in response.items():
                try:
                    index_stats = indices_stats.get(index_name, {})
                    aliases = list(index_data.get("aliases", {}).keys())

                    # 提取索引创建时间
                    creation_date = int(
                        index_data.get("settings", {})
                        .get("index", {})
                        .get("creation_date", 0)
                    )

                    # 从批量获取的健康状态中提取
                    idx_health_status = health_status_map.get(index_name, {})
                    idx_health = idx_health_status.get("health", "")
                    idx_status = idx_health_status.get("status", "")

                    # 应用过滤条件
                    if health and idx_health != health:
                        continue
                    if status and idx_status != status:
                        continue

                    # 直接构建 IndexInfo，避免多次 API 调用
                    info = IndexInfo(
                        name=index_name,
                        aliases=aliases,
                        mappings=index_data.get("mappings", {}),
                        settings=index_data.get("settings", {}),
                        docs_count=index_stats.get("primaries", {})
                        .get("docs", {})
                        .get("count", 0),
                        store_size=index_stats.get("primaries", {})
                        .get("store", {})
                        .get("size_in_bytes", 0),
                        health=idx_health,
                        status=idx_status,
                        creation_date=creation_date,
                    )
                    indices.append(info)
                except Exception as e:
                    logger.warning(f"获取索引 '{index_name}' 信息失败: {str(e)}")

        except NotFoundError:
            return indices
        except Exception as e:
            logger.warning(f"列出索引失败: {str(e)}")

        return indices

    def bulk_create_indices(
        self,
        index_configs: list[dict[str, Any]],
    ) -> dict[str, bool]:
        """批量创建索引.

        Args:
            index_configs: 索引配置列表，每个配置包含：
                - index_name: 索引名称
                - mappings: 索引映射配置（可选）
                - settings: 索引设置配置（可选）

        Returns:
            字典，键为索引名称，值为是否创建成功

        Example:
            >>> manager = IndexManager(es_client)
            >>> configs = [
            ...     {"index_name": "users-001"},
            ...     {"index_name": "users-002", "mappings": {...}},
            ... ]
            >>> results = manager.bulk_create_indices(configs)
        """
        results: dict[str, bool] = {}

        for config in index_configs:
            index_name = config.get("index_name")
            if not index_name:
                logger.warning("批量创建索引：配置中缺少 index_name")
                continue

            # 验证索引名称
            if not _validate_index_name(index_name):
                logger.warning(f"批量创建索引：索引名称 '{index_name}' 不符合规范")
                results[index_name] = False
                continue

            try:
                success = self.create_index(
                    index_name=index_name,
                    mappings=config.get("mappings"),
                    settings=config.get("settings"),
                )
                results[index_name] = success
            except Exception as e:
                logger.warning(f"批量创建索引 '{index_name}' 失败: {str(e)}")
                results[index_name] = False

        return results

    def bulk_delete_indices(
        self,
        index_names: list[str],
    ) -> dict[str, bool]:
        """批量删除索引.

        Args:
            index_names: 索引名称列表（支持通配符，慎用！）

        Returns:
            字典，键为索引名称，值为是否删除成功

        Example:
            >>> manager = IndexManager(es_client)
            >>> results = manager.bulk_delete_indices(["users-001", "users-002"])
        """
        # 检查是否包含通配符，警告可能批量删除
        wildcard_indices = [name for name in index_names if "*" in name or "?" in name]
        if wildcard_indices:
            logger.warning(
                f"检测到包含通配符的索引名称: {wildcard_indices}，"
                "可能会意外删除多个索引，请谨慎操作！"
            )

        results: dict[str, bool] = {}

        for index_name in index_names:
            try:
                success = self.delete_index(index_name=index_name)
                results[index_name] = success
            except Exception as e:
                logger.warning(f"批量删除索引 '{index_name}' 失败: {str(e)}")
                results[index_name] = False

        return results

    def bulk_put_settings(
        self,
        index_names: list[str],
        settings: IndexSettings,
    ) -> dict[str, bool]:
        """批量更新索引设置.

        Args:
            index_names: 索引名称列表
            settings: 要更新的设置

        Returns:
            字典，键为索引名称，值为是否更新成功

        Example:
            >>> manager = IndexManager(es_client)
            >>> settings = {"index": {"refresh_interval": "1s"}}
            >>> results = manager.bulk_put_settings(
            ...     ["users-001", "users-002"], settings
            ... )
        """
        results: dict[str, bool] = {}

        for index_name in index_names:
            try:
                success = self.put_settings(index_name=index_name, settings=settings)
                results[index_name] = success
            except Exception as e:
                logger.warning(f"批量更新索引 '{index_name}' 设置失败: {str(e)}")
                results[index_name] = False

        return results

    def put_settings(
        self,
        index_name: str,
        settings: IndexSettings,
    ) -> bool:
        """更新索引设置.

        Args:
            index_name: 索引名称（支持通配符）
            settings: 要更新的设置，格式如 {"index": {"refresh_interval": "1s"}}

        Returns:
            是否成功更新设置

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.put_settings("users", {"index": {"refresh_interval": "1s"}})
        """
        try:
            response = self.es_client.indices.put_settings(
                index=index_name,
                body=settings,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 设置更新成功")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"更新索引 '{index_name}' 设置失败: {str(e)}"
            ) from e

    def create_rollover_index(
        self,
        alias: str,
        initial_index: str,
        mappings: IndexMappings | None = None,
        settings: IndexSettings | None = None,
        is_write_index: bool = True,
    ) -> bool:
        """创建支持滚动的索引结构.

        创建初始索引并设置别名，后续可以通过 rollover_index 滚动到新索引。

        注意：此方法支持事务处理，如果别名创建失败，会自动回滚已创建的索引。

        Args:
            alias: 滚动别名名称
            initial_index: 初始索引名称（通常以 -000001 结尾）
            mappings: 索引映射配置
            settings: 索引设置配置
            is_write_index: 是否设置为写索引，默认为 True

        Returns:
            是否成功创建滚动索引结构

        Raises:
            IndexManagerError: 创建失败时抛出

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.create_rollover_index(
            ...     "logs",
            ...     "logs-000001",
            ...     mappings={"properties": {"timestamp": {"type": "date"}}},
            ... )
        """
        index_created = False

        try:
            # 构建索引设置
            index_settings = settings or {}

            # 创建初始索引
            body: dict[str, Any] = {"settings": index_settings}
            if mappings:
                body["mappings"] = mappings

            # 创建索引
            create_response = self.es_client.indices.create(
                index=initial_index,
                body=body,
            )
            index_created = True  # 标记索引已创建，用于回滚

            # 创建别名并指向初始索引
            alias_body: dict[str, Any] | None = None
            if is_write_index:
                alias_body = {"is_write_index": True}

            alias_response = self.es_client.indices.put_alias(
                index=initial_index,
                name=alias,
                body=alias_body,
            )

            acknowledged = create_response.get(
                "acknowledged", False
            ) and alias_response.get("acknowledged", False)

            if acknowledged:
                logger.info(
                    f"滚动索引结构创建成功: 别名 '{alias}' -> 索引 '{initial_index}'"
                )
                return True
            else:
                # 如果别名创建失败，回滚已创建的索引
                logger.warning(f"别名创建失败，回滚索引 '{initial_index}'")
                try:
                    self.es_client.indices.delete(index=initial_index)
                except Exception as rollback_error:
                    logger.error(
                        f"回滚索引 '{initial_index}' 失败: {str(rollback_error)}"
                    )
                return False

        except Exception as e:
            # 如果出现异常且索引已创建，尝试回滚
            if index_created:
                logger.warning(f"创建滚动索引结构失败，回滚索引 '{initial_index}'")
                try:
                    self.es_client.indices.delete(index=initial_index)
                except Exception as rollback_error:
                    logger.error(
                        f"回滚索引 '{initial_index}' 失败: {str(rollback_error)}"
                    )
            raise IndexManagerError(
                f"创建滚动索引结构失败 (别名: '{alias}', 索引: '{initial_index}'): {str(e)}"
            ) from e

    def rollover_index(
        self,
        alias: str,
        conditions: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> RolloverInfo:
        """滚动索引.

        当满足条件时，创建新索引并将别名指向新索引。

        Args:
            alias: 滚动别名名称
            conditions: 滚动条件，例如：
                {"max_age": "7d", "max_docs": 1000000, "max_size": "50GB"}
                如果为 None，则无条件执行滚动操作
            dry_run: 是否为试运行（不实际创建新索引）

        Returns:
            滚动索引信息

        Example:
            >>> manager = IndexManager(es_client)
            >>> result = manager.rollover_index("logs", conditions={"max_age": "7d"})
            >>> if result.rolled_over:
            ...     print(f"滚动成功: {result.old_index} -> {result.new_index}")
        """
        try:
            # 获取当前索引
            indices_by_alias = self.get_indices_by_alias(alias)
            if not indices_by_alias:
                raise AliasNotFoundError(f"别名 '{alias}' 不存在或未指向任何索引")

            old_index = indices_by_alias[0]

            # 执行滚动
            if conditions:
                body = {"conditions": conditions}
            else:
                body = None

            response = self.es_client.indices.rollover(
                alias=alias,
                body=body,
                dry_run=dry_run,
            )

            # 解析响应
            rollover_info = RolloverInfo(
                alias=alias,
                old_index=old_index,
                dry_run=dry_run,
            )

            # 检查是否真的发生了滚动
            new_index = response.get("new_index")

            if new_index and old_index != new_index:
                rollover_info.new_index = new_index
                rollover_info.rolled_over = True
                logger.info(f"索引滚动成功: '{old_index}' -> '{new_index}'")
            else:
                rollover_info.rolled_over = False
                logger.info(f"索引滚动条件未满足，保持当前索引: '{old_index}'")

            # 提取条件状态
            conditions_info = response.get("conditions", {})
            rollover_info.conditions = conditions
            rollover_info.condition_status = conditions_info

            return rollover_info

        except NotFoundError as e:
            raise AliasNotFoundError(f"别名 '{alias}' 不存在") from e
        except Exception as e:
            raise RolloverError(f"滚动索引失败 (别名: '{alias}'): {str(e)}") from e

    def get_rollover_info(self, alias: str) -> RolloverInfo:
        """获取滚动索引信息.

        获取别名的当前滚动状态和索引信息。

        Args:
            alias: 滚动别名名称

        Returns:
            滚动索引信息

        Example:
            >>> manager = IndexManager(es_client)
            >>> info = manager.get_rollover_info("logs")
            >>> print(f"当前索引: {info.old_index}")
        """
        try:
            indices_by_alias = self.get_indices_by_alias(alias)
            if not indices_by_alias:
                raise AliasNotFoundError(f"别名 '{alias}' 不存在或未指向任何索引")

            old_index = indices_by_alias[0]

            return RolloverInfo(
                alias=alias,
                old_index=old_index,
                new_index=None,
                rolled_over=False,
                conditions=None,
                condition_status={},
                dry_run=False,
            )

        except Exception as e:
            raise RolloverError(
                f"获取滚动索引信息失败 (别名: '{alias}'): {str(e)}"
            ) from e

    def create_alias(
        self,
        index_name: str,
        alias_name: str,
        is_write_index: bool = False,
        filter_query: dict[str, Any] | None = None,
        routing: str | None = None,
    ) -> bool:
        """为索引创建别名.

        Args:
            index_name: 索引名称
            alias_name: 别名名称
            is_write_index: 是否为写索引
            filter_query: 别名过滤条件
            routing: 别名路由配置

        Returns:
            是否成功创建别名

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.create_alias("users-2024", "users-current")
        """
        try:
            if not self.index_exists(index_name):
                logger.warning(f"索引 '{index_name}' 不存在，无法创建别名")
                return False

            body: dict[str, Any] = {}
            if is_write_index:
                body["is_write_index"] = True
            if filter_query:
                body["filter"] = filter_query
            if routing:
                body["routing"] = routing

            response = self.es_client.indices.put_alias(
                index=index_name,
                name=alias_name,
                body=body if body else None,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"别名 '{alias_name}' 创建成功，指向索引 '{index_name}'")
                return True
            return False

        except Exception as e:
            raise IndexManagerError(f"创建别名失败: {str(e)}") from e

    def delete_alias(
        self,
        index_name: str,
        alias_name: str,
    ) -> bool:
        """删除索引别名.

        Args:
            index_name: 索引名称
            alias_name: 别名名称

        Returns:
            是否成功删除别名

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.delete_alias("users-2024", "users-current")
        """
        try:
            response = self.es_client.indices.delete_alias(
                index=index_name,
                name=alias_name,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"别名 '{alias_name}' 删除成功")
                return True
            return False

        except NotFoundError:
            logger.warning(f"别名 '{alias_name}' 或索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"删除别名失败: {str(e)}") from e

    def add_alias_to_index(
        self,
        index_name: str,
        alias_name: str,
        is_write_index: bool = False,
        filter_query: dict[str, Any] | None = None,
        routing: str | None = None,
    ) -> bool:
        """为指定索引添加别名.

        这是 create_alias 方法的别名，提供更直观的方法命名。

        Args:
            index_name: 索引名称
            alias_name: 别名名称
            is_write_index: 是否为写索引
            filter_query: 别名过滤条件
            routing: 别名路由配置

        Returns:
            是否成功添加别名

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.add_alias_to_index("users-2024", "users-read")
        """
        return self.create_alias(
            index_name=index_name,
            alias_name=alias_name,
            is_write_index=is_write_index,
            filter_query=filter_query,
            routing=routing,
        )

    def remove_alias_from_index(
        self,
        index_name: str,
        alias_name: str,
    ) -> bool:
        """从指定索引移除别名.

        这是 delete_alias 方法的别名，提供更直观的方法命名。

        Args:
            index_name: 索引名称
            alias_name: 别名名称

        Returns:
            是否成功移除别名

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.remove_alias_from_index("users-2024", "users-read")
        """
        return self.delete_alias(index_name=index_name, alias_name=alias_name)

    def get_aliases(self, index_name: str) -> list[AliasInfo]:
        """获取索引的所有别名.

        Args:
            index_name: 索引名称

        Returns:
            别名信息列表

        Example:
            >>> manager = IndexManager(es_client)
            >>> aliases = manager.get_aliases("users-2024")
            >>> for alias in aliases:
            ...     print(f"别名: {alias.name}")
        """
        alias_infos: list[AliasInfo] = []

        try:
            response = self.es_client.indices.get_alias(index=index_name)

            for idx_name, alias_data in response.items():
                aliases = alias_data.get("aliases", {})
                for alias_name, alias_config in aliases.items():
                    alias_info = AliasInfo(
                        name=alias_name,
                        indices=[idx_name],
                        filter_query=alias_config.get("filter"),
                        routing=alias_config.get("routing"),
                        is_write_index=alias_config.get("is_write_index", False),
                    )
                    alias_infos.append(alias_info)

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
        except Exception as e:
            logger.warning(f"获取索引 '{index_name}' 别名失败: {str(e)}")

        return alias_infos

    def get_indices_by_alias(self, alias_name: str) -> list[str]:
        """获取别名指向的所有索引.

        Args:
            alias_name: 别名名称

        Returns:
            索引名称列表

        Example:
            >>> manager = IndexManager(es_client)
            >>> indices = manager.get_indices_by_alias("logs")
            >>> for index in indices:
            ...     print(index)
        """
        try:
            response = self.es_client.indices.get_alias(name=alias_name)
            return list(response.keys())
        except NotFoundError:
            return []
        except Exception as e:
            logger.warning(f"获取别名 '{alias_name}' 指向的索引失败: {str(e)}")
            return []

    def create_index_template(
        self,
        template_name: str,
        index_patterns: list[str],
        priority: int | None = None,
        composed_of: list[str] | None = None,
        version: int | None = None,
        mappings: IndexMappings | None = None,
        settings: IndexSettings | None = None,
    ) -> bool:
        """创建索引模板（兼容 ES 7.8+ Index Template V2）.

        注意：此方法使用 Index Template V2 API，mappings 和 settings 会被
        放入 template 对象中，以兼容 ES 7.8+ 版本。

        Args:
            template_name: 模板名称
            index_patterns: 索引匹配模式列表
            priority: 模板优先级
            composed_of: 组件模板列表
            version: 模板版本
            mappings: 映射配置
            settings: 设置配置

        Returns:
            是否成功创建模板

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.create_index_template(
            ...     "logs-template",
            ...     ["logs-*"],
            ...     mappings={"properties": {"timestamp": {"type": "date"}}},
            ... )
        """
        try:
            body: dict[str, Any] = {
                "index_patterns": index_patterns,
            }

            if priority is not None:
                body["priority"] = priority
            if composed_of:
                body["composed_of"] = composed_of
            if version is not None:
                body["version"] = version

            # ES 7.8+ Index Template V2：mappings 和 settings 需要放在 template 对象中
            if mappings or settings:
                body["template"] = {}
                if mappings:
                    body["template"]["mappings"] = mappings
                if settings:
                    body["template"]["settings"] = settings

            response = self.es_client.indices.put_index_template(
                name=template_name,
                body=body,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引模板 '{template_name}' 创建成功")
                return True
            return False

        except Exception as e:
            raise IndexManagerError(
                f"创建索引模板 '{template_name}' 失败: {str(e)}"
            ) from e

    def delete_index_template(self, template_name: str) -> bool:
        """删除索引模板.

        Args:
            template_name: 模板名称

        Returns:
            是否成功删除模板

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.delete_index_template("logs-template")
        """
        try:
            response = self.es_client.indices.delete_index_template(
                name=template_name,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引模板 '{template_name}' 删除成功")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引模板 '{template_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"删除索引模板 '{template_name}' 失败: {str(e)}"
            ) from e

    def get_index_template(self, template_name: str) -> IndexTemplateInfo | None:
        """获取索引模板信息.

        注意：此方法兼容 ES 7.8+ Index Template V2，会从 template 对象中
        提取 mappings 和 settings。

        Args:
            template_name: 模板名称

        Returns:
            索引模板信息，如果模板不存在则返回 None

        Example:
            >>> manager = IndexManager(es_client)
            >>> template = manager.get_index_template("logs-template")
            >>> print(template.mappings)
        """
        try:
            response = self.es_client.indices.get_index_template(name=template_name)

            for template_data in response.get("index_templates", []):
                if template_data.get("name") == template_name:
                    template_obj = template_data.get("index_template", {})
                    # ES 7.8+ Index Template V2：mappings 和 settings 在 template 对象中
                    template_inner = template_obj.get("template", {})
                    return IndexTemplateInfo(
                        name=template_name,
                        index_patterns=template_obj.get("index_patterns", []),
                        priority=template_obj.get("priority"),
                        composed_of=template_obj.get("composed_of", []),
                        version=template_obj.get("version"),
                        mappings=template_inner.get("mappings", {}),
                        settings=template_inner.get("settings", {}),
                    )

            return None

        except NotFoundError:
            return None
        except Exception as e:
            logger.error(f"获取索引模板 '{template_name}' 失败: {str(e)}")
            return None

    def list_index_templates(self) -> list[IndexTemplateInfo]:
        """列出所有索引模板.

        注意：此方法兼容 ES 7.8+ Index Template V2，会从 template 对象中
        提取 mappings 和 settings。

        Returns:
            索引模板信息列表

        Example:
            >>> manager = IndexManager(es_client)
            >>> templates = manager.list_index_templates()
            >>> for template in templates:
            ...     print(f"{template.name}: {template.index_patterns}")
        """
        template_infos: list[IndexTemplateInfo] = []

        try:
            response = self.es_client.indices.get_index_template()

            for template_data in response.get("index_templates", []):
                name = template_data.get("name", "")
                template_obj = template_data.get("index_template", {})
                # ES 7.8+ Index Template V2：mappings 和 settings 在 template 对象中
                template_inner = template_obj.get("template", {})
                template_info = IndexTemplateInfo(
                    name=name,
                    index_patterns=template_obj.get("index_patterns", []),
                    priority=template_obj.get("priority"),
                    composed_of=template_obj.get("composed_of", []),
                    version=template_obj.get("version"),
                    mappings=template_inner.get("mappings", {}),
                    settings=template_inner.get("settings", {}),
                )
                template_infos.append(template_info)

        except NotFoundError:
            return template_infos
        except Exception as e:
            logger.error(f"列出索引模板失败: {str(e)}")

        return template_infos

    def create_ilm_policy(
        self,
        policy_name: str,
        phases: dict[str, dict[str, Any]],
        version: int | None = None,
    ) -> bool:
        """创建索引生命周期管理（ILM）策略.

        Args:
            policy_name: 策略名称
            phases: 阶段配置字典，包含 hot、warm、cold、delete 阶段配置
            version: 策略版本

        Returns:
            是否成功创建策略

        Example:
            >>> manager = IndexManager(es_client)
            >>> phases = {
            ...     "hot": {
            ...         "min_age": "0ms",
            ...         "actions": {"rollover": {"max_size": "50GB", "max_age": "30d"}},
            ...     },
            ...     "delete": {"min_age": "90d", "actions": {"delete": {}}},
            ... }
            >>> manager.create_ilm_policy("logs_policy", phases)
        """
        try:
            body: dict[str, Any] = {
                "policy": {"phases": phases},
            }

            if version is not None:
                body["policy"]["version"] = version

            response = self.es_client.ilm.put_lifecycle(
                policy=policy_name,
                body=body,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"ILM策略 '{policy_name}' 创建成功")
                return True
            return False

        except Exception as e:
            raise IndexManagerError(
                f"创建ILM策略 '{policy_name}' 失败: {str(e)}"
            ) from e

    def attach_ilm_policy(
        self,
        index_name: str,
        policy_name: str,
    ) -> bool:
        """将ILM策略应用到索引.

        Args:
            index_name: 索引名称
            policy_name: ILM策略名称

        Returns:
            是否成功应用策略

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.attach_ilm_policy("logs-000001", "logs_policy")
        """
        try:
            settings = {"index": {"lifecycle": {"name": policy_name}}}
            return self.put_settings(index_name, settings)

        except Exception as e:
            raise IndexManagerError(
                f"应用ILM策略 '{policy_name}' 到索引 '{index_name}' 失败: {str(e)}"
            ) from e

    def attach_ilm_policy_to_template(
        self,
        template_name: str,
        policy_name: str,
        rollover_alias: str | None = None,
    ) -> bool:
        """将ILM策略应用到索引模板.

        Args:
            template_name: 索引模板名称
            policy_name: ILM策略名称
            rollover_alias: 滚动别名，默认为模板名称

        Returns:
            是否成功应用策略

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.attach_ilm_policy_to_template("logs-template", "logs_policy")
        """
        try:
            template = self.get_index_template(template_name)
            if not template:
                raise TemplateNotFoundError(f"索引模板 '{template_name}' 不存在")

            # 更新模板设置
            settings = template.settings or {}
            settings.setdefault("index", {})
            settings["index"]["lifecycle"] = {
                "name": policy_name,
                "rollover_alias": rollover_alias or template_name,
            }

            # 使用 put_index_template 直接覆盖更新模板设置
            return self.create_index_template(
                template_name=template_name,
                index_patterns=template.index_patterns,
                priority=template.priority,
                composed_of=template.composed_of,
                version=template.version,
                mappings=template.mappings,
                settings=settings,
            )

        except Exception as e:
            raise IndexManagerError(
                f"应用ILM策略 '{policy_name}' 到模板 '{template_name}' 失败: {str(e)}"
            ) from e

    def get_ilm_policy(self, policy_name: str) -> ILMPolicyInfo | None:
        """获取ILM策略信息.

        Args:
            policy_name: 策略名称

        Returns:
            ILM策略信息，如果策略不存在则返回 None

        Example:
            >>> manager = IndexManager(es_client)
            >>> policy = manager.get_ilm_policy("logs_policy")
            >>> if policy:
            ...     print(policy.phases)
        """
        try:
            response = self.es_client.ilm.get_lifecycle(policy=policy_name)
            policy_data = response.get(policy_name, {})
            policy_obj = policy_data.get("policy", {})

            # 解析阶段配置
            phases_dict = policy_obj.get("phases", {})
            phases = {}

            for phase_name, phase_config in phases_dict.items():
                phases[phase_name] = ILMPhase(
                    name=phase_name,
                    min_age=phase_config.get("min_age", "0ms"),
                    actions=phase_config.get("actions", {}),
                )

            return ILMPolicyInfo(
                name=policy_name,
                phases=phases,
                version=policy_obj.get("version"),
                modified_date=policy_obj.get("modified_date"),
            )

        except NotFoundError:
            logger.warning(f"ILM策略 '{policy_name}' 不存在")
            return None
        except Exception as e:
            logger.warning(f"获取ILM策略 '{policy_name}' 失败: {str(e)}")
            return None

    def delete_ilm_policy(self, policy_name: str) -> bool:
        """删除ILM策略.

        Args:
            policy_name: 策略名称

        Returns:
            是否成功删除策略

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.delete_ilm_policy("logs_policy")
        """
        try:
            response = self.es_client.ilm.delete_lifecycle(policy=policy_name)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"ILM策略 '{policy_name}' 删除成功")
                return True
            return False

        except NotFoundError:
            logger.warning(f"ILM策略 '{policy_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"删除ILM策略 '{policy_name}' 失败: {str(e)}"
            ) from e

    def get_ilm_status(self, index_name: str) -> ILMIndexStatus | None:
        """获取索引的ILM执行状态.

        Args:
            index_name: 索引名称

        Returns:
            ILM索引状态信息，如果索引未应用ILM则返回 None

        Example:
            >>> manager = IndexManager(es_client)
            >>> status = manager.get_ilm_status("logs-000001")
            >>> print(f"当前阶段: {status.phase}")
        """
        try:
            if not self.index_exists(index_name):
                return None

            response = self.es_client.ilm.explain_lifecycle(index=index_name)
            indices = response.get("indices", {})

            if index_name not in indices:
                return None

            index_data = indices[index_name]

            return ILMIndexStatus(
                index=index_name,
                step=index_data.get("step", ""),
                phase=index_data.get("phase", ""),
                action=index_data.get("action", ""),
                step_info=index_data.get("step_info"),
                version=index_data.get("policy", {}).get("version"),
                failed_step=index_data.get("failed_step"),
            )

        except NotFoundError:
            return None
        except Exception as e:
            logger.warning(f"获取索引 '{index_name}' ILM状态失败: {str(e)}")
            return None

    def list_ilm_policies(self) -> list[str]:
        """列出所有ILM策略名称.

        Returns:
            ILM策略名称列表

        Example:
            >>> manager = IndexManager(es_client)
            >>> policies = manager.list_ilm_policies()
            >>> for policy in policies:
            ...     print(policy)
        """
        try:
            response = self.es_client.ilm.get_lifecycle()
            return list(response.keys())
        except Exception as e:
            logger.warning(f"列出ILM策略失败: {str(e)}")
            return []

    def get_ilm_policy_or_raise(self, policy_name: str) -> ILMPolicyInfo:
        """获取ILM策略信息，如果不存在则抛出异常.

        与 get_ilm_policy 方法不同，此方法在策略不存在时抛出 ILMNotFoundError 异常，
        而不是返回 None。适用于需要确保策略存在的场景。

        Args:
            policy_name: 策略名称

        Returns:
            ILM策略信息

        Raises:
            ILMNotFoundError: 当策略不存在时抛出

        Example:
            >>> manager = IndexManager(es_client)
            >>> try:
            ...     policy = manager.get_ilm_policy_or_raise("logs_policy")
            ...     print(policy.phases)
            ... except ILMNotFoundError:
            ...     print("策略不存在")
        """
        policy = self.get_ilm_policy(policy_name)
        if policy is None:
            raise ILMNotFoundError(f"ILM策略 '{policy_name}' 不存在")
        return policy

    # ==================== 索引操作方法 ====================

    def refresh_index(self, index_name: str) -> bool:
        """刷新索引，使最近的文档可见.

        Args:
            index_name: 索引名称（支持通配符）

        Returns:
            是否成功刷新索引。
            - True: 所有分片刷新成功
            - False: 部分或全部刷新失败（包括索引不存在）

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.refresh_index("users")
        """
        try:
            response = self.es_client.indices.refresh(index=index_name)
            # refresh 返回 _shards 信息
            shards = response.get("_shards", {})
            failed = shards.get("failed", 0)
            if failed == 0:
                logger.info(f"索引 '{index_name}' 刷新成功")
                return True
            logger.warning(
                f"索引 '{index_name}' 部分刷新失败: "
                f"{failed}/{shards.get('total', 0)} 个分片失败"
            )
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"刷新索引 '{index_name}' 失败: {str(e)}") from e

    def open_index(self, index_name: str) -> bool:
        """打开已关闭的索引.

        Args:
            index_name: 索引名称（支持通配符）

        Returns:
            是否成功打开索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.open_index("users")
        """
        try:
            response = self.es_client.indices.open(index=index_name)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 已打开")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"打开索引 '{index_name}' 失败: {str(e)}") from e

    def close_index(self, index_name: str) -> bool:
        """关闭索引.

        关闭的索引不接受读写操作，但保留元数据。

        Args:
            index_name: 索引名称（支持通配符）

        Returns:
            是否成功关闭索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.close_index("users")
        """
        try:
            response = self.es_client.indices.close(index=index_name)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 已关闭")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"关闭索引 '{index_name}' 失败: {str(e)}") from e

    def clone_index(
        self,
        source_index: str,
        target_index: str,
        settings: IndexSettings | None = None,
    ) -> bool:
        """克隆索引.

        将源索引克隆到目标索引。源索引必须处于只读状态或已关闭状态。

        注意：克隆前需要将源索引设置为只读：
        manager.put_settings(source_index, {"index": {"blocks": {"write": True}}})

        Args:
            source_index: 源索引名称
            target_index: 目标索引名称
            settings: 目标索引的设置（可选）

        Returns:
            是否成功克隆索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> # 先设置源索引为只读
            >>> manager.put_settings("users", {"index": {"blocks": {"write": True}}})
            >>> # 克隆索引
            >>> manager.clone_index("users", "users-clone")
        """
        try:
            body: dict[str, Any] = {}
            if settings:
                body["settings"] = settings

            response = self.es_client.indices.clone(
                index=source_index,
                target=target_index,
                body=body if body else None,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{source_index}' 已克隆到 '{target_index}'")
                return True
            return False

        except NotFoundError:
            logger.warning(f"源索引 '{source_index}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"克隆索引 '{source_index}' 到 '{target_index}' 失败: {str(e)}"
            ) from e

    def shrink_index(
        self,
        source_index: str,
        target_index: str,
        number_of_shards: int = 1,
        settings: IndexSettings | None = None,
    ) -> bool:
        """收缩索引分片.

        将源索引的分片数量收缩到更少的分片。源索引必须处于只读状态，
        且所有分片必须分配到同一节点。

        注意：收缩前需要准备源索引：
        1. 设置只读：manager.put_settings(source_index, {"index": {"blocks": {"write": True}}})
        2. 分配到单节点（可选，ES会自动处理）

        Args:
            source_index: 源索引名称
            target_index: 目标索引名称
            number_of_shards: 目标分片数量，必须是源分片数的因数
            settings: 目标索引的额外设置（可选）

        Returns:
            是否成功收缩索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> # 先设置源索引为只读
            >>> manager.put_settings("users", {"index": {"blocks": {"write": True}}})
            >>> # 收缩索引到 1 个分片
            >>> manager.shrink_index("users", "users-shrink", number_of_shards=1)
        """
        try:
            body: dict[str, Any] = {
                "settings": {
                    "index.number_of_shards": number_of_shards,
                }
            }
            if settings:
                body["settings"].update(settings)

            response = self.es_client.indices.shrink(
                index=source_index,
                target=target_index,
                body=body,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(
                    f"索引 '{source_index}' 已收缩到 '{target_index}' "
                    f"({number_of_shards} 个分片)"
                )
                return True
            return False

        except NotFoundError:
            logger.warning(f"源索引 '{source_index}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"收缩索引 '{source_index}' 到 '{target_index}' 失败: {str(e)}"
            ) from e

    def force_merge(
        self,
        index_name: str,
        max_num_segments: int = 1,
        only_expunge_deletes: bool = False,
    ) -> bool:
        """强制合并索引段.

        将索引的多个段合并为更少的段，可以减少存储空间和提高搜索性能。
        建议在索引不再写入时执行此操作。

        Args:
            index_name: 索引名称（支持通配符）
            max_num_segments: 合并后的最大段数，默认为 1
            only_expunge_deletes: 是否只删除已标记删除的文档

        Returns:
            是否成功执行合并

        Raises:
            IndexManagerError: 当索引已关闭时抛出

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.force_merge("users", max_num_segments=1)
        """
        # 检查索引状态
        try:
            indices_info = self.list_indices(index_name, status="open")
            if not indices_info:
                # 获取所有匹配的索引
                all_indices = self.list_indices(index_name)
                if all_indices:
                    # 所有匹配的索引都已关闭
                    closed_indices = [
                        idx.name for idx in all_indices if idx.status == "close"
                    ]
                    raise IndexManagerError(
                        f"无法强制合并索引 '{index_name}'，"
                        f"以下索引已关闭: {', '.join(closed_indices)}"
                    )
        except IndexManagerError:
            # 重新抛出业务异常
            raise
        except Exception as check_error:
            # 如果获取索引状态失败，记录警告并继续执行（ES 会返回错误）
            logger.warning(
                f"检查索引 '{index_name}' 状态失败: {str(check_error)}，继续执行合并操作"
            )

        try:
            response = self.es_client.indices.forcemerge(
                index=index_name,
                max_num_segments=max_num_segments,
                only_expunge_deletes=only_expunge_deletes,
            )
            shards = response.get("_shards", {})
            failed = shards.get("failed", 0)
            if failed == 0:
                logger.info(
                    f"索引 '{index_name}' 强制合并成功 "
                    f"(max_num_segments={max_num_segments})"
                )
                return True
            logger.warning(
                f"索引 '{index_name}' 部分合并失败: {failed}/{shards.get('total', 0)} 个分片失败"
            )
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"强制合并索引 '{index_name}' 失败: {str(e)}"
            ) from e

    def reindex(
        self,
        source_index: str,
        dest_index: str,
        query: dict[str, Any] | None = None,
        script: dict[str, Any] | None = None,
        wait_for_completion: bool = True,
        slices: int | str = "auto",
    ) -> dict[str, Any]:
        """重建索引（数据迁移）.

        将源索引的数据复制到目标索引，支持查询过滤和脚本转换。

        Args:
            source_index: 源索引名称（支持通配符）
            dest_index: 目标索引名称
            query: 过滤查询条件，只复制匹配的文档（可选）
            script: 转换脚本，用于修改文档内容（可选）
            wait_for_completion: 是否等待操作完成，默认为 True
            slices: 并行切片数，"auto" 表示自动选择

        Returns:
            重建索引结果信息，包含 took、total、created、updated 等字段

        Raises:
            IndexManagerError: 操作失败时抛出

        Example:
            >>> manager = IndexManager(es_client)
            >>> # 简单重建
            >>> result = manager.reindex("users-old", "users-new")
            >>> print(f"迁移文档数: {result['total']}")
            >>> # 带过滤条件的重建
            >>> result = manager.reindex(
            ...     "users-old", "users-new", query={"term": {"status": "active"}}
            ... )
        """
        try:
            body: dict[str, Any] = {
                "source": {"index": source_index},
                "dest": {"index": dest_index},
            }

            if query:
                body["source"]["query"] = query
            if script:
                body["script"] = script

            response = self.es_client.reindex(
                body=body,
                wait_for_completion=wait_for_completion,
                slices=slices,
            )

            if wait_for_completion:
                logger.info(
                    f"索引 '{source_index}' 重建到 '{dest_index}' 完成: "
                    f"total={response.get('total', 0)}, "
                    f"created={response.get('created', 0)}, "
                    f"updated={response.get('updated', 0)}"
                )
            else:
                task_id = response.get("task")
                logger.info(
                    f"索引 '{source_index}' 重建到 '{dest_index}' 已提交，"
                    f"任务ID: {task_id}"
                )

            return response

        except NotFoundError as e:
            raise IndexNotFoundError(f"源索引 '{source_index}' 不存在") from e
        except Exception as e:
            raise IndexManagerError(
                f"重建索引 '{source_index}' 到 '{dest_index}' 失败: {str(e)}"
            ) from e

    def split_index(
        self,
        source_index: str,
        target_index: str,
        number_of_shards: int,
        settings: IndexSettings | None = None,
    ) -> bool:
        """分割索引分片.

        将源索引的分片数量扩展到更多的分片。源索引必须处于只读状态。
        目标分片数必须是源分片数的整数倍。

        注意：分割前需要将源索引设置为只读：
        manager.put_settings(source_index, {"index": {"blocks": {"write": True}}})

        Args:
            source_index: 源索引名称
            target_index: 目标索引名称
            number_of_shards: 目标分片数量，必须是源分片数的整数倍
            settings: 目标索引的额外设置（可选）

        Returns:
            是否成功分割索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> # 先设置源索引为只读
            >>> manager.put_settings("users", {"index": {"blocks": {"write": True}}})
            >>> # 分割索引到 10 个分片
            >>> manager.split_index("users", "users-split", number_of_shards=10)
        """
        try:
            body: dict[str, Any] = {
                "settings": {
                    "index.number_of_shards": number_of_shards,
                }
            }
            if settings:
                body["settings"].update(settings)

            response = self.es_client.indices.split(
                index=source_index,
                target=target_index,
                body=body,
            )
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(
                    f"索引 '{source_index}' 已分割到 '{target_index}' "
                    f"({number_of_shards} 个分片)"
                )
                return True
            return False

        except NotFoundError:
            logger.warning(f"源索引 '{source_index}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(
                f"分割索引 '{source_index}' 到 '{target_index}' 失败: {str(e)}"
            ) from e

    def clear_cache(
        self,
        index_name: str | None = None,
        fielddata: bool = False,
        query: bool = False,
        request: bool = False,
    ) -> bool:
        """清理索引缓存.

        清理指定索引的缓存，可以选择性地清理不同类型的缓存。

        Args:
            index_name: 索引名称（支持通配符），为 None 时清理所有索引缓存
            fielddata: 是否清理字段数据缓存
            query: 是否清理查询缓存
            request: 是否清理请求缓存

        Returns:
            是否成功清理缓存

        Example:
            >>> manager = IndexManager(es_client)
            >>> # 清理指定索引的所有缓存
            >>> manager.clear_cache("users")
            >>> # 清理所有索引的查询缓存
            >>> manager.clear_cache(query=True)
        """
        try:
            kwargs: dict[str, Any] = {}
            if index_name:
                kwargs["index"] = index_name
            if fielddata:
                kwargs["fielddata"] = True
            if query:
                kwargs["query"] = True
            if request:
                kwargs["request"] = True

            response = self.es_client.indices.clear_cache(**kwargs)
            shards = response.get("_shards", {})
            failed = shards.get("failed", 0)

            target = f"索引 '{index_name}'" if index_name else "所有索引"
            if failed == 0:
                logger.info(f"{target} 缓存清理成功")
                return True
            logger.warning(
                f"{target} 部分缓存清理失败: {failed}/{shards.get('total', 0)} 个分片失败"
            )
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"清理缓存失败: {str(e)}") from e

    def freeze_index(self, index_name: str) -> bool:
        """冻结索引.

        冻结的索引是只读的，不再占用堆内存，适合存档不活跃的数据。
        冻结的索引仍可以被搜索，但搜索速度会变慢。

        注意：此功能在 ES 7.14 后已被废弃，建议使用 ILM 的 frozen tier。

        Args:
            index_name: 索引名称

        Returns:
            是否成功冻结索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.freeze_index("logs-2023")
        """
        try:
            response = self.es_client.indices.freeze(index=index_name)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 已冻结")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"冻结索引 '{index_name}' 失败: {str(e)}") from e

    def unfreeze_index(self, index_name: str) -> bool:
        """解冻索引.

        将冻结的索引恢复为正常状态。

        注意：此功能在 ES 7.14 后已被废弃。

        Args:
            index_name: 索引名称

        Returns:
            是否成功解冻索引

        Example:
            >>> manager = IndexManager(es_client)
            >>> manager.unfreeze_index("logs-2023")
        """
        try:
            response = self.es_client.indices.unfreeze(index=index_name)
            acknowledged = response.get("acknowledged", False)
            if acknowledged:
                logger.info(f"索引 '{index_name}' 已解冻")
                return True
            return False

        except NotFoundError:
            logger.warning(f"索引 '{index_name}' 不存在")
            return False
        except Exception as e:
            raise IndexManagerError(f"解冻索引 '{index_name}' 失败: {str(e)}") from e
