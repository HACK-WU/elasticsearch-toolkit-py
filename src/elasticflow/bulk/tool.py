"""批量操作核心工具类."""

import time
import logging
from typing import Any
from collections.abc import Callable, Iterable
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import TransportError

from .models import BulkAction, BulkOperation, BulkResult, BulkErrorItem
from .exceptions import BulkProcessingError, BulkRetryExhaustedError

logger = logging.getLogger(__name__)


class BulkOperationTool:
    """批量操作核心工具类.

    提供高效的 Elasticsearch 批量操作功能，支持：
    - 自动分批处理
    - 失败重试机制
    - 操作结果统计
    - 流式处理支持

    Args:
        es_client: Elasticsearch 客户端实例
        batch_size: 每批次操作数量，默认为 500
        max_retries: 最大重试次数，默认为 3
        retry_delay: 重试延迟时间（秒），默认为 1.0
        raise_on_error: 遇到错误时是否抛出异常，默认为 False
        chunk_size: 与 batch_size 同义，保留用于兼容性
    """

    def __init__(
        self,
        es_client: Elasticsearch,
        batch_size: int = 500,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        raise_on_error: bool = False,
        chunk_size: int | None = None,
    ):
        self.es_client = es_client
        self.batch_size = chunk_size if chunk_size is not None else batch_size
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.raise_on_error = raise_on_error
        logger.info(
            f"初始化批量操作工具: batch_size={self.batch_size}, "
            f"max_retries={max_retries}, retry_delay={retry_delay}"
        )

    def _prepare_bulk_action(
        self,
        operation: BulkOperation,
        doc_as_upsert: bool = False,
    ) -> dict[str, Any]:
        """准备批量操作所需的动作字典.

        Args:
            operation: 批量操作项
            doc_as_upsert: 是否启用 doc_as_upsert（用于 UPSERT 操作）

        Returns:
            可用于 elasticsearch.helpers.bulk 的操作字典
        """
        # 对于 UPSERT 操作，实际使用 UPDATE 作为底层操作类型
        op_type = operation.action.value
        if operation.action == BulkAction.UPSERT:
            op_type = "update"

        action: dict[str, Any] = {
            "_op_type": op_type,
            "_index": operation.index_name,
        }

        if operation.doc_id is not None:
            action["_id"] = operation.doc_id

        if operation.routing is not None:
            action["_routing"] = operation.routing

        # 根据操作类型处理 source 数据
        if operation.action in (BulkAction.INDEX, BulkAction.CREATE):
            if operation.source is not None:
                action["_source"] = operation.source
            else:
                raise BulkProcessingError(
                    f"操作类型 {operation.action.value} 需要提供 source 数据"
                )
        elif operation.action in (BulkAction.UPDATE, BulkAction.UPSERT):
            if operation.source is not None:
                # UPDATE 操作使用 doc 字段而非 _source
                action["doc"] = operation.source
                # 如果是 UPSERT 操作，启用 doc_as_upsert
                if operation.action == BulkAction.UPSERT or doc_as_upsert:
                    action["doc_as_upsert"] = True

        if (
            operation.action in (BulkAction.UPDATE, BulkAction.UPSERT)
            and operation.retry_on_conflict is not None
        ):
            action["retry_on_conflict"] = operation.retry_on_conflict

        return action

    def _execute_bulk_with_retry(
        self,
        actions: list[dict[str, Any]],
    ) -> tuple[int, int, list[dict[str, Any]], list[dict[str, Any]]]:
        """执行批量操作并支持重试.

        Args:
            actions: 操作动作列表

        Returns:
            元组：(成功数, 失败数, 错误详情列表, 成功详情列表)
        """
        success_count = 0
        failed_count = 0
        errors: list[dict[str, Any]] = []
        successes: list[dict[str, Any]] = []

        for attempt in range(self.max_retries + 1):
            try:
                # 直接使用 stats_only=False 获取详细结果，避免重复执行
                success_count = 0
                failed_count = 0
                errors = []
                successes = []

                for ok, info in bulk(
                    self.es_client,
                    actions,
                    raise_on_exception=False,
                    raise_on_error=False,
                    stats_only=False,
                ):
                    if ok:
                        success_count += 1
                        successes.append(info)
                    else:
                        failed_count += 1
                        errors.append(info)

                # 检查是否有版本冲突错误需要重试
                if errors:
                    conflict_errors = [e for e in errors if e.get("status") == 409]

                    if conflict_errors and attempt < self.max_retries:
                        logger.warning(
                            f"检测到 {len(conflict_errors)} 个版本冲突错误，"
                            f"第 {attempt + 1} 次重试..."
                        )
                        time.sleep(self.retry_delay)
                        # 只重试冲突的操作（需要根据索引位置匹配）
                        retry_actions = []
                        for i, action in enumerate(actions):
                            # 检查该操作是否在错误列表中且是版本冲突
                            for error in errors:
                                if (
                                    error.get("_id") == action.get("_id")
                                    and error.get("_index") == action.get("_index")
                                    and error.get("status") == 409
                                ):
                                    retry_actions.append(action)
                                    break
                        if retry_actions:
                            actions = retry_actions
                            continue

                # 成功完成，跳出重试循环
                break

            except (TransportError, Exception) as e:
                if attempt < self.max_retries:
                    logger.warning(
                        f"批量操作遇到异常，第 {attempt + 1} 次重试: {str(e)}"
                    )
                    time.sleep(self.retry_delay)
                else:
                    # 重试次数耗尽
                    raise BulkRetryExhaustedError(
                        f"批量操作重试次数耗尽: {str(e)}"
                    ) from e

        return success_count, failed_count, errors, successes

    def _process_errors(
        self,
        errors: list[dict[str, Any]],
    ) -> list[BulkErrorItem]:
        """处理错误信息，转换为 BulkErrorItem 列表.

        Args:
            errors: 原始错误信息列表

        Returns:
            BulkErrorItem 列表
        """
        error_items: list[BulkErrorItem] = []

        for error in errors:
            try:
                # 提取错误信息
                index_name = error.get("index", error.get("_index", ""))
                doc_id = error.get("_id", error.get("id"))
                error_info = error.get("error", {})
                error_type = error_info.get("type", "unknown")
                error_reason = error_info.get("reason", "unknown error")
                status = error.get("status", 0)

                # 提取根本原因
                caused_by = None
                if "caused_by" in error_info:
                    caused_by_info = error_info["caused_by"]
                    caused_by = f"{caused_by_info.get('type', '')}: {caused_by_info.get('reason', '')}"

                # 提取操作类型
                operation = error.get("op_type")
                if operation:
                    try:
                        operation_enum = BulkAction(operation)
                    except ValueError:
                        operation_enum = None
                else:
                    operation_enum = None

                error_item = BulkErrorItem(
                    index_name=index_name,
                    doc_id=doc_id,
                    error_type=error_type,
                    error_reason=error_reason,
                    status=status,
                    caused_by=caused_by,
                    operation=operation_enum,
                )
                error_items.append(error_item)

            except Exception as e:
                logger.error(f"解析错误信息失败: {str(e)}, 错误内容: {error}")

        return error_items

    def _execute_bulk_operations(
        self,
        operations: list[BulkOperation],
    ) -> BulkResult:
        """执行批量操作的核心方法.

        Args:
            operations: 批量操作项列表

        Returns:
            批量操作结果
        """
        if not operations:
            return BulkResult(total=0, success=0, failed=0)

        result = BulkResult(total=len(operations))
        start_time = time.time()
        batch_count = 0

        # 分批处理
        for i in range(0, len(operations), self.batch_size):
            batch = operations[i : i + self.batch_size]
            batch_count += 1

            # 准备操作动作
            actions = [self._prepare_bulk_action(op) for op in batch]

            try:
                # 执行批量操作
                success_count, failed_count, errors, successes = (
                    self._execute_bulk_with_retry(actions)
                )

                result.success += success_count
                result.failed += failed_count

                # 处理错误
                if errors:
                    error_items = self._process_errors(errors)
                    result.errors.extend(error_items)

                if failed_count > 0:
                    logger.warning(
                        f"批次 {batch_count}: 成功 {success_count}, 失败 {failed_count}"
                    )
                else:
                    logger.info(f"批次 {batch_count}: 全部成功 ({success_count})")

            except Exception as e:
                logger.error(f"批次 {batch_count} 处理失败: {str(e)}")
                # 将整个批次标记为失败
                for op in batch:
                    result.add_error(
                        index_name=op.index_name,
                        doc_id=op.doc_id,
                        error_type="BatchProcessingError",
                        error_reason=str(e),
                        status=500,
                        operation=op.action,
                    )
                result.failed += len(batch)

        result.took = time.time() - start_time
        result.batch_count = batch_count

        # 如果配置了 raise_on_error 且有失败，抛出异常
        if self.raise_on_error and result.failed > 0:
            raise BulkProcessingError(
                f"批量操作完成，但有 {result.failed} 个失败: "
                f"{result.get_error_summary()}"
            )

        return result

    def bulk_execute(
        self,
        operations: list[BulkOperation],
    ) -> BulkResult:
        """执行批量操作.

        Args:
            operations: 批量操作项列表

        Returns:
            批量操作结果

        Example:
            >>> from elasticflow.bulk import (
            ...     BulkOperationTool,
            ...     BulkOperation,
            ...     BulkAction,
            ... )
            >>> bulk_tool = BulkOperationTool(es_client)
            >>> operations = [
            ...     BulkOperation(
            ...         action=BulkAction.INDEX,
            ...         index_name="users",
            ...         doc_id="1",
            ...         source={"name": "Alice"},
            ...     ),
            ...     BulkOperation(
            ...         action=BulkAction.INDEX,
            ...         index_name="users",
            ...         doc_id="2",
            ...         source={"name": "Bob"},
            ...     ),
            ... ]
            >>> result = bulk_tool.bulk_execute(operations)
        """
        return self._execute_bulk_operations(operations)

    def bulk_stream(
        self,
        operations: Iterable[BulkOperation],
        progress_callback: Callable[[int, int, BulkResult], None] | None = None,
    ) -> BulkResult:
        """流式执行批量操作.

        适用于处理超大量数据，不需要将所有数据加载到内存。

        Args:
            operations: 批量操作项迭代器
            progress_callback: 进度回调函数，参数为 (当前处理数, 已知总数或-1, 当前批次结果)

        Returns:
            批量操作结果

        Example:
            >>> def progress_callback(current, total, result):
            ...     print(
            ...         f"已处理: {current}, 成功: {result.success}, 失败: {result.failed}"
            ...     )
            >>>
            >>> operations = (BulkOperation(...) for _ in range(1000000))
            >>> result = bulk_tool.bulk_stream(
            ...     operations, progress_callback=progress_callback
            ... )
        """
        result = BulkResult(total=0)
        start_time = time.time()
        batch: list[BulkOperation] = []
        batch_count = 0
        processed_count = 0

        try:
            for operation in operations:
                batch.append(operation)

                # 达到批次大小时执行
                if len(batch) >= self.batch_size:
                    batch_count += 1
                    batch_result = self._execute_bulk_operations(batch)

                    # 合并结果
                    result.total += batch_result.total
                    result.success += batch_result.success
                    result.failed += batch_result.failed
                    result.created += batch_result.created
                    result.updated += batch_result.updated
                    result.deleted += batch_result.deleted
                    result.errors.extend(batch_result.errors)
                    result.warnings.extend(batch_result.warnings)
                    processed_count += batch_result.total

                    # 调用进度回调
                    if progress_callback:
                        progress_callback(processed_count, -1, batch_result)

                    batch.clear()

            # 处理剩余的操作
            if batch:
                batch_count += 1
                batch_result = self._execute_bulk_operations(batch)

                result.total += batch_result.total
                result.success += batch_result.success
                result.failed += batch_result.failed
                result.created += batch_result.created
                result.updated += batch_result.updated
                result.deleted += batch_result.deleted
                result.errors.extend(batch_result.errors)
                result.warnings.extend(batch_result.warnings)
                processed_count += batch_result.total

                if progress_callback:
                    progress_callback(processed_count, -1, batch_result)

        except Exception as e:
            logger.error(f"流式处理过程中发生异常: {str(e)}")
            # 记录已处理的结果
            if progress_callback:
                progress_callback(processed_count, -1, result)
            raise BulkProcessingError(f"流式处理失败: {str(e)}") from e

        result.took = time.time() - start_time
        result.batch_count = batch_count

        return result

    def set_config(
        self,
        batch_size: int | None = None,
        max_retries: int | None = None,
        retry_delay: float | None = None,
        raise_on_error: bool | None = None,
    ) -> None:
        """更新工具配置.

        Args:
            batch_size: 每批次操作数量
            max_retries: 最大重试次数
            retry_delay: 重试延迟时间（秒）
            raise_on_error: 遇到错误时是否抛出异常
        """
        if batch_size is not None:
            self.batch_size = batch_size
        if max_retries is not None:
            self.max_retries = max_retries
        if retry_delay is not None:
            self.retry_delay = retry_delay
        if raise_on_error is not None:
            self.raise_on_error = raise_on_error

        logger.info(
            f"更新配置: batch_size={self.batch_size}, "
            f"max_retries={self.max_retries}, "
            f"retry_delay={self.retry_delay}, "
            f"raise_on_error={self.raise_on_error}"
        )

    def bulk_index(
        self,
        index_name: str,
        documents: list[dict[str, Any]],
        doc_id_field: str | None = None,
        routing_field: str | None = None,
    ) -> BulkResult:
        """批量索引文档.

        将文档批量索引到指定索引。如果文档已存在则更新。

        Args:
            index_name: 索引名称
            documents: 文档列表
            doc_id_field: 用作文档ID的字段名，如果不指定则让ES自动生成ID
            routing_field: 用作路由键的字段名

        Returns:
            批量操作结果

        Example:
            >>> bulk_tool = BulkOperationTool(es_client)
            >>> documents = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
            >>> result = bulk_tool.bulk_index("users", documents, doc_id_field="id")
            >>> print(f"成功: {result.success}, 失败: {result.failed}")
        """
        operations: list[BulkOperation] = []

        for doc in documents:
            doc_id = None
            if doc_id_field:
                doc_id = doc.get(doc_id_field)

            routing = None
            if routing_field:
                routing = doc.get(routing_field)

            operation = BulkOperation(
                action=BulkAction.INDEX,
                index_name=index_name,
                doc_id=doc_id,
                source=doc,
                routing=routing,
            )
            operations.append(operation)

        return self._execute_bulk_operations(operations)

    def bulk_create(
        self,
        index_name: str,
        documents: list[dict[str, Any]],
        doc_id_field: str | None = None,
        routing_field: str | None = None,
    ) -> BulkResult:
        """批量创建文档.

        将文档批量创建到指定索引。如果文档已存在则返回错误。

        Args:
            index_name: 索引名称
            documents: 文档列表
            doc_id_field: 用作文档ID的字段名，如果不指定则让ES自动生成ID
            routing_field: 用作路由键的字段名

        Returns:
            批量操作结果

        Example:
            >>> bulk_tool = BulkOperationTool(es_client)
            >>> documents = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
            >>> result = bulk_tool.bulk_create("users", documents, doc_id_field="id")
            >>> print(f"成功: {result.success}, 失败: {result.failed}")
        """
        operations: list[BulkOperation] = []

        for doc in documents:
            doc_id = None
            if doc_id_field:
                doc_id = doc.get(doc_id_field)

            routing = None
            if routing_field:
                routing = doc.get(routing_field)

            operation = BulkOperation(
                action=BulkAction.CREATE,
                index_name=index_name,
                doc_id=doc_id,
                source=doc,
                routing=routing,
            )
            operations.append(operation)

        return self._execute_bulk_operations(operations)

    def bulk_update(
        self,
        index_name: str,
        updates: list[dict[str, Any]],
        doc_id_field: str = "id",
        retry_on_conflict: int | None = None,
    ) -> BulkResult:
        """批量更新文档.

        批量更新指定索引中的文档。

        Args:
            index_name: 索引名称
            updates: 更新数据列表，每个元素应包含文档ID和要更新的字段
            doc_id_field: 用作文档ID的字段名，默认为 "id"
            retry_on_conflict: 版本冲突时的重试次数，默认为 3

        Returns:
            批量操作结果

        Example:
            >>> bulk_tool = BulkOperationTool(es_client)
            >>> updates = [
            ...     {"id": "1", "name": "Alice Smith"},
            ...     {"id": "2", "name": "Bob Johnson"},
            ... ]
            >>> result = bulk_tool.bulk_update("users", updates, doc_id_field="id")
            >>> print(f"成功: {result.success}, 失败: {result.failed}")
        """
        if retry_on_conflict is None:
            retry_on_conflict = 3

        operations: list[BulkOperation] = []

        for update_data in updates:
            doc_id = update_data.get(doc_id_field)
            if not doc_id:
                raise BulkProcessingError(
                    f"更新数据中缺少文档ID字段 '{doc_id_field}': {update_data}"
                )

            # 排除ID字段，只保留需要更新的字段
            source = {k: v for k, v in update_data.items() if k != doc_id_field}

            operation = BulkOperation(
                action=BulkAction.UPDATE,
                index_name=index_name,
                doc_id=doc_id,
                source=source,
                retry_on_conflict=retry_on_conflict,
            )
            operations.append(operation)

        return self._execute_bulk_operations(operations)

    def bulk_delete(
        self,
        index_name: str,
        doc_ids: list[str],
    ) -> BulkResult:
        """批量删除文档.

        批量删除指定索引中的文档。

        Args:
            index_name: 索引名称
            doc_ids: 文档ID列表

        Returns:
            批量操作结果

        Example:
            >>> bulk_tool = BulkOperationTool(es_client)
            >>> doc_ids = ["1", "2", "3"]
            >>> result = bulk_tool.bulk_delete("users", doc_ids)
            >>> print(f"成功: {result.success}, 失败: {result.failed}")
        """
        operations: list[BulkOperation] = []

        for doc_id in doc_ids:
            operation = BulkOperation(
                action=BulkAction.DELETE,
                index_name=index_name,
                doc_id=doc_id,
            )
            operations.append(operation)

        result = self._execute_bulk_operations(operations)
        # 对于删除操作，成功的数量就是删除的数量
        result.deleted = result.success
        return result

    def bulk_upsert(
        self,
        index_name: str,
        documents: list[dict[str, Any]],
        doc_id_field: str = "id",
    ) -> BulkResult:
        """批量执行 UPSERT 操作（存在则更新，不存在则创建）.

        根据文档ID判断，如果文档不存在则创建新文档，如果存在则更新文档。

        Args:
            index_name: 索引名称
            documents: 文档列表
            doc_id_field: 用作文档ID的字段名，默认为 "id"

        Returns:
            批量操作结果，包含 created 和 updated 统计数

        Example:
            >>> bulk_tool = BulkOperationTool(es_client)
            >>> documents = [
            ...     {"id": "1", "name": "Alice"},
            ...     {"id": "2", "name": "Bob"},
            ...     {"id": "3", "name": "Charlie"},
            ... ]
            >>> result = bulk_tool.bulk_upsert("users", documents)
            >>> print(
            ...     f"创建: {result.created}, 更新: {result.updated}, 失败: {result.failed}"
            ... )
        """
        operations: list[BulkOperation] = []
        result = BulkResult(total=len(documents))
        start_time = time.time()
        batch_count = 0

        for doc in documents:
            doc_id = doc.get(doc_id_field)

            if not doc_id:
                warning_msg = f"文档中缺少ID字段 '{doc_id_field}'，跳过: {doc}"
                logger.warning(warning_msg)
                result.add_warning(warning_msg)
                continue

            # 使用 UPSERT 操作类型，底层会转换为 UPDATE + doc_as_upsert
            # 当 doc_id 不存在时，会创建新文档；存在时则更新
            operation = BulkOperation(
                action=BulkAction.UPSERT,
                index_name=index_name,
                doc_id=doc_id,
                source=doc,
                retry_on_conflict=3,
            )
            operations.append(operation)

        if not operations:
            logger.warning("没有有效的操作需要执行")
            result.took = time.time() - start_time
            return result

        # 分批执行
        for i in range(0, len(operations), self.batch_size):
            batch = operations[i : i + self.batch_size]
            batch_count += 1

            # 准备操作动作
            actions = [self._prepare_bulk_action(op) for op in batch]

            try:
                # 执行批量操作
                success_count, failed_count, errors, successes = (
                    self._execute_bulk_with_retry(actions)
                )

                result.success += success_count
                result.failed += failed_count

                # 处理错误
                if errors:
                    error_items = self._process_errors(errors)
                    result.errors.extend(error_items)

                # 从成功结果中统计创建和更新数
                for success_info in successes:
                    # 从响应中提取 result 字段
                    # success_info 格式类似: {"update": {"_index": "xxx", "_id": "xxx", "result": "created"}}
                    for op_type, op_result in success_info.items():
                        result_status = op_result.get("result", "")
                        if result_status == "created":
                            result.created += 1
                        elif result_status in ("updated", "noop"):
                            # noop 表示没有变化的更新，也算作更新
                            result.updated += 1

                if failed_count > 0:
                    logger.warning(
                        f"UPSERT 批次 {batch_count}: 成功 {success_count}, "
                        f"失败 {failed_count}"
                    )
                else:
                    logger.info(
                        f"UPSERT 批次 {batch_count}: 全部成功 ({success_count})"
                    )

            except Exception as e:
                logger.error(f"UPSERT 批次 {batch_count} 处理失败: {str(e)}")
                # 将整个批次标记为失败
                for op in batch:
                    result.add_error(
                        index_name=op.index_name,
                        doc_id=op.doc_id,
                        error_type="BatchProcessingError",
                        error_reason=str(e),
                        status=500,
                        operation=op.action,
                    )
                result.failed += len(batch)

        result.took = time.time() - start_time
        result.batch_count = batch_count

        return result
