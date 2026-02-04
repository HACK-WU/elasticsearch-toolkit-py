"""批量操作工具单元测试."""

import unittest
from unittest.mock import MagicMock, patch
from elasticsearch import Elasticsearch
from elasticflow.bulk import (
    BulkAction,
    BulkOperation,
    BulkOperationTool,
    BulkResult,
)
from elasticflow.bulk.exceptions import BulkProcessingError


class TestBulkOperationTool(unittest.TestCase):
    """BulkOperationTool 类单元测试."""

    def setUp(self):
        """设置测试环境."""
        self.es_client = MagicMock(spec=Elasticsearch)
        self.bulk_tool = BulkOperationTool(
            self.es_client, batch_size=100, max_retries=2, retry_delay=0.5
        )

    def test_initialization(self):
        """测试初始化."""
        tool = BulkOperationTool(self.es_client)
        self.assertEqual(tool.batch_size, 500)
        self.assertEqual(tool.max_retries, 3)
        self.assertEqual(tool.retry_delay, 1.0)

    def test_custom_initialization(self):
        """测试自定义初始化."""
        tool = BulkOperationTool(
            self.es_client, batch_size=200, max_retries=5, retry_delay=2.0
        )
        self.assertEqual(tool.batch_size, 200)
        self.assertEqual(tool.max_retries, 5)
        self.assertEqual(tool.retry_delay, 2.0)

    def test_prepare_bulk_action_index(self):
        """测试准备 INDEX 操作."""
        operation = BulkOperation(
            action=BulkAction.INDEX,
            index_name="test-index",
            doc_id="1",
            source={"name": "test"},
            routing="123",
        )

        action = self.bulk_tool._prepare_bulk_action(operation)

        self.assertEqual(action["_op_type"], "index")
        self.assertEqual(action["_index"], "test-index")
        self.assertEqual(action["_id"], "1")
        self.assertEqual(action["_source"], {"name": "test"})
        self.assertEqual(action["_routing"], "123")

    def test_prepare_bulk_action_update(self):
        """测试准备 UPDATE 操作."""
        operation = BulkOperation(
            action=BulkAction.UPDATE,
            index_name="test-index",
            doc_id="1",
            source={"name": "updated"},
            retry_on_conflict=5,
        )

        action = self.bulk_tool._prepare_bulk_action(operation)

        self.assertEqual(action["_op_type"], "update")
        self.assertEqual(action["_index"], "test-index")
        self.assertEqual(action["_id"], "1")
        # UPDATE 操作使用 doc 字段而非 _source
        self.assertEqual(action["doc"], {"name": "updated"})
        self.assertEqual(action["retry_on_conflict"], 5)

    def test_prepare_bulk_action_delete(self):
        """测试准备 DELETE 操作."""
        operation = BulkOperation(
            action=BulkAction.DELETE,
            index_name="test-index",
            doc_id="1",
        )

        action = self.bulk_tool._prepare_bulk_action(operation)

        self.assertEqual(action["_op_type"], "delete")
        self.assertEqual(action["_index"], "test-index")
        self.assertEqual(action["_id"], "1")

    def test_bulk_index(self):
        """测试批量索引."""
        documents = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]

        with patch.object(self.bulk_tool, "_execute_bulk_operations") as mock_execute:
            mock_execute.return_value = BulkResult(total=2, success=2, failed=0)

            result = self.bulk_tool.bulk_index("users", documents, doc_id_field="id")

            self.assertEqual(result.success, 2)
            self.assertEqual(result.failed, 0)
            mock_execute.assert_called_once()

    def test_bulk_update(self):
        """测试批量更新."""
        updates = [
            {"id": "1", "name": "Alice Smith"},
            {"id": "2", "name": "Bob Johnson"},
        ]

        with patch.object(self.bulk_tool, "_execute_bulk_operations") as mock_execute:
            mock_execute.return_value = BulkResult(total=2, success=2, failed=0)

            result = self.bulk_tool.bulk_update("users", updates)

            self.assertEqual(result.success, 2)
            self.assertEqual(result.failed, 0)
            mock_execute.assert_called_once()

    def test_bulk_update_missing_id(self):
        """测试批量更新缺少ID字段."""
        updates = [{"name": "Alice"}]

        with self.assertRaises(BulkProcessingError):
            self.bulk_tool.bulk_update("users", updates)

    def test_bulk_delete(self):
        """测试批量删除."""
        doc_ids = ["1", "2", "3"]

        with patch.object(self.bulk_tool, "_execute_bulk_operations") as mock_execute:
            mock_execute.return_value = BulkResult(total=3, success=3, failed=0)

            result = self.bulk_tool.bulk_delete("users", doc_ids)

            self.assertEqual(result.success, 3)
            self.assertEqual(result.deleted, 3)
            self.assertEqual(result.failed, 0)

    def test_bulk_upsert(self):
        """测试批量UPSERT."""
        documents = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]

        with patch.object(self.bulk_tool, "_execute_bulk_with_retry") as mock_execute:
            # 返回值增加第四个元素：成功详情列表
            mock_execute.return_value = (
                2,
                0,
                [],
                [
                    {"update": {"_index": "users", "_id": "1", "result": "created"}},
                    {"update": {"_index": "users", "_id": "2", "result": "updated"}},
                ],
            )

            result = self.bulk_tool.bulk_upsert("users", documents)

            self.assertEqual(result.success, 2)
            self.assertEqual(result.failed, 0)
            self.assertEqual(result.created, 1)
            self.assertEqual(result.updated, 1)
            mock_execute.assert_called_once()

    def test_bulk_upsert_missing_id(self):
        """测试批量UPSERT缺少ID字段."""
        documents = [{"name": "Alice"}, {"id": "2", "name": "Bob"}]

        with patch.object(self.bulk_tool, "_execute_bulk_with_retry") as mock_execute:
            # 返回值增加第四个元素：成功详情列表
            mock_execute.return_value = (
                1,
                0,
                [],
                [
                    {"update": {"_index": "users", "_id": "2", "result": "created"}},
                ],
            )

            result = self.bulk_tool.bulk_upsert("users", documents)

            self.assertEqual(result.success, 1)
            self.assertEqual(len(result.warnings), 1)

    def test_bulk_stream(self):
        """测试流式批量处理."""
        operations = [
            BulkOperation(
                action=BulkAction.INDEX,
                index_name="test-index",
                doc_id=str(i),
                source={"id": i},
            )
            for i in range(250)
        ]

        with patch.object(self.bulk_tool, "_execute_bulk_operations") as mock_execute:
            mock_execute.return_value = BulkResult(total=100, success=100, failed=0)

            def callback(current, total, result):
                pass

            result = self.bulk_tool.bulk_stream(
                iter(operations), progress_callback=callback
            )

            self.assertEqual(result.success, 300)  # 3 batches
            self.assertEqual(mock_execute.call_count, 3)

    def test_set_config(self):
        """测试更新配置."""
        self.bulk_tool.set_config(
            batch_size=1000,
            max_retries=5,
            retry_delay=2.0,
            raise_on_error=True,
        )

        self.assertEqual(self.bulk_tool.batch_size, 1000)
        self.assertEqual(self.bulk_tool.max_retries, 5)
        self.assertEqual(self.bulk_tool.retry_delay, 2.0)
        self.assertTrue(self.bulk_tool.raise_on_error)


class TestBulkResult(unittest.TestCase):
    """BulkResult 类单元测试."""

    def test_is_success(self):
        """测试判断是否成功."""
        result = BulkResult(total=10, success=10, failed=0)
        self.assertTrue(result.is_success())

        result.failed = 1
        self.assertFalse(result.is_success())

    def test_add_error(self):
        """测试添加错误."""
        result = BulkResult()
        result.add_error(
            index_name="test-index",
            doc_id="1",
            error_type="VersionConflict",
            error_reason="Document already exists",
            status=409,
        )

        self.assertEqual(len(result.errors), 1)
        self.assertEqual(result.errors[0].error_type, "VersionConflict")

    def test_add_warning(self):
        """测试添加警告."""
        result = BulkResult()
        result.add_warning("Test warning")

        self.assertEqual(len(result.warnings), 1)
        self.assertEqual(result.warnings[0], "Test warning")

    def test_get_error_summary(self):
        """测试获取错误摘要."""
        result = BulkResult()
        result.add_error(
            index_name="test-index",
            doc_id="1",
            error_type="Error1",
            error_reason="Reason1",
            status=400,
        )

        summary = result.get_error_summary()
        self.assertIn("Total errors: 1", summary)
        self.assertIn("Reason1", summary)  # 错误原因会包含在摘要中

    def test_get_error_summary_no_errors(self):
        """测试获取错误摘要（无错误）."""
        result = BulkResult()
        summary = result.get_error_summary()
        self.assertEqual(summary, "No errors")


if __name__ == "__main__":
    unittest.main()
