"""索引管理器单元测试."""

import unittest
from unittest.mock import MagicMock
from elasticflow.index_manager import (
    IndexManager,
)
from elasticflow.index_manager.exceptions import (
    IndexAlreadyExistsError,
)


class TestIndexManager(unittest.TestCase):
    """IndexManager 类单元测试."""

    def setUp(self):
        """设置测试环境."""
        self.es_client = MagicMock()
        self.es_client.indices = MagicMock()
        self.es_client.ilm = MagicMock()
        self.manager = IndexManager(self.es_client)

    def test_initialization(self):
        """测试初始化."""
        self.assertIsNotNone(self.manager.es_client)

    def test_create_index(self):
        """测试创建索引."""
        self.es_client.indices.create.return_value = {"acknowledged": True}

        result = self.manager.create_index("test-index")

        self.assertTrue(result)
        self.es_client.indices.create.assert_called_once()

    def test_create_index_already_exists(self):
        """测试创建已存在的索引."""
        from elasticsearch.exceptions import RequestError

        # 创建一个Mock对象来模拟RequestError
        error = RequestError(
            400,
            "resource_already_exists_exception",
            {"error": {"type": "resource_already_exists_exception"}},
        )
        # 使用PropertyMock来模拟status_code属性
        from unittest.mock import PropertyMock

        type(error).status_code = PropertyMock(return_value=400)

        # 修改错误信息以包含resource_already_exists_exception
        error.message = "resource_already_exists_exception"
        self.es_client.indices.create.side_effect = error

        with self.assertRaises(IndexAlreadyExistsError):
            self.manager.create_index("test-index")

    def test_delete_index(self):
        """测试删除索引."""
        self.es_client.indices.delete.return_value = {"acknowledged": True}
        self.es_client.indices.exists.return_value = True

        result = self.manager.delete_index("test-index")

        self.assertTrue(result)

    def test_delete_index_not_exists(self):
        """测试删除不存在的索引."""
        self.es_client.indices.exists.return_value = False

        result = self.manager.delete_index("test-index")

        self.assertFalse(result)

    def test_index_exists(self):
        """测试检查索引是否存在."""
        self.es_client.indices.exists.return_value = True

        result = self.manager.index_exists("test-index")

        self.assertTrue(result)

    def test_index_not_exists(self):
        """测试检查索引不存在."""
        self.es_client.indices.exists.return_value = False

        result = self.manager.index_exists("test-index")

        self.assertFalse(result)

    def test_get_index(self):
        """测试获取索引信息."""
        self.es_client.indices.exists.return_value = True
        self.es_client.indices.get.return_value = {
            "test-index": {
                "aliases": {"test-alias": {}},
                "mappings": {"properties": {"name": {"type": "keyword"}}},
                "settings": {"index": {"number_of_shards": 1}},
            }
        }
        self.es_client.indices.stats.return_value = {
            "indices": {
                "test-index": {
                    "primaries": {
                        "docs": {"count": 100, "created": 100},
                        "store": {"size_in_bytes": 1024},
                    }
                }
            }
        }

        info = self.manager.get_index("test-index")

        self.assertIsNotNone(info)
        self.assertEqual(info.name, "test-index")
        self.assertEqual(info.docs_count, 100)
        self.assertIn("test-alias", info.aliases)

    def test_list_indices(self):
        """测试列出所有索引."""
        self.es_client.indices.get.return_value = {
            "index1": {"aliases": {}},
            "index2": {"aliases": {}},
        }
        self.es_client.indices.exists.return_value = True
        self.es_client.indices.stats.return_value = {
            "indices": {
                "index1": {
                    "primaries": {"docs": {"count": 0}, "store": {"size_in_bytes": 0}}
                },
                "index2": {
                    "primaries": {"docs": {"count": 0}, "store": {"size_in_bytes": 0}}
                },
            }
        }

        indices = self.manager.list_indices()

        self.assertEqual(len(indices), 2)

    def test_put_settings(self):
        """测试更新索引设置."""
        self.es_client.indices.put_settings.return_value = {"acknowledged": True}

        result = self.manager.put_settings(
            "test-index", {"index": {"refresh_interval": "1s"}}
        )

        self.assertTrue(result)

    def test_create_rollover_index(self):
        """测试创建滚动索引."""
        self.es_client.indices.create.return_value = {"acknowledged": True}
        self.es_client.indices.put_alias.return_value = {"acknowledged": True}

        result = self.manager.create_rollover_index("logs", "logs-000001")

        self.assertTrue(result)

    def test_rollover_index(self):
        """测试滚动索引."""
        self.es_client.indices.get_alias.return_value = {
            "logs-000001": {"aliases": {"logs": {}}}
        }
        self.es_client.indices.rollover.return_value = {
            "old_index": "logs-000001",
            "new_index": "logs-000002",
            "acknowledged": True,
            "shards_acknowledged": True,
            "dry_run": False,
            "conditions": {},
            "old_index_aliases": {},
        }

        result = self.manager.rollover_index("logs")

        self.assertTrue(result.rolled_over)
        self.assertEqual(result.old_index, "logs-000001")
        self.assertEqual(result.new_index, "logs-000002")

    def test_create_alias(self):
        """测试创建别名."""
        self.es_client.indices.put_alias.return_value = {"acknowledged": True}

        result = self.manager.create_alias("test-index", "test-alias")

        self.assertTrue(result)

    def test_delete_alias(self):
        """测试删除别名."""
        self.es_client.indices.delete_alias.return_value = {"acknowledged": True}

        result = self.manager.delete_alias("test-index", "test-alias")

        self.assertTrue(result)

    def test_add_alias_to_index(self):
        """测试为索引添加别名."""
        self.es_client.indices.put_alias.return_value = {"acknowledged": True}

        result = self.manager.add_alias_to_index("test-index", "test-alias")

        self.assertTrue(result)

    def test_remove_alias_from_index(self):
        """测试从索引移除别名."""
        self.es_client.indices.delete_alias.return_value = {"acknowledged": True}

        result = self.manager.remove_alias_from_index("test-index", "test-alias")

        self.assertTrue(result)

    def test_get_aliases(self):
        """测试获取索引别名."""
        self.es_client.indices.get_alias.return_value = {
            "test-index": {
                "aliases": {
                    "alias1": {"is_write_index": True},
                    "alias2": {},
                }
            }
        }

        aliases = self.manager.get_aliases("test-index")

        self.assertEqual(len(aliases), 2)

    def test_get_indices_by_alias(self):
        """测试获取别名指向的索引."""
        self.es_client.indices.get_alias.return_value = {
            "index1": {"aliases": {"logs": {}}},
            "index2": {"aliases": {"logs": {}}},
        }

        indices = self.manager.get_indices_by_alias("logs")

        self.assertEqual(len(indices), 2)

    def test_create_index_template(self):
        """测试创建索引模板."""
        self.es_client.indices.put_index_template.return_value = {"acknowledged": True}

        result = self.manager.create_index_template(
            "logs-template",
            ["logs-*"],
            mappings={"properties": {"timestamp": {"type": "date"}}},
        )

        self.assertTrue(result)

    def test_delete_index_template(self):
        """测试删除索引模板."""
        self.es_client.indices.delete_index_template.return_value = {
            "acknowledged": True
        }

        result = self.manager.delete_index_template("logs-template")

        self.assertTrue(result)

    def test_get_index_template(self):
        """测试获取索引模板."""
        self.es_client.indices.get_index_template.return_value = {
            "index_templates": [
                {
                    "name": "logs-template",
                    "index_template": {
                        "index_patterns": ["logs-*"],
                        "priority": 100,
                        "mappings": {},
                        "settings": {},
                    },
                }
            ]
        }

        template = self.manager.get_index_template("logs-template")

        self.assertIsNotNone(template)
        self.assertEqual(template.name, "logs-template")
        self.assertEqual(template.priority, 100)

    def test_list_index_templates(self):
        """测试列出所有索引模板."""
        self.es_client.indices.get_index_template.return_value = {
            "index_templates": [
                {
                    "name": "template1",
                    "index_template": {"index_patterns": ["index1-*"], "mappings": {}},
                },
                {
                    "name": "template2",
                    "index_template": {"index_patterns": ["index2-*"], "mappings": {}},
                },
            ]
        }

        templates = self.manager.list_index_templates()

        self.assertEqual(len(templates), 2)

    def test_create_ilm_policy(self):
        """测试创建ILM策略."""
        self.es_client.ilm.put_lifecycle.return_value = {"acknowledged": True}

        phases = {
            "hot": {"min_age": "0ms", "actions": {"rollover": {"max_size": "50GB"}}},
            "delete": {"min_age": "90d", "actions": {"delete": {}}},
        }

        result = self.manager.create_ilm_policy("logs_policy", phases)

        self.assertTrue(result)

    def test_attach_ilm_policy(self):
        """测试应用ILM策略到索引."""
        self.es_client.indices.put_settings.return_value = {"acknowledged": True}

        result = self.manager.attach_ilm_policy("logs-000001", "logs_policy")

        self.assertTrue(result)

    def test_get_ilm_policy(self):
        """测试获取ILM策略."""
        self.es_client.ilm.get_lifecycle.return_value = {
            "logs_policy": {
                "policy": {
                    "phases": {
                        "hot": {"min_age": "0ms", "actions": {}},
                        "delete": {"min_age": "90d", "actions": {}},
                    },
                    "version": 1,
                }
            }
        }

        policy = self.manager.get_ilm_policy("logs_policy")

        self.assertIsNotNone(policy)
        self.assertEqual(policy.name, "logs_policy")
        self.assertEqual(policy.version, 1)
        self.assertIn("hot", policy.phases)
        self.assertIn("delete", policy.phases)

    def test_delete_ilm_policy(self):
        """测试删除ILM策略."""
        self.es_client.ilm.delete_lifecycle.return_value = {"acknowledged": True}

        result = self.manager.delete_ilm_policy("logs_policy")

        self.assertTrue(result)

    def test_get_ilm_status(self):
        """测试获取索引ILM状态."""
        self.es_client.indices.exists.return_value = True
        self.es_client.ilm.explain_lifecycle.return_value = {
            "indices": {
                "logs-000001": {
                    "step": "check-rollover-ready",
                    "phase": "hot",
                    "action": "rollover",
                    "step_info": {},
                    "policy": {"version": 1},
                }
            }
        }

        status = self.manager.get_ilm_status("logs-000001")

        self.assertIsNotNone(status)
        self.assertEqual(status.index, "logs-000001")
        self.assertEqual(status.phase, "hot")

    def test_list_ilm_policies(self):
        """测试列出所有ILM策略."""
        self.es_client.ilm.get_lifecycle.return_value = {
            "policy1": {"policy": {"phases": {}}},
            "policy2": {"policy": {"phases": {}}},
        }

        policies = self.manager.list_ilm_policies()

        self.assertEqual(len(policies), 2)


if __name__ == "__main__":
    unittest.main()
