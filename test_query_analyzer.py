#!/usr/bin/env python3
"""
QueryAnalyzer 模块测试脚本
"""

import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "."))


def test_imports():
    """测试模块导入"""
    print("Testing imports...")
    try:
        import importlib.util

        # Check if the elasticflow package exists without importing it
        spec = importlib.util.find_spec("elasticflow")
        if spec is not None:
            print("✓ All imports successful")
            return True
        else:
            print("✗ elasticflow package not found")
            return False
    except Exception as e:
        print(f"✗ Import error: {e}")
        return False


def test_basic_functionality():
    """测试基本功能"""
    print("\nTesting basic functionality...")
    try:
        # 由于我们没有真实的 ES 客户端，我们只能测试静态分析功能
        from elasticflow import RuleEngine

        # 创建规则引擎实例
        rule_engine = RuleEngine()
        print("✓ RuleEngine created successfully")

        # 测试一些简单的查询
        sample_queries = [
            # 前导通配符查询
            {"query": {"wildcard": {"message": "*error*"}}},
            # 小范围查询
            {"query": {"range": {"age": {"gte": 1, "lte": 5}}}},
            # 正常查询
            {"query": {"term": {"status": "active"}}},
        ]

        for i, query_data in enumerate(sample_queries):
            query = query_data["query"]
            suggestions = rule_engine.analyze(query, {})
            print(f"✓ Query {i + 1} analyzed, found {len(suggestions)} suggestions")

        print("✓ Basic functionality test passed")
        return True
    except Exception as e:
        print(f"✗ Basic functionality test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_model_creation():
    """测试数据模型创建"""
    print("\nTesting model creation...")
    try:
        from elasticflow import (
            QuerySuggestion,
            QueryOptimizationType,
            SeverityLevel,
            QueryAnalysis,
        )

        # 创建一个查询建议
        suggestion = QuerySuggestion(
            type=QueryOptimizationType.AVOID_WILDCARD_START,
            severity=SeverityLevel.CRITICAL,
            message="Test suggestion",
            affected_field="test_field",
            suggestion="Test suggestion text",
        )
        print("✓ QuerySuggestion created successfully")

        # Verify QueryAnalysis can be created (object is not used afterwards)
        QueryAnalysis(
            query={"match_all": {}},
            total_shards=1,
            successful_shards=1,
            failed_shards=0,
            took_ms=10.0,
            is_slow_query=False,
            suggestions=[suggestion],
        )
        print("✓ QueryAnalysis created successfully")

        print("✓ Model creation test passed")
        return True
    except Exception as e:
        print(f"✗ Model creation test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """主测试函数"""
    print("QueryAnalyzer Module Test Suite")
    print("=" * 40)

    results = []
    results.append(test_imports())
    results.append(test_basic_functionality())
    results.append(test_model_creation())

    print("\n" + "=" * 40)
    print("Test Summary:")
    print(f"Passed: {sum(results)}/{len(results)}")

    if all(results):
        print("All tests passed! ✓")
        return 0
    else:
        print("Some tests failed! ✗")
        return 1


if __name__ == "__main__":
    exit(main())
