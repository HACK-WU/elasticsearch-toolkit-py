"""批量操作工具使用示例.

本文件展示了如何使用 BulkOperationTool 进行高效的 Elasticsearch 批量操作。
"""

from elasticsearch import Elasticsearch
from elasticflow.bulk import (
    BulkAction,
    BulkOperation,
    BulkOperationTool,
)
from elasticflow.index_manager import IndexManager

# 创建 Elasticsearch 客户端连接
es_client = Elasticsearch(["http://localhost:9200"])

# 创建批量操作工具实例
bulk_tool = BulkOperationTool(
    es_client=es_client,
    batch_size=1000,  # 每批处理1000条记录
    max_retries=3,  # 失败时最多重试3次
    retry_delay=1.0,  # 重试间隔1秒
    raise_on_error=False,  # 不抛出异常，通过结果对象检查错误
)


# ==================== 示例1：批量索引 ====================
def example_bulk_index():
    """批量索引文档到 Elasticsearch."""
    # 准备数据
    documents = [
        {"id": "1", "name": "张三", "age": 25, "city": "北京"},
        {"id": "2", "name": "李四", "age": 30, "city": "上海"},
        {"id": "3", "name": "王五", "age": 28, "city": "广州"},
        {"id": "4", "name": "赵六", "age": 35, "city": "深圳"},
    ]

    # 执行批量索引
    result = bulk_tool.bulk_index(
        index_name="users",
        documents=documents,
        doc_id_field="id",  # 使用 id 字段作为文档ID
    )

    # 检查结果
    print("批量索引结果:")
    print(f"  总数: {result.total}")
    print(f"  成功: {result.success}")
    print(f"  失败: {result.failed}")
    print(f"  耗时: {result.took:.2f}秒")
    print(f"  批次: {result.batch_count}")

    if result.failed > 0:
        print(f"  错误摘要:\n{result.get_error_summary()}")

    return result


# ==================== 示例2：批量更新 ====================
def example_bulk_update():
    """批量更新现有文档."""
    # 准备更新数据
    updates = [
        {"id": "1", "age": 26, "city": "杭州"},  # 更新张三的年龄和城市
        {"id": "2", "age": 31},  # 只更新李四的年龄
        {"id": "3", "name": "王小五"},  # 只更新王五的名字
    ]

    # 执行批量更新
    result = bulk_tool.bulk_update(
        index_name="users",
        updates=updates,
        doc_id_field="id",
        retry_on_conflict=3,  # 版本冲突时重试3次
    )

    print(f"批量更新结果: 成功={result.success}, 失败={result.failed}")
    return result


# ==================== 示例3：批量删除 ====================
def example_bulk_delete():
    """批量删除文档."""
    # 要删除的文档ID列表
    doc_ids = ["4"]  # 删除赵六

    # 执行批量删除
    result = bulk_tool.bulk_delete(
        index_name="users",
        doc_ids=doc_ids,
    )

    print(f"批量删除结果: 删除={result.deleted}, 失败={result.failed}")
    return result


# ==================== 示例4：批量 UPSERT ====================
def example_bulk_upsert():
    """批量执行 UPSERT 操作（存在则更新，不存在则创建）."""
    # 准备数据，包含已存在和新文档
    documents = [
        {"id": "1", "name": "张三", "age": 27, "city": "成都"},  # 更新已存在的文档
        {"id": "2", "name": "李四", "age": 32, "city": "武汉"},  # 更新已存在的文档
        {"id": "5", "name": "孙七", "age": 29, "city": "西安"},  # 创建新文档
        {"id": "6", "name": "周八", "age": 31, "city": "南京"},  # 创建新文档
    ]

    # 执行批量 UPSERT
    result = bulk_tool.bulk_upsert(
        index_name="users",
        documents=documents,
        doc_id_field="id",
    )

    print("批量 UPSERT 结果:")
    print(f"  成功: {result.success}")
    print(f"  创建: {result.created}")
    print(f"  更新: {result.updated}")
    print(f"  失败: {result.failed}")

    if result.warnings:
        print(f"  警告: {result.warnings}")

    return result


# ==================== 示例5：流式批量处理 ====================
def example_bulk_stream():
    """使用流式处理处理大量数据."""

    # 生成大量模拟数据（100000条）
    def generate_documents():
        for i in range(100000):
            yield BulkOperation(
                action=BulkAction.INDEX,
                index_name="logs",
                doc_id=f"log_{i}",
                source={
                    "timestamp": f"2024-01-{i % 31:02d}T{i % 24:02d}:00:00",
                    "level": ["INFO", "WARNING", "ERROR"][i % 3],
                    "message": f"日志消息 {i}",
                },
            )

    # 定义进度回调函数
    def progress_callback(current, total, batch_result):
        print(
            f"已处理: {current}, "
            f"当前批次成功: {batch_result.success}, "
            f"当前批次失败: {batch_result.failed}"
        )

    # 执行流式批量处理
    result = bulk_tool.bulk_stream(
        operations=generate_documents(),
        progress_callback=progress_callback,
    )

    print(
        f"流式处理完成: 总数={result.total}, 成功={result.success}, 失败={result.failed}"
    )
    return result


# ==================== 示例6：自定义批量操作 ====================
def example_custom_bulk():
    """使用自定义操作类型进行批量操作."""
    # 创建混合操作
    operations = [
        # 创建新文档
        BulkOperation(
            action=BulkAction.CREATE,
            index_name="users",
            doc_id="7",
            source={"name": "吴九", "age": 33, "city": "重庆"},
        ),
        # 更新现有文档
        BulkOperation(
            action=BulkAction.UPDATE,
            index_name="users",
            doc_id="1",
            source={"city": "成都"},
        ),
        # 删除文档
        BulkOperation(
            action=BulkAction.DELETE,
            index_name="users",
            doc_id="2",
        ),
        # 索引文档（存在则覆盖）
        BulkOperation(
            action=BulkAction.INDEX,
            index_name="users",
            doc_id="8",
            source={"name": "郑十", "age": 40, "city": "天津"},
        ),
    ]

    # 执行批量操作
    result = bulk_tool.bulk_execute(operations)

    print(f"自定义批量操作结果: 成功={result.success}, 失败={result.failed}")
    return result


# ==================== 示例7：索引管理器 ====================
def example_index_manager():
    """展示索引管理器的使用."""
    # 创建索引管理器实例
    manager = IndexManager(es_client)

    # 创建索引
    mappings = {
        "properties": {
            "name": {"type": "keyword"},
            "age": {"type": "integer"},
            "city": {"type": "keyword"},
        }
    }

    manager.create_index(
        index_name="users_new",
        mappings=mappings,
        settings={"index": {"number_of_shards": 3, "number_of_replicas": 1}},
    )

    # 获取索引信息
    info = manager.get_index("users_new")
    if info:
        print(f"索引信息: 名称={info.name}, 文档数={info.docs_count}")

    # 创建别名
    manager.create_alias(
        index_name="users_new",
        alias_name="users_alias",
        is_write_index=True,
    )

    # 列出索引
    indices = manager.list_indices("users*")
    for idx in indices:
        print(f"索引: {idx.name}, 文档数: {idx.docs_count}")

    # 创建滚动索引
    manager.create_rollover_index(
        alias="logs_alias",
        initial_index="logs-000001",
        conditions={"max_age": "30d", "max_size": "50GB"},
    )

    # 创建 ILM 策略
    phases = {
        "hot": {
            "min_age": "0ms",
            "actions": {"rollover": {"max_size": "50GB", "max_age": "30d"}},
        },
        "warm": {"min_age": "30d", "actions": {"forcemerge": {"max_num_segments": 1}}},
        "delete": {"min_age": "90d", "actions": {"delete": {}}},
    }

    manager.create_ilm_policy("logs_policy", phases)

    # 应用 ILM 策略到索引
    manager.attach_ilm_policy("logs-000001", "logs_policy")


# ==================== 主函数 ====================
def main():
    """运行所有示例."""
    print("=" * 50)
    print("批量操作工具示例")
    print("=" * 50)

    # 示例1: 批量索引
    print("\n1. 批量索引示例")
    print("-" * 50)
    example_bulk_index()

    # 示例2: 批量更新
    print("\n2. 批量更新示例")
    print("-" * 50)
    example_bulk_update()

    # 示例3: 批量删除
    print("\n3. 批量删除示例")
    print("-" * 50)
    example_bulk_delete()

    # 示例4: 批量 UPSERT
    print("\n4. 批量 UPSERT 示例")
    print("-" * 50)
    example_bulk_upsert()

    # 示例5: 流式批量处理
    print("\n5. 流式批量处理示例")
    print("-" * 50)
    # 取消注释以下代码以运行流式处理示例
    # example_bulk_stream()

    # 示例6: 自定义批量操作
    print("\n6. 自定义批量操作示例")
    print("-" * 50)
    example_custom_bulk()

    # 示例7: 索引管理器
    print("\n7. 索引管理器示例")
    print("-" * 50)
    # 取消注释以下代码以运行索引管理器示例
    # example_index_manager()

    print("\n" + "=" * 50)
    print("所有示例运行完成！")
    print("=" * 50)


if __name__ == "__main__":
    main()
