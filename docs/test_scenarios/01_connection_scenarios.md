# 模块 1-2：连接管理 + 索引管理 测试场景

## 模块 1：ESClientFactory — 连接管理

### 1.1 正常流程场景（5个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 1.1 | 正确配置创建客户端 | `get_client()` 返回可用客户端，能执行 `info()` | P0 |
| 1.2 | 读写分离回退 | `get_read_client()` / `get_write_client()` 回退到 MASTER | P0 |
| 1.3 | 集群健康检查 | `health_check()` 返回 green/yellow，`is_healthy()` 为 True | P0 |
| 1.4 | 多客户端缓存 | 多次 `get_client()` 返回同一实例（`is` 判断） | P1 |
| 1.5 | 上下文管理器 | `with ESClientFactory(...)` 退出后连接关闭 | P1 |

### 1.2 输出示例

```
── 场景 1.1: 正确配置创建客户端 ──────────────────────────
  操作: ESClientFactory([ClusterConfig(hosts=[ES_HOST], role=MASTER)])
        调用 get_client().info() 获取集群信息
  预期: 返回包含 cluster_name 和 version.number 的字典
  实际: cluster_name=es-cluster, version=7.17.10
  结果: ✅ PASS (11.21ms)
```

---

## 模块 2：IndexManager — 索引管理

### 2.1 正常流程场景（7个）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 2.1 | 创建索引（含 Mapping） | 索引成功创建，mapping 与定义一致 | P0 |
| 2.2 | 检查索引是否存在 | 已创建索引返回 True，不存在索引返回 False | P0 |
| 2.3 | 获取索引信息 | 返回完整 mapping + settings | P0 |
| 2.4 | 创建/查询/删除别名 | 别名 CRUD 完整链路 | P0 |
| 2.5 | 创建/获取/删除索引模板 | 模板 CRUD | P1 |
| 2.6 | 创建/获取/删除 ILM 策略 | ILM 策略 CRUD | P1 |
| 2.7 | 重复创建索引异常 | 捕获 `IndexAlreadyExistsError` | P0 |

### 2.2 输出示例

```
── 场景 2.1: 创建索引（含 Mapping） ───────────────────────
  操作: IndexManager.create_index("test_logs", mappings={...})
        mapping 包含 8 个字段: timestamp, level, service, message, 
        response_time, status_code, ip, tags
  预期: 返回 True，索引创建成功
  实际: acknowledged=True, index=test_logs
  结果: ✅ PASS (268.73ms)

── 场景 2.4: 别名 CRUD ──────────────────────────────────
  操作: create_alias(test_logs, test_logs_alias)
        get_aliases(test_logs)
        get_indices_by_alias(test_logs_alias)
        delete_alias(test_logs, test_logs_alias)
  预期: 别名创建后可查询，删除后不存在
  实际: alias=test_logs_alias, indices=[test_logs], delete=True
  结果: ✅ PASS (81.88ms)
```

### 2.3 高级场景（可选）

| # | 场景 | 验证点 | 优先级 |
|---|------|--------|--------|
| 2.8 | 索引设置更新 | 动态修改 settings | P2 |
| 2.9 | 索引统计信息 | 获取文档数、存储大小 | P2 |
| 2.10 | 索引滚动 (rollover) | ILM 滚动触发条件 | P2 |
