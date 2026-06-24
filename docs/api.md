# API 文档

本项目提供基于 **Gin** 的 KV 存储 HTTP API，所有接口均位于 `/kv` 路由组下（在 `cmd/api-server/main.go` 中通过 `router.Group("/kv")` 挂载）。以下是每个端点的详细说明。

## 基本路径
```
GET    /kv/:key            获取键值（可选等待）
POST   /kv/:key            设置键值（完整覆盖）
PUT    /kv/:key            浅合并（仅第一层字段）
PATCH  /kv/:key            深度合并（递归合并）
DELETE /kv/:key            删除键
```

---

### `GET /kv/:key`
- **描述**：获取指定 `key` 的当前值。
- **查询参数**：`wait`（可选）
  - `wait=N` 时，服务器将在最长 `N` 秒内阻塞等待该键被写入或更新；超时返回 `408 Request Timeout`。
- **返回示例**（成功）
  ```json
  {
    "field1": "value1",
    "field2": 123
  }
  ```
- **错误码**
  - `404 Not Found`：键不存在且未提供 `wait` 参数。
  - `400 Bad Request`：`wait` 不是正整数。
  - `408 Request Timeout`：在 `wait` 秒内未收到键值。

---

### `POST /kv/:key`
- **描述**：为指定 `key` 设置一个完整的 JSON 对象，会覆盖已有数据。
- **请求体**：必须是合法的 JSON 对象，例如
  ```json
  {
    "name": "alice",
    "age": 30
  }
  ```
- **返回**：`{"ok": true}`
- **错误码**
  - `400 Bad Request`：请求体不是 JSON 对象。

---

### `PUT /kv/:key`
- **描述**：对已有键执行 **浅合并**（仅合并第一层字段），不影响嵌套对象的完整性。
- **请求体**：同 `POST`，必须是 JSON 对象。
- **返回**：`{"ok": true}`
- **错误码**：同 `POST`。

---

### `PATCH /kv/:key`
- **描述**：对已有键执行 **深度合并**（递归合并），会把请求体中的嵌套字段逐层合并进当前值。
- **请求体**：同 `POST`，必须是 JSON 对象。
- **返回**：`{"ok": true}`
- **错误码**：同 `POST`。

---

### `DELETE /kv/:key`
- **描述**：删除指定键及其所有关联数据。
- **返回**：`{"ok": true}`
- **错误码**：无（即使键不存在也返回成功）。

---

## 示例 Curl 调用
```bash
# 设置键值
curl -X POST -H "Content-Type: application/json" -d '{"foo": "bar"}' http://localhost:8080/kv/example

# 获取键值（立即返回）
curl http://localhost:8080/kv/example

# 阻塞等待键值出现（最多 10 秒）
curl http://localhost:8080/kv/example?wait=10

# 浅合并
curl -X PUT -H "Content-Type: application/json" -d '{"newField": 123}' http://localhost:8080/kv/example

# 深度合并
curl -X PATCH -H "Content-Type: application/json" -d '{"nested": {"inner": "value"}}' http://localhost:8080/kv/example

# 删除键
curl -X DELETE http://localhost:8080/kv/example
```

## 代码实现位置
- 路由注册：`pkg/api/handler.go` 中的 `Handler.RegisterRoutes`。
- 业务逻辑：`pkg/kv/store.go` 提供线程安全的 `Get`、`Peek`、`Set`、`ShallowMerge`、`DeepMerge`、`Delete` 等方法。
- 服务器入口：`cmd/api-server/main.go`。

---

> **注意**：所有接口均返回 JSON，错误信息统一为 `{ "error": "message" }` 的结构，便于前端统一错误处理。
