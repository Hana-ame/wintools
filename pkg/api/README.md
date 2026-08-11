# pkg/api — HTTP API 层

`pkg/api` 基于 [Gin](https://github.com/gin-gonic/gin) 框架，封装 `pkg/kv.Store` 提供 RESTful 键值操作接口。

---

## 安装

```go
import "github.com/Hana-ame/wintools/pkg/api"
```

---

## 快速开始

```go
package main

import (
    "time"
    "github.com/gin-gonic/gin"
    "github.com/Hana-ame/wintools/pkg/api"
    "github.com/Hana-ame/wintools/pkg/kv"
)

func main() {
    store := kv.NewStore(5*time.Minute, 30*time.Second)
    defer store.Stop()

    handler := api.NewHandler(store)

    r := gin.Default()
    grp := r.Group("/kv")
    handler.RegisterRoutes(grp)

    r.Run(":8080")
}
```

启动后：

```bash
# 设置 key
curl -X POST http://localhost:8080/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"name":"alice","age":30}'

# 非阻塞读取
curl http://localhost:8080/kv/mykey

# 长轮询（最多等 10 秒）
curl "http://localhost:8080/kv/mykey?wait=10"

# 浅层合并
curl -X PUT http://localhost:8080/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"age":31}'

# 深层合并
curl -X PATCH http://localhost:8080/kv/mykey \
  -H "Content-Type: application/json" \
  -d '{"nested":{"x":1}}'

# 删除
curl -X DELETE http://localhost:8080/kv/mykey
```

---

## API 参考

### GET /:key

读取 key 的值。

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `wait` | int | 否 | 等不到数据时最长等待秒数 |

**无 wait 参数**（非阻塞）：

```json
// 200 — key 存在
{"name":"alice","age":30}

// 404 — key 不存在
{"error":"not found"}
```

**有 wait 参数**（长轮询）：

```json
// 200 — 数据在超时前到达
{"name":"alice","age":30}

// 408 — 超时
{"error":"timeout"}

// 404 — key 在等待期间被删除
{"error":"not found"}
```

---

### POST /:key

设置/替换 key 的值。

- **请求体**：必须是 JSON 对象 `{}`
- **200**: `{"ok":true}`
- **400**: `{"error":"body must be a JSON object"}`

---

### PUT /:key

浅层合并：仅覆盖顶层字段，不递归合并嵌套对象。

- **请求体**：必须是 JSON 对象
- **200**: `{"ok":true}`
- **400**: `{"error":"body must be a JSON object"}`

---

### PATCH /:key

深层递归合并：嵌套的 `map[string]any` 会递归合并，非 map 字段直接覆盖。

- **请求体**：必须是 JSON 对象
- **200**: `{"ok":true}`
- **400**: `{"error":"body must be a JSON object"}`

---

### DELETE /:key

删除 key。

- **200**: `{"ok":true}`

---

## 使用技术

### [Gin Web Framework](https://github.com/gin-gonic/gin)

高性能 HTTP 框架，提供路由注册、请求参数解析（`c.Param`、`c.Query`）、JSON 绑定（`c.ShouldBindJSON`）、JSON 响应（`c.JSON`）。

### [pkg/kv](../kv/README.md)

底层内存键值存储引擎，提供：
- 并发安全的 `map[string]any` 存储
- 阻塞读取（channel broadcast 唤醒机制）
- 浅层/深层合并
- TTL 自动过期

### context.Context

长轮询通过 `context.WithTimeout` 实现超时控制，与 `kv.Store.Get` 的 context-aware 阻塞机制配合，避免 goroutine 泄漏。

### net/http/httptest

所有测试基于 `httptest.NewRecorder` + `httptest.NewRequest`，无需真实 HTTP 服务器即可完整测试路由逻辑。

---

## 错误码汇总

| HTTP 状态码 | 含义 |
|-------------|------|
| 200 | 成功 |
| 400 | 请求参数错误（`?wait` 非法、body 不是 JSON 对象） |
| 404 | key 不存在 |
| 408 | 长轮询超时 |

---

## 完整示例

```go
package main

import (
    "context"
    "fmt"
    "net/http"
    "time"
    "github.com/gin-gonic/gin"
    "github.com/Hana-ame/wintools/pkg/api"
    "github.com/Hana-ame/wintools/pkg/kv"
)

func main() {
    store := kv.NewStore(0, time.Hour)
    defer store.Stop()

    handler := api.NewHandler(store)
    r := gin.Default()
    handler.RegisterRoutes(r.Group("/kv"))

    // 异步写入触发长轮询
    go func() {
        time.Sleep(200 * time.Millisecond)
        http.Post("http://localhost:8080/kv/alert",
            "application/json",
            strings.NewReader(`{"level":"info","msg":"hello"}`))
    }()

    r.Run(":8080")
}
```

```bash
# 另开终端执行
curl "http://localhost:8080/kv/alert?wait=5"
# 约 200ms 后收到: {"level":"info","msg":"hello"}
```
