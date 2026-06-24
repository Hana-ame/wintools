# pkg/kv — 内存键值存储

`pkg/kv` 是一个并发安全的内存键值存储库，支持**阻塞读取（长轮询）**、**浅层/深层合并**和 **TTL 自动过期**。

---

## 安装

```go
import "github.com/Hana-ame/wintools/pkg/kv"
```

---

## 快速开始

```go
store := kv.NewStore(5*time.Minute, 30*time.Second)
defer store.Stop()

// 设置
store.Set("user:1", map[string]any{"name": "Alice", "age": 30})

// 非阻塞读取
data, ok := store.Peek("user:1")

// 阻塞读取（带超时）
ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
defer cancel()
data, ok = store.Get(ctx, "user:1")

// 删除
store.Delete("user:1")
```

---

## API

### NewStore

```go
func NewStore(ttl, tick time.Duration) *Store
```

创建一个新的 Store 并启动后台过期清理 goroutine。

| 参数 | 说明 |
|------|------|
| `ttl` | 条目过期时间。`0` 表示永不过期。 |
| `tick` | 过期清理循环的执行间隔。 |

不再使用时必须调用 `Stop()` 停止后台 goroutine。

---

### Stop

```go
func (s *Store) Stop()
```

停止后台过期清理 goroutine。调用后 Store 仍可正常读写，但不再自动清理过期条目。

---

### Set

```go
func (s *Store) Set(key string, val map[string]any)
```

设置 key 的值为 val。如果 key 已存在，旧值被**完全替换**。

每次 Set 会唤醒所有阻塞在该 key 上的 `Get` 调用。

```go
store.Set("config", map[string]any{
    "theme": "dark",
    "lang":  "zh",
})
```

---

### Get

```go
func (s *Store) Get(ctx context.Context, key string) (map[string]any, bool)
```

返回 key 的值。

- **key 已有数据**：立即返回 `(data, true)`。
- **key 无数据**：阻塞等待，直到：
  - 数据到达（Set/Merge 写入）→ 返回 `(data, true)`
  - key 被删除 → 返回 `(nil, false)`
  - context 超时/取消 → 返回 `(nil, false)`

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

data, ok := store.Get(ctx, "job:result")
if !ok {
    log.Println("获取超时或 key 不存在")
}
```

> **注意：** 始终传入带超时的 context，避免 goroutine 永久泄漏。

---

### Peek

```go
func (s *Store) Peek(key string) (map[string]any, bool)
```

非阻塞读取。如果 key 存在且有数据，返回 `(data, true)`；否则返回 `(nil, false)`。

```go
data, ok := store.Peek("cache:weather")
if ok {
    fmt.Println("缓存命中:", data)
}
```

---

### ShallowMerge

```go
func (s *Store) ShallowMerge(key string, val map[string]any)
```

浅层合并：仅覆盖 val 中存在的**顶层字段**，不递归嵌套。

```go
store.Set("cfg", map[string]any{"a": 1, "b": 2})
store.ShallowMerge("cfg", map[string]any{"b": 3, "c": 4})
// 结果: {"a": 1, "b": 3, "c": 4}
```

---

### DeepMerge

```go
func (s *Store) DeepMerge(key string, val map[string]any)
```

深层递归合并。嵌套的 `map[string]any` 会递归合并，非 map 字段直接覆盖。

```go
store.Set("cfg", map[string]any{
    "nest": map[string]any{"x": 1, "y": 2},
})
store.DeepMerge("cfg", map[string]any{
    "nest": map[string]any{"y": 99, "z": 3},
})
// 结果: {"nest": {"x": 1, "y": 99, "z": 3}}
```

---

### Delete

```go
func (s *Store) Delete(key string)
```

删除 key 及其数据。会唤醒所有阻塞在该 key 上的 `Get` 调用（返回 `(nil, false)`）。

删除不存在的 key 是安全的无操作。

---

## 工作原理

### 并发安全

采用**双层 RWMutex** 设计：

- `Store.mu` — 保护 key → Entry 的映射表
- `Entry.mu` — 保护单个条目的数据和 broadcast channel

读操作（Peek、Get 的数据已存在时）只读锁，写入操作（Set、Merge、Delete）写锁，最大限度允许多个并发的非冲突读取。

### 阻塞唤醒机制（长轮询）

每个 Entry 包含一个 `broadCh chan struct{}`：

- **等待：** `Get` 在无数据时 `select` 在 `broadCh` 和 `ctx.Done()` 上
- **唤醒：** `Set`、`Merge`、`Delete` 关闭旧的 `broadCh` 并创建新的，所有等待者收到关闭信号后苏醒
- **安全：** 关闭 channel 是 Go 中唯一安全的广播方式，所有 goroutine 同时被通知

```
Set/Merge/Delete
       │
       ▼
close(broadCh)  ──►  阻塞的 Get Goroutine 1  ──► 苏醒读取数据
                     阻塞的 Get Goroutine 2  ──► 苏醒读取数据
broadCh = make(...)   ...
```

### TTL 过期

后台 goroutine 按 `tick` 间隔扫描所有条目：

1. **第一轮（读锁）：** 遍历映射表，收集 `now > lastTouch + ttl` 的 key
2. **第二轮（写锁）：** 二次确认后删除并唤醒等待者

`Peek`、`Get` 和所有写入操作都会刷新 `lastTouch`，避免频繁访问的条目被误清理。

---

## 最佳实践

### 用 Peek 实现缓存读取

```go
if data, ok := store.Peek("cache:key"); ok {
    return data
}
data := fetchFromDB()
store.Set("cache:key", data)
```

### 用 Get 实现任务结果等待

```go
// Worker goroutine
store.Set("task:123", result)

// Consumer goroutine
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
data, ok := store.Get(ctx, "task:123")
```

### 用 DeepMerge 实现配置覆盖

```go
defaults := map[string]any{
    "theme": "light",
    "editor": map[string]any{
        "fontSize": 14,
        "tabSize":  4,
    },
}
store.DeepMerge("user:cfg", userOverrides)
```

---

## 完整示例

```go
package main

import (
    "context"
    "fmt"
    "time"
    "github.com/Hana-ame/wintools/pkg/kv"
)

func main() {
    store := kv.NewStore(time.Minute, 10*time.Second)
    defer store.Stop()

    // 设置初始值
    store.Set("alert", map[string]any{"count": 0})

    // 启动监听
    go func() {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        data, ok := store.Get(ctx, "alert")
        if ok {
            fmt.Println("收到告警:", data)
        }
    }()

    time.Sleep(100 * time.Millisecond)

    // 更新值 → 唤醒监听
    store.ShallowMerge("alert", map[string]any{"count": 1})

    time.Sleep(100 * time.Millisecond)
    // 输出: 收到告警: map[count:1]
}
```
