# FreeGPT API 中转站调查 (gov.freegpt.win)

> 研究笔记，与项目自身的 KV API（见 `api.md`）无关。前端/接入调查使用。

前端项目：EasyChat (Next.js, `buildMode: export`, v2.16.1)
源码：https://github.com/leadscloud/FreeGPT (Electron 桌面版)

### 接入线路

| 线路 | Base URL | 说明 |
|------|----------|------|
| 国内 | `https://standalone.freegpt.win:3001` | 国内可直接连接 |
| 国际 | `https://7fa179251cde.freegpt.tech` | 需代理，DNS 可能不解析 |

### CORS

两个端点均 echo `Origin` 头，`Access-Control-Allow-Credentials: true`，允许 `GET,HEAD,PUT,PATCH,POST,DELETE`。

### API 端点

**OpenAI 兼容**（One API）：

```
POST <baseUrl>/api/oneapi/v1/chat/completions
GET  <baseUrl>/api/oneapi/v1/models
```

**Provider 代理**（前端选择模型后自动路由）：

```
POST <baseUrl>/api/<provider>
```

支持 provider：`openai`, `anthropic`, `google`, `deepseek`, `moonshot`, `siliconflow`, `xai`, `chatglm`, `baidu`, `bytedance`, `alibaba`, `tencent`, `iflytek`, `302ai`, `stability`, `azure`

**其他端点：**

```
POST <baseUrl>/api/config       # 获取模型列表/配置
POST <baseUrl>/api/file/upload   # 文件上传
```

### 鉴权

需要 `Authorization: Bearer <key>`。API key 获取地址：https://site.tinycms.xyz

### 请求格式 (OpenAI 兼容)

```bash
curl -X POST https://standalone.freegpt.win:3001/api/oneapi/v1/chat/completions \
  -H "Authorization: Bearer <key>" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-4o-mini",
    "messages": [{"role": "user", "content": "hi"}],
    "stream": false
  }'
```

### 备注

- 国内线路 `standalone.freegpt.win:3001` 已验证可达，CORS 宽松，需 API key
- 国际线路 `7fa179251cde.freegpt.tech` 当前 DNS 不解析，可能已迁移或需特定网络
- 前端通过 `/api/<provider>` 自动路由到对应厂商，API key 由服务端注入
