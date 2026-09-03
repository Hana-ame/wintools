/* peerfs bridge.js — 浏览器端纯 JS 客户端 SDK
 * 
 * 核心特性：
 * 1. Para Conns（并发连接池）：同时建立多个 WebRTC DataChannel，彻底告别单流阻塞；
 * 2. 边下边播（Streaming & Range）：支持 ReadableStream 流式直出与 Service Worker 虚拟 HTTP 206 代理；
 * 3. 极简直接获取：无需 list 遍历，直接通过 fs.get() / fs.stream() / fs.url() / fs.download() 取文件；
 * 4. 目录能力保留：仍然完整暴露 fs.list() 与 fs.stat()。
 */
(function (global) {
  'use strict';

  var DEFAULTS = {
    host: '0.peerjs.com',
    port: 443,
    secure: true,
    key: 'peerjs',
    path: '/',
    peerId: '',
    token: '',
    conns: 64, // 默认 64 条并行 WebRTC 数据通道！
  };

  function genReqId() {
    return 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  }

  function getMime(path) {
    var ext = (path.split('.').pop() || '').toLowerCase();
    var mimes = {
      'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska', 'mov': 'video/quicktime',
      'mp3': 'audio/mpeg', 'ogg': 'audio/ogg', 'wav': 'audio/wav', 'flac': 'audio/flac', 'm4a': 'audio/mp4',
      'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif', 'webp': 'image/webp',
      'avif': 'image/avif', 'svg': 'image/svg+xml', 'pdf': 'application/pdf', 'txt': 'text/plain; charset=utf-8',
      'md': 'text/markdown; charset=utf-8', 'json': 'application/json', 'html': 'text/html; charset=utf-8',
      'js': 'text/javascript; charset=utf-8', 'css': 'text/css; charset=utf-8', 'xml': 'application/xml'
    };
    return mimes[ext] || '';
  }

  function PeerFS(opts) {
    Object.assign(this, DEFAULTS, opts || {});
    this.workers = [];        // 64 条连接工作池
    this.queue = [];          // 等待空闲连接的任务队列
    this.connected = false;
    this.peer = null;
    this.onWorkerReady = null; // (readyCount, totalCount) => {}
    this.onDisconnect = null;
    this.onReconnect = null;
    this.autoReconnect = true;
    this.reconnecting = false;
    this.heartbeatTimer = null;
    this.lastPong = Date.now();
    this.logger = (opts && opts.logger) || null;
  }

  PeerFS.prototype._log = function (msg, color) {
    console.log('[peerfs] ' + msg);
    if (this.logger) {
      try { this.logger(msg, color); } catch (e) {}
    }
  };

  // _wait 一次性事件包装为 Promise
  PeerFS.prototype._wait = function (emitter, ev, ms, what) {
    return new Promise(function (resolve, reject) {
      var timer = setTimeout(function () {
        cleanup();
        reject(new Error(what || (ev + ' timeout')));
      }, ms);
      function onOk(a) { cleanup(); resolve(a); }
      function onErr(e) { cleanup(); reject(e instanceof Error ? e : new Error(String(e))); }
      function cleanup() {
        clearTimeout(timer);
        emitter.off(ev, onOk);
        emitter.off('error', onErr);
      }
      emitter.once(ev, onOk);
      emitter.on('error', onErr);
    });
  };

  // 保活心跳：每 5 秒发送 UDP ping 报文，永久锁定两端 NAT/防火墙放行规则
  PeerFS.prototype._startHeartbeat = function () {
    var self = this;
    this._stopHeartbeat();
    this.lastPong = Date.now();
    this.heartbeatTimer = setInterval(function () {
      if (!self.connected || self.workers.length === 0) return;
      var count = Math.min(4, self.workers.length);
      for (var i = 0; i < count; i++) {
        var w = self.workers[i];
        if (w && w.conn && !w.busy) {
          try {
            w.conn.send(JSON.stringify({ type: 'ping' }));
          } catch (e) {}
        }
      }
    }, 5000);
  };

  PeerFS.prototype._stopHeartbeat = function () {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  };

  // 断线自动保活重连
  PeerFS.prototype._scheduleReconnect = function () {
    var self = this;
    if (this.reconnecting) return;
    this.reconnecting = true;
    this._stopHeartbeat();
    console.log('[peerfs] 连接已断开，2秒后自动尝试保活重连...');
    setTimeout(function () {
      if (!self.connected) {
        self.connect().then(function () {
          self.reconnecting = false;
          console.log('[peerfs] 保活重连成功！');
          if (self.onReconnect) self.onReconnect();
        }).catch(function (e) {
          self.reconnecting = false;
          console.warn('[peerfs] 重连重试失败，继续等待重试:', e);
          self._scheduleReconnect();
        });
      } else {
        self.reconnecting = false;
      }
    }, 2000);
  };

  // 建立 64 通道并行连接池
  PeerFS.prototype.connect = function () {
    var self = this;
    if (!this.peerId) return Promise.reject(new Error('peerId required'));

    this._log('正在连接 PeerJS 信令服务器: ' + this.host + ':' + this.port + (this.secure ? ' (TLS)' : ''), '#7aa2f7');

    this.peer = new Peer(undefined, {
      host: this.host,
      port: this.port,
      secure: this.secure,
      key: this.key,
      path: this.path,
      config: {
        iceServers: [
          { urls: 'stun:stun.l.google.com:19302' },
          { urls: 'stun:stun1.l.google.com:19302' },
          { urls: 'stun:stun.cloudflare.com:3478' }
        ],
      },
    });

    return this._wait(this.peer, 'open', 20000, 'signaling open timeout').then(async function () {
      self._log('信令就绪 (我的ID: ' + self.peer.id + ')，正在向目标节点 ' + self.peerId + ' 发起 WebRTC 连接...', '#7aa2f7');
      var numConns = Math.max(1, self.conns || 64);
      self.workers = [];
      self.connected = true;
      self._setupServiceWorkerBridge();

      // 方案 2：底层仅建立 1 个 WebRTC PeerConnection（单次 ICE 打洞 + 单次 DTLS 握手）
      var primary = await self._createPrimaryWorker();
      self.workers.push(primary);
      self._log('主数据通道 [0] 已打通并完成首帧鉴权！', '#73daca');
      if (self.onWorkerReady) self.onWorkerReady(self.workers.length, numConns);

      // 在该 PeerConnection (SCTP 协议栈) 上瞬间并发建立其余 63 条轻量 DataChannel (DCEP 原生协议)
      var pc = primary.conn.peerConnection;
      if (pc && numConns > 1) {
        self._log('正在利用底层 SCTP 关联并发拉起剩余 ' + (numConns - 1) + ' 条轻量通道...', '#7aa2f7');
        for (var i = 1; i < numConns; i++) {
          (function (idx) {
            try {
              var rdc = pc.createDataChannel('fs-' + idx, { ordered: true });
              rdc.binaryType = 'arraybuffer';
              var subWorker = {
                id: idx,
                dc: rdc,
                conn: {
                  open: false,
                  send: function (msg) { rdc.send(msg); },
                  close: function () { rdc.close(); },
                },
                busy: false,
                activeStream: null,
                pending: new Map(),
              };
              rdc.onopen = function () {
                subWorker.conn.open = true;
                try {
                  rdc.send(JSON.stringify({ type: 'hello', token: self.token || '' }));
                } catch (e) {}
                self.workers.push(subWorker);
                if (self.onWorkerReady) self.onWorkerReady(self.workers.length, numConns);
                if (self.workers.length === numConns) {
                  self._log('全部 ' + numConns + ' 条轻量 DataChannel 并发建立完成！', '#9ece6a');
                }
                if (self.queue.length > 0) {
                  var nextTask = self.queue.shift();
                  self._runTaskOnWorker(subWorker, nextTask);
                }
              };
              rdc.onmessage = function (ev) {
                self._onWorkerData(subWorker, ev.data);
              };
              rdc.onclose = function () {
                subWorker.conn.open = false;
                subWorker.busy = false;
                if (subWorker.activeStream && subWorker.activeStream.onError) {
                  subWorker.activeStream.onError(new Error('datachannel closed'));
                }
                subWorker.activeStream = null;
                subWorker.pending.forEach(function (p) { p.reject(new Error('channel closed')); });
                subWorker.pending.clear();
              };
            } catch (err) {
              console.warn('createDataChannel fs-' + idx + ' failed:', err);
            }
          })(i);
        }
      }

      self._startHeartbeat();
      return self;
    });
  };

  // 建立唯一的主 WebRTC DataChannel Worker
  PeerFS.prototype._createPrimaryWorker = function () {
    var self = this;
    var worker = {
      id: 0,
      conn: null,
      busy: false,
      activeStream: null,
      pending: new Map(),
    };

    var conn = this.peer.connect(this.peerId, {
      serialization: 'raw',
      reliable: true,
      metadata: { channelIndex: 0 },
    });
    worker.conn = conn;

    // 候选 HTTP 临时通道：底层 ICE 一旦生成候选，立即通过 HTTP 8080 临时通道推给服务端
    // 服务端借此第一时间向浏览器出站打 UDP STUN Ping 包，触发 NAT / 防火墙动态放行
    var hookICECandidate = function () {
      if (conn.peerConnection) {
        conn.peerConnection.addEventListener('icecandidate', function (ev) {
          if (ev.candidate && ev.candidate.candidate) {
            fetch('/__peerfs/candidate', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                candidate: ev.candidate.toJSON(),
                connectionId: conn.connectionId,
                peerId: self.peerId,
              }),
            }).catch(function () {});
          }
        });
      } else {
        setTimeout(hookICECandidate, 20);
      }
    };
    hookICECandidate();

    conn.on('data', function (d) {
      self._onWorkerData(worker, d);
    });

    conn.on('close', function () {
      self._stopHeartbeat();
      self.connected = false;
      worker.busy = false;
      if (worker.activeStream && worker.activeStream.onError) {
        worker.activeStream.onError(new Error('connection closed'));
      }
      worker.activeStream = null;
      worker.pending.forEach(function (p) {
        p.reject(new Error('channel closed'));
      });
      worker.pending.clear();
      if (self.onDisconnect) self.onDisconnect();
      if (self.autoReconnect) {
        self._scheduleReconnect();
      }
    });

    return this._wait(conn, 'open', 15000, 'datachannel [0] open timeout').then(function () {
      conn.send(JSON.stringify({ type: 'hello', token: self.token || '' }));
      return worker;
    });
  };

  // 分发数据帧给 Worker
  PeerFS.prototype._onWorkerData = function (worker, d) {
    if (typeof d === 'string') {
      var h;
      try { h = JSON.parse(d); } catch (e) { return; }

      // 0. 保活心跳回应
      if (h.type === 'pong') {
        this.lastPong = Date.now();
        return;
      }

      // 1. 如果是 meta 头：通知当前流式读取，并记录总大小
      if (h.type === 'meta') {
        if (worker.activeStream && worker.activeStream.onMeta) {
          worker.activeStream.onMeta(h);
        }
        var pMeta = h.reqId ? worker.pending.get(h.reqId) : null;
        if (pMeta && pMeta.onMeta) pMeta.onMeta(h);
        return;
      }

      // 2. 寻找对应的 reqId 回调
      var p = h.reqId ? worker.pending.get(h.reqId) : null;
      if (p) {
        worker.pending.delete(h.reqId);
        if (h.type === 'entries') {
          this._releaseWorker(worker);
          p.resolve(h.entries || []);
        } else if (h.type === 'done') {
          var stream = worker.activeStream;
          worker.activeStream = null;
          this._releaseWorker(worker);
          if (stream && stream.onDone) stream.onDone();
          p.resolve(stream ? stream.getResult() : null);
        } else if (h.type === 'err') {
          var errStream = worker.activeStream;
          worker.activeStream = null;
          this._releaseWorker(worker);
          var err = new Error(h.msg || 'server error');
          if (errStream && errStream.onError) errStream.onError(err);
          p.reject(err);
        } else {
          p.resolve(h);
        }
      }
      return;
    }

    // 二进制数据帧：直接推给当前 worker 的 activeStream
    if (worker.activeStream && worker.activeStream.onChunk) {
      worker.activeStream.onChunk(d);
    }
  };

  // 释放 Worker 并调度队列任务
  PeerFS.prototype._releaseWorker = function (worker) {
    worker.busy = false;
    worker.activeStream = null;
    if (this.queue.length > 0) {
      var nextTask = this.queue.shift();
      this._runTaskOnWorker(worker, nextTask);
    }
  };

  // 获取一个可用 Worker（优先空闲，无空闲则入排队）
  PeerFS.prototype._acquireWorker = function () {
    var self = this;
    return new Promise(function (resolve, reject) {
      if (!self.connected) {
        return reject(new Error('peerfs not connected'));
      }
      for (var i = 0; i < self.workers.length; i++) {
        var w = self.workers[i];
        var isOpen = w.conn && (w.conn.open === true || (w.dc && w.dc.readyState === 'open'));
        if (!w.busy && isOpen) {
          w.busy = true;
          return resolve(w);
        }
      }
      // 全忙，入队等待
      self.queue.push(resolve);
    });
  };

  PeerFS.prototype._runTaskOnWorker = function (worker, resolveTask) {
    worker.busy = true;
    resolveTask(worker);
  };

  // ===================== 核心 API =====================

  // 1. list(path) — 目录遍历（依然暴露！）
  PeerFS.prototype.list = function (dirPath) {
    var self = this;
    dirPath = dirPath || '/';
    return this._acquireWorker().then(function (worker) {
      return new Promise(function (resolve, reject) {
        var reqId = genReqId();
        worker.pending.set(reqId, {
          resolve: resolve,
          reject: function (err) {
            self._releaseWorker(worker);
            reject(err);
          },
        });
        worker.conn.send(JSON.stringify({ type: 'list', path: dirPath, reqId: reqId }));
      });
    });
  };

  // 2. stat(path) — 快速获取文件大小（无需传输数据）
  PeerFS.prototype.stat = function (filePath) {
    var self = this;
    return this._acquireWorker().then(function (worker) {
      return new Promise(function (resolve, reject) {
        var reqId = genReqId();
        var metaInfo = null;
        worker.pending.set(reqId, {
          onMeta: function (m) {
            metaInfo = {
              path: filePath,
              size: m.fileSize || m.total,
              total: m.total,
            };
          },
          resolve: function () {
            self._releaseWorker(worker);
            resolve(metaInfo || { path: filePath, size: 0 });
          },
          reject: function (err) {
            self._releaseWorker(worker);
            reject(err);
          },
        });
        // 请求 size: 0 即可触发 meta 后立即 done
        worker.conn.send(JSON.stringify({
          type: 'read',
          path: filePath,
          offset: 0,
          size: 0,
          reqId: reqId,
        }));
      });
    });
  };

  // 3. stream(path, opts) — 边下边输出的 ReadableStream（核心：边下边播/流式）
  PeerFS.prototype.stream = function (filePath, opts) {
    opts = opts || {};
    var self = this;
    var offset = opts.offset || 0;
    var size = opts.size === undefined ? -1 : opts.size;
    var reqId = genReqId();

    var streamController = null;
    var readable = new ReadableStream({
      start: function (controller) {
        streamController = controller;
      },
      cancel: function () {
        // 取消读取
      },
    });

    this._acquireWorker().then(function (worker) {
      worker.activeStream = {
        onMeta: function (m) {
          if (opts.onMeta) opts.onMeta(m);
        },
        onChunk: function (chunk) {
          var bytes = new Uint8Array(chunk);
          if (opts.onChunk) opts.onChunk(bytes);
          if (streamController) streamController.enqueue(bytes);
        },
        onDone: function () {
          if (streamController) streamController.close();
        },
        onError: function (err) {
          if (streamController) streamController.error(err);
        },
        getResult: function () { return null; },
      };

      worker.pending.set(reqId, {
        resolve: function () {},
        reject: function (err) {
          self._releaseWorker(worker);
          if (streamController) streamController.error(err);
        },
      });

      worker.conn.send(JSON.stringify({
        type: 'read',
        path: filePath,
        offset: offset,
        size: size,
        reqId: reqId,
      }));
    }).catch(function (err) {
      if (streamController) streamController.error(err);
    });

    return readable;
  };

  // 4. read(path, opts) — 读取完整文件或切片为 Blob
  PeerFS.prototype.read = function (filePath, opts) {
    opts = opts || {};
    var self = this;
    var offset = opts.offset || 0;
    var size = opts.size === undefined ? -1 : opts.size;
    var reqId = genReqId();

    return this._acquireWorker().then(function (worker) {
      return new Promise(function (resolve, reject) {
        var chunks = [];
        var received = 0;
        var total = -1;

        worker.activeStream = {
          onMeta: function (m) {
            total = m.total;
          },
          onChunk: function (chunk) {
            chunks.push(chunk);
            received += chunk.byteLength;
            if (opts.onProgress) opts.onProgress(received, total);
          },
          onDone: function () {
            // done
          },
          onError: function (err) {
            reject(err);
          },
          getResult: function () {
            var mime = opts.mime || getMime(filePath);
            return mime ? new Blob(chunks, { type: mime }) : new Blob(chunks);
          },
        };

        worker.pending.set(reqId, {
          resolve: function (blob) {
            resolve(blob);
          },
          reject: function (err) {
            self._releaseWorker(worker);
            reject(err);
          },
        });

        worker.conn.send(JSON.stringify({
          type: 'read',
          path: filePath,
          offset: offset,
          size: size,
          reqId: reqId,
        }));
      });
    });
  };

  // 5. url(path) — 获取文件 Blob URL（直接喂给 <img src> / <a href>）
  PeerFS.prototype.url = function (filePath, opts) {
    return this.read(filePath, opts).then(function (blob) {
      return URL.createObjectURL(blob);
    });
  };

  // 6. streamUrl(path) — 获取 Service Worker 虚拟流式播放 URL（边下边播+拖动进度条）
  PeerFS.prototype.streamUrl = function (filePath) {
    var clean = filePath.replace(/^\/+/, '');
    return '/__peerfs/stream/' + encodeURIComponent(clean);
  };

  // 7. readParallel(path, opts) — 64 通道分片并发多线程下载加速
  PeerFS.prototype.readParallel = async function (filePath, opts) {
    opts = opts || {};
    var meta = await this.stat(filePath);
    var totalSize = meta.size;
    if (totalSize <= 0) {
      return this.read(filePath, opts);
    }
    var numWorkers = this.workers.length || 1;
    var concurrency = Math.min(opts.concurrency || numWorkers, numWorkers, 64);
    if (totalSize < 512 * 1024 || concurrency <= 1) {
      return this.read(filePath, opts);
    }

    var chunkSize = Math.ceil(totalSize / concurrency);
    var parts = new Array(concurrency);
    var receivedPerChunk = new Array(concurrency).fill(0);
    var self = this;

    var tasks = [];
    for (var i = 0; i < concurrency; i++) {
      (function (idx) {
        var offset = idx * chunkSize;
        var size = Math.min(chunkSize, totalSize - offset);
        if (size <= 0) return;

        tasks.push(self.read(filePath, {
          offset: offset,
          size: size,
          onProgress: function (recv) {
            receivedPerChunk[idx] = recv;
            if (opts.onProgress) {
              var totalRecv = receivedPerChunk.reduce(function (a, b) { return a + b; }, 0);
              opts.onProgress(totalRecv, totalSize);
            }
          }
        }).then(function (blob) {
          parts[idx] = blob;
        }));
      })(i);
    }

    await Promise.all(tasks);
    var mime = opts.mime || getMime(filePath);
    return mime ? new Blob(parts, { type: mime }) : new Blob(parts);
  };

  // 8. readText(path, opts) — 直接读取文件为 UTF-8 字符串
  PeerFS.prototype.readText = function (filePath, opts) {
    return this.read(filePath, opts).then(function (blob) {
      return blob.text();
    });
  };

  // 8. download(path, filename, opts) — 直接下载文件到本地（大文件自动开启多通道分片加速）
  PeerFS.prototype.download = function (filePath, filename, opts) {
    opts = opts || {};
    var fname = filename || filePath.split('/').pop() || 'download';
    var self = this;
    return this.stat(filePath).then(function (meta) {
      var readPromise = (meta.size > 2 * 1024 * 1024 && self.workers.length > 1)
        ? self.readParallel(filePath, { concurrency: Math.min(self.workers.length, 32), onProgress: opts.onProgress })
        : self.read(filePath, opts);

      return readPromise.then(function (blob) {
        var blobUrl = URL.createObjectURL(blob);
        var a = document.createElement('a');
        a.href = blobUrl;
        a.download = fname;
        document.body.appendChild(a);
        a.click();
        setTimeout(function () {
          document.body.removeChild(a);
          URL.revokeObjectURL(blobUrl);
        }, 2000);
        return true;
      });
    });
  };

  // 8. get(path) — 极简统一取文件（无需 list，一次拿到流、Blob 与 URL）
  PeerFS.prototype.get = function (filePath) {
    var self = this;
    return {
      path: filePath,
      stat: function () { return self.stat(filePath); },
      blob: function (opts) { return self.read(filePath, opts); },
      url: function () { return self.url(filePath); },
      stream: function (opts) { return self.stream(filePath, opts); },
      streamUrl: function () { return self.streamUrl(filePath); },
      download: function (fname) { return self.download(filePath, fname); },
    };
  };

  // 9. play(path, mediaEl) — 播放助手（优先 Service Worker 虚拟 Range 边下边播，降级纯内存 Blob）
  PeerFS.prototype.play = function (filePath, mediaEl) {
    var self = this;
    var streamUrl = this.streamUrl(filePath);

    // 优先：如果浏览器已激活 Service Worker 控制器，采用虚拟 Range 流式播放（真正的边下边播 + 支持快进/快退 Seek）
    if ('serviceWorker' in navigator && navigator.serviceWorker.controller) {
      this._log('启用 Service Worker 虚拟 Range 管道，实现秒开【边下边播】: ' + filePath, '#73daca');
      mediaEl.src = streamUrl;
      return mediaEl.play().catch(function (err) {
        self._log('播放器等待手势或正在缓冲: ' + err.message, '#e0af68');
      });
    }

    // 次选：无 Service Worker 或未激活时，直接通过 DataChannel 并发拉取内存 Blob 播放
    this._log('Service Worker 未激活，使用 WebRTC DataChannel 内存直接解码: ' + filePath, '#e0af68');
    return this.url(filePath).then(function (u) {
      mediaEl.src = u;
      return mediaEl.play().catch(function () {});
    });
  };

  // ===================== Service Worker 桥接 =====================
  PeerFS.prototype._setupServiceWorkerBridge = function () {
    var self = this;
    if (!('serviceWorker' in navigator)) return;

    navigator.serviceWorker.addEventListener('message', function (event) {
      var data = event.data;
      if (!data || data.type !== 'PEER_RANGE_REQ') return;

      var port = event.ports && event.ports[0];
      if (!port) return;

      var filePath = data.path;
      var offset = data.offset || 0;
      var size = data.size === undefined ? -1 : data.size;
      var reqId = genReqId();

      self._acquireWorker().then(function (worker) {
        var aborted = false;

        port.onmessage = function (pe) {
          if (pe.data && pe.data.type === 'abort') {
            aborted = true;
            self._releaseWorker(worker);
          }
        };

        worker.activeStream = {
          onMeta: function (m) {
            if (aborted) return;
            port.postMessage({
              type: 'meta',
              total: m.total,
              fileSize: m.fileSize || m.total,
            });
          },
          onChunk: function (chunk) {
            if (aborted) return;
            // 零拷贝转移 ArrayBuffer
            try {
              port.postMessage({ type: 'chunk', data: chunk }, [chunk]);
            } catch (e) {
              port.postMessage({ type: 'chunk', data: chunk });
            }
          },
          onDone: function () {
            if (aborted) return;
            port.postMessage({ type: 'done' });
          },
          onError: function (err) {
            if (aborted) return;
            port.postMessage({ type: 'error', msg: err.message });
          },
          getResult: function () { return null; },
        };

        worker.pending.set(reqId, {
          resolve: function () {},
          reject: function (err) {
            self._releaseWorker(worker);
            if (!aborted) port.postMessage({ type: 'error', msg: err.message });
          },
        });

        worker.conn.send(JSON.stringify({
          type: 'read',
          path: filePath,
          offset: offset,
          size: size,
          reqId: reqId,
        }));
      }).catch(function (err) {
        port.postMessage({ type: 'error', msg: err.message });
      });
    });
  };

  PeerFS.prototype.close = function () {
    this.connected = false;
    for (var i = 0; i < this.workers.length; i++) {
      try { this.workers[i].conn.close(); } catch (e) {}
    }
    this.workers = [];
    this.queue = [];
    try { if (this.peer) this.peer.destroy(); } catch (e) {}
  };

  global.PeerFS = PeerFS;
})(typeof window !== 'undefined' ? window : this);
