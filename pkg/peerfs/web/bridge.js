/* peerfs bridge.js — 浏览器端封装：标准 peerjs（serialization:"raw"）连 Go 节点，
 * 收发与 pkg/peerfs 约定的帧协议：文本帧 = JSON 控制头，二进制帧 = 文件块。
 *
 * 依赖页面先加载 peerjs UMD 包（暴露全局 Peer）。
 * 用法：
 *   const fs = await new PeerFS({ peerId: 'wt-media-x', host, port, secure, key, token }).connect();
 *   const entries = await fs.list('/');                  // 目录列表
 *   const blob = await fs.read('a.jpg');                 // 整文件 → Blob
 *   const url = await fs.url('v.mp4');                   // Blob objectURL（<video> 可拖进度条）
 */
(function (global) {
  'use strict';

  var DEFAULTS = {
    host: '0.peerjs.com', port: 443, secure: true, key: 'peerjs', path: '/',
    peerId: '', token: '',
  };

  function genReqId() {
    return 'r' + Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
  }

  function PeerFS(opts) {
    Object.assign(this, DEFAULTS, opts || {});
    this._pending = new Map();   // reqId → {onHeader}
    this._activeRead = null;     // 当前读流（lock-step：同一时刻只有一个）
    this.connected = false;
  }

  // _wait 把一次性事件包成 Promise（带超时与 error 透传）。
  PeerFS.prototype._wait = function (emitter, ev, ms, what) {
    var self = this;
    return new Promise(function (resolve, reject) {
      var timer = setTimeout(function () {
        cleanup(); reject(new Error(what || (ev + ' timeout')));
      }, ms);
      function onOk(a) { clearTimeout(timer); cleanup(); resolve(a); }
      function onErr(e) { clearTimeout(timer); cleanup(); reject(e instanceof Error ? e : new Error(String(e))); }
      function cleanup() {
        emitter.off(ev, onOk); emitter.off('error', onErr);
      }
      emitter.once(ev, onOk); emitter.on('error', onErr);
      void self;
    });
  };

  PeerFS.prototype.connect = function () {
    var self = this;
    if (!this.peerId) return Promise.reject(new Error('peerId required'));
    this.peer = new Peer(undefined, {
      host: this.host, port: this.port, secure: this.secure,
      key: this.key, path: this.path,
      config: { iceServers: [{ urls: 'stun:stun.l.google.com:19302' }] },
    });
    return this._wait(this.peer, 'open', 20000, 'signaling open timeout').then(function () {
      // serialization:'raw' 必须由发起方声明：string→文本帧 / ArrayBuffer→二进制帧直传
      self.conn = self.peer.connect(self.peerId, { serialization: 'raw', reliable: true });
      self.conn.on('data', function (d) { self._onData(d); });
      self.conn.on('close', function () {
        self.connected = false;
        self._rejectAll('datachannel closed');
      });
      return self._wait(self.conn, 'open', 30000, 'datachannel open timeout');
    }).then(function () {
      self.conn.send(JSON.stringify({ type: 'hello', token: self.token || '' }));
      self.connected = true;
      return self;
    });
  };

  PeerFS.prototype.close = function () {
    this.connected = false;
    this._rejectAll('client closed');
    try { if (this.conn) this.conn.close(); } catch (e) {}
    try { if (this.peer) this.peer.destroy(); } catch (e) {}
  };

  PeerFS.prototype._onData = function (d) {
    if (typeof d === 'string') {
      var h; try { h = JSON.parse(d); } catch (e) { return; }
      var p = h.reqId ? this._pending.get(h.reqId) : null;
      if (p) p.onHeader(h);
      return;
    }
    // 二进制块：属于当前读流（服务端 lock-step 保证只有一条流在传）
    var st = this._activeRead;
    if (!st) return;
    st.chunks.push(d);
    st.received += d.byteLength;
    if (st.onProgress) st.onProgress(st.received, st.total);
  };

  PeerFS.prototype._rejectAll = function (why) {
    var pend = this._pending; this._pending = new Map();
    this._activeRead = null;
    pend.forEach(function (p) { p.reject(new Error(why)); });
  };

  PeerFS.prototype._sendHeader = function (h) {
    this.conn.send(JSON.stringify(h));
  };

  // list(path) → Entry[]；Entry = {name, dir?, size?}
  PeerFS.prototype.list = function (path) {
    var self = this;
    path = path || '/';
    return new Promise(function (resolve, reject) {
      var reqId = genReqId();
      self._pending.set(reqId, {
        resolve: resolve, reject: reject,
        onHeader: function (h) {
          self._pending.delete(reqId);
          if (h.type === 'entries') resolve(h.entries || []);
          else reject(new Error(h.msg || ('unexpected ' + h.type)));
        },
      });
      self._sendHeader({ type: 'list', path: path, reqId: reqId });
    });
  };

  // read(path, {offset,size,onProgress}) → Blob。size=-1 读到尾；
  // 视频 seek 用 offset 分段拉，或整文件拉回本地 Blob 后随意 seek。
  PeerFS.prototype.read = function (path, opts) {
    var self = this;
    opts = opts || {};
    return new Promise(function (resolve, reject) {
      var reqId = genReqId();
      var state = { chunks: [], received: 0, total: -1, onProgress: opts.onProgress };
      self._activeRead = state;
      self._pending.set(reqId, {
        resolve: resolve, reject: reject,
        onHeader: function (h) {
          if (h.type === 'meta') { state.total = h.total; return; }
          self._pending.delete(reqId);
          self._activeRead = null;
          if (h.type === 'done') {
            if (state.total >= 0 && state.received !== state.total) {
              reject(new Error('truncated: got ' + state.received + '/' + state.total));
            } else {
              resolve(new Blob(state.chunks));
            }
          } else {
            reject(new Error(h.msg || ('unexpected ' + h.type)));
          }
        },
      });
      self._sendHeader({ type: 'read', path: path, offset: opts.offset | 0, size: opts.size === undefined ? -1 : opts.size, reqId: reqId });
    });
  };

  // url(path) → objectURL，直接喂 <img>/<video>/<audio> 的 src。
  PeerFS.prototype.url = function (path, opts) {
    return this.read(path, opts).then(function (b) { return URL.createObjectURL(b); });
  };

  global.PeerFS = PeerFS;
})(window);
