/* peerfs sw.js — Service Worker 虚拟 HTTP 代理
 * 拦截 /__peerfs/stream/* 的 HTTP Range 请求，通过 MessageChannel 转给主页面的
 * PeerFS WebRTC 连接池，实现原生 <video> 边下边播与随意拖动进度条（206 Partial Content）。
 */

self.addEventListener('install', function (e) {
  self.skipWaiting();
});

self.addEventListener('activate', function (e) {
  e.waitUntil(self.clients.claim());
});

function getMime(path) {
  var ext = (path.split('.').pop() || '').toLowerCase();
  var mimes = {
    'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska', 'mov': 'video/quicktime',
    'mp3': 'audio/mpeg', 'ogg': 'audio/ogg', 'wav': 'audio/wav', 'flac': 'audio/flac', 'm4a': 'audio/mp4',
    'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif', 'webp': 'image/webp',
    'avif': 'image/avif', 'svg': 'image/svg+xml', 'pdf': 'application/pdf', 'txt': 'text/plain; charset=utf-8',
    'json': 'application/json'
  };
  return mimes[ext] || 'application/octet-stream';
}

self.addEventListener('fetch', function (event) {
  var url = new URL(event.request.url);
  var prefix = '/__peerfs/stream/';
  var idx = url.pathname.indexOf(prefix);
  if (idx === -1) return;

  var filePath = decodeURIComponent(url.pathname.substring(idx + prefix.length));
  if (!filePath) return;

  event.respondWith((async function () {
    console.log('[SW-Range] 收到虚拟流媒体请求:', filePath, 'Range:', rangeHeader || '全量');
    var client = null;
    if (event.clientId) {
      client = await self.clients.get(event.clientId);
    }
    if (!client) {
      var all = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
      if (all && all.length > 0) client = all[0];
    }
    if (!client) {
      console.warn('[SW-Range] 503: 未找到活跃的 PeerFS 页面客户端');
      return new Response('No active peerfs page to handle request', { status: 503 });
    }

    // 解析 Range 请求头
    var rangeHeader = event.request.headers.get('Range') || '';
    var start = 0;
    var end = -1;
    if (rangeHeader) {
      var match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
      if (match) {
        start = parseInt(match[1], 10) || 0;
        if (match[2]) end = parseInt(match[2], 10);
      }
    }

    var channel = new MessageChannel();
    var earlyChunks = [];
    var isDone = false;
    var earlyError = null;
    var streamController = null;
    var metaResolver = null;
    var metaRejecter = null;

    channel.port1.onmessage = function (e) {
      var msg = e.data;
      if (!msg) return;
      if (msg.type === 'meta') {
        if (metaResolver) metaResolver(msg);
      } else if (msg.type === 'chunk') {
        if (streamController) {
          streamController.enqueue(new Uint8Array(msg.data));
        } else {
          earlyChunks.push(msg.data);
        }
      } else if (msg.type === 'done') {
        if (streamController) {
          streamController.close();
        } else {
          isDone = true;
        }
      } else if (msg.type === 'error') {
        var err = new Error(msg.msg || 'read error');
        if (metaRejecter) metaRejecter(err);
        if (streamController) {
          streamController.error(err);
        } else {
          earlyError = err;
        }
      }
    };

    // 等待主页面返回 meta 头
    var metaPromise = new Promise(function (resolve, reject) {
      var timeout = setTimeout(function () {
        reject(new Error('peerfs bridge response timeout (15s)'));
      }, 15000);

      metaResolver = function (data) {
        clearTimeout(timeout);
        resolve(data);
      };
      metaRejecter = function (err) {
        clearTimeout(timeout);
        reject(err);
      };
    });

    var reqSize = (end >= start) ? (end - start + 1) : -1;
    client.postMessage({
      type: 'PEER_RANGE_REQ',
      path: filePath,
      offset: start,
      size: reqSize,
    }, [channel.port2]);

    try {
      var meta = await metaPromise;
      var total = meta.total;
      var fileSize = meta.fileSize || total;
      var actualEnd = (end >= start && end < start + total) ? end : (start + total - 1);
      console.log('[SW-Range] 元数据已确认: total=' + total + ', fileSize=' + fileSize + '，返回 206 管道');

      var stream = new ReadableStream({
        start: function (controller) {
          streamController = controller;
          // 冲刷在 controller 初始化前到达的早期数据块
          while (earlyChunks.length > 0) {
            controller.enqueue(new Uint8Array(earlyChunks.shift()));
          }
          if (isDone) {
            controller.close();
          }
          if (earlyError) {
            controller.error(earlyError);
          }
        },
        cancel: function () {
          try {
            channel.port1.postMessage({ type: 'abort' });
          } catch (e) {}
        }
      });

      var headers = {
        'Content-Type': getMime(filePath),
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'no-cache, no-store',
      };

      if (rangeHeader) {
        headers['Content-Range'] = 'bytes ' + start + '-' + actualEnd + '/' + fileSize;
        headers['Content-Length'] = String(total);
        return new Response(stream, {
          status: 206,
          statusText: 'Partial Content',
          headers: headers,
        });
      } else {
        headers['Content-Length'] = String(total);
        return new Response(stream, {
          status: 200,
          statusText: 'OK',
          headers: headers,
        });
      }
    } catch (err) {
      console.error('[SW-Range] 流媒体虚拟拦截异常:', err.message);
      return new Response('PeerFS Stream Pipeline Error: ' + err.message, {
        status: 503,
        statusText: 'Service Unavailable',
        headers: { 'Content-Type': 'text/plain; charset=utf-8' },
      });
    }
  })());
});
