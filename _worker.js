import { connect } from 'cloudflare:sockets';

// =================================================================
// 核心配置：请把下面的 UUID 替换为你自己的专属密钥
// =================================================================
const userID = 'de305d54-75b4-431b-adb2-eb6b9e546014'; 

export default {
  async fetch(request, env, ctx) {
    try {
      const upgradeHeader = request.headers.get('Upgrade');
      if (!upgradeHeader || upgradeHeader !== 'websocket') {
        // 防探测伪装网页
        return new Response(`<html><body style="font-family:sans-serif;text-align:center;padding-top:50px;">
          <h1>Service Operational</h1>
          <p>The server is running successfully.</p>
        </body></html>`, {
          headers: { 'Content-Type': 'text/html; charset=utf-8' },
        });
      }
      return await vlessOverWSHandler(request);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function vlessOverWSHandler(request) {
  const webSocketPair = new WebSocketPair();
  const [client, server] = Object.values(webSocketPair);
  server.accept();

  let address = '';
  let portWithRandomLog = '';
  const log = (info, event) => { console.log(`[${address}:${portWithRandomLog}] ${info}`, event || ''); };

  const readableStream = new ReadableStream({
    start(controller) {
      server.addEventListener('message', (event) => {
        controller.enqueue(event.data);
      });
      server.addEventListener('close', () => { safeCloseWebSocket(server); controller.close(); });
      server.addEventListener('error', (err) => { log('webSocket 错误', err); safeCloseWebSocket(server); controller.close(); });
    },
  });

  let vlessBuffer = null;
  let remoteSocketWapper = { value: null };

  readableStream.pipeTo(new WritableStream({
    async write(chunk, controller) {
      if (vlessBuffer) {
        vlessBuffer = concatBuffer(vlessBuffer, chunk);
      } else {
        vlessBuffer = chunk;
      }
      
      if (vlessBuffer.byteLength < 24) return;
      
      const version = new Uint8Array(vlessBuffer.slice(0, 1));
      const id = new Uint8Array(vlessBuffer.slice(1, 17));
      
      // 【Bug 1 已修复】修正逻辑判断，确保 UUID 正确比对
      if (stringify(id) !== userID) {
        log('UUID 验证失败，拒绝连接');
        safeCloseWebSocket(server);
        return;
      }
      
      const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];
      const cmd = new Uint8Array(vlessBuffer.slice(18 + optLength, 19 + optLength))[0];
      
      if (cmd !== 1) {
        log(`不支持的命令: ${cmd} (只支持 TCP 连接)`);
        safeCloseWebSocket(server);
        return;
      }
      
      const portIndex = 19 + optLength;
      const portBuffer = vlessBuffer.slice(portIndex, portIndex + 2);
      const port = new DataView(portBuffer).getUint16(0);
      portWithRandomLog = port;
      
      const addressTypeIndex = portIndex + 2;
      const addressType = new Uint8Array(vlessBuffer.slice(addressTypeIndex, addressTypeIndex + 1))[0];
      let addressLength = 0;
      let addressBeginIndex = addressTypeIndex + 1;
      
      if (addressType === 1) {
        addressLength = 4;
        address = new Uint8Array(vlessBuffer.slice(addressBeginIndex, addressBeginIndex + addressLength)).join('.');
      } else if (addressType === 2) {
        addressLength = new Uint8Array(vlessBuffer.slice(addressBeginIndex, addressBeginIndex + 1))[0];
        addressBeginIndex += 1;
        address = new TextDecoder().decode(vlessBuffer.slice(addressBeginIndex, addressBeginIndex + addressLength));
      } else if (addressType === 3) {
        addressLength = 16;
        const dataView = new DataView(vlessBuffer.slice(addressBeginIndex, addressBeginIndex + addressLength));
        const ipv6 = [];
        for (let i = 0; i < 8; i++) { ipv6.push(dataView.getUint16(i * 2).toString(16)); }
        address = ipv6.join(':');
      } else {
        log(`不支持的地址类型: ${addressType}`);
        return;
      }
      
      const remoteAddress = address;
      const vlessHeaderLength = addressBeginIndex + addressLength;
      const rawClientData = vlessBuffer.slice(vlessHeaderLength);
      
      vlessBuffer = new Uint8Array(0); // 清空
      
      log(`正在连接目标服务器...`);
      await handleTCPConnect(remoteSocketWapper, remoteAddress, port, rawClientData, server, log);
    }
  })).catch((err) => { log('流管道异常错误', err); });

  return new Response(null, { status: 101, webSocket: client });
}

async function handleTCPConnect(remoteSocketWapper, remoteAddress, port, rawClientData, webSocket, log) {
  try {
    const tcpSocket = connect({ hostname: remoteAddress, port: port });
    remoteSocketWapper.value = tcpSocket;
    const writer = tcpSocket.writable.getWriter();
    await writer.write(rawClientData);
    writer.releaseLock();
    
    tcpSocket.readable.pipeTo(new WritableStream({
      async write(chunk) {
        if (webSocket.readyState === WebSocket.OPEN) {
          // 【Bug 2 已修复】移除多余的 [0, 0] 填充，原汁原味回传标准 VLESS 协议流
          webSocket.send(concatBuffer(new Uint8Array(), chunk));
        }
      },
      close() { log('目标服务器主动断开 TCP'); safeCloseWebSocket(webSocket); },
      abort(err) { log('目标服务器连接异常终止', err); safeCloseWebSocket(webSocket); }
    }));
  } catch (e) {
    log('TCP 连接失败', e);
    safeCloseWebSocket(webSocket);
  }
}

function concatBuffer(buffer1, buffer2) {
  const tmp = new Uint8Array(buffer1.byteLength + buffer2.byteLength);
  tmp.set(new Uint8Array(buffer1), 0);
  tmp.set(new Uint8Array(buffer2), buffer1.byteLength);
  return tmp.buffer;
}

function stringify(arr) {
  const uuids = [];
  for (let i = 0; i < arr.length; i++) {
    uuids.push((arr[i] < 16 ? '0' : '') + arr[i].toString(16));
  }
  return [
    uuids.slice(0, 4).join(''), uuids.slice(4, 6).join(''),
    uuids.slice(6, 8).join(''), uuids.slice(8, 10).join(''),
    uuids.slice(10, 16).join('')
  ].join('-');
}

function safeCloseWebSocket(socket) {
  try { if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) socket.close(); } catch (e) {}
}
