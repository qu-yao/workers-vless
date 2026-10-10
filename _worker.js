import { connect } from 'cloudflare:sockets';

// 填入你自己的 UUID，或者在 Pages 中添加 USER_ID 加密环境变量。
const USER_ID = '626aa69b-305c-4e8e-8fd7-da948ebbf0c1';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TIMEOUT_MS = 10000;

export default {
  async fetch(request, env) {
    if (request.headers.get('Upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('Service Operational', {
        headers: { 'Content-Type': 'text/plain; charset=utf-8' },
      });
    }

    const userID = String(env?.USER_ID || USER_ID).trim().toLowerCase();
    if (!UUID_PATTERN.test(userID)) {
      return new Response('Configure USER_ID before connecting.', { status: 500 });
    }
    // 此版本不处理 HTTP 头中的首包数据。客户端路径用 /，关闭 Early Data。
    if (new URL(request.url).searchParams.has('ed') ||
        request.headers.has('Sec-WebSocket-Protocol')) {
      return new Response('Disable WebSocket Early Data and use path /.', { status: 400 });
    }
    return createTunnel(userID);
  },
};

function createTunnel(userID) {
  const [client, server] = Object.values(new WebSocketPair());
  server.accept();
  server.binaryType = 'arraybuffer';

  let headerBuffer = new Uint8Array(0);
  let socket = null;
  let writer = null;
  let stopped = false;
  let pending = Promise.resolve();
  const headerTimer = setTimeout(() => fail(new Error('VLESS header timeout')), TIMEOUT_MS);

  function stop(code = 1000, reason = 'Connection closed') {
    if (stopped) return;
    stopped = true;
    clearTimeout(headerTimer);
    headerBuffer = new Uint8Array(0);
    if (socket) {
      try { Promise.resolve(socket.close()).catch(() => {}); } catch {}
    }
    try {
      if (server.readyState === 1) server.close(code, reason);
    } catch {}
  }

  function fail(error) {
    console.error('[vless] connection failed:', error?.message || String(error));
    stop(1011, 'Tunnel failed; check server logs');
  }

  // 排队处理消息：首包只解析一次，后续数据全部写入同一条 TCP 连接。
  server.addEventListener('message', (event) => {
    pending = pending.then(async () => {
      if (stopped) return;
      const chunk = await toBytes(event.data);
      if (stopped || chunk.byteLength === 0) return;
      if (writer) {
        await writer.write(chunk);
        return;
      }

      headerBuffer = concatBytes(headerBuffer, chunk);
      const header = parseHeader(headerBuffer, userID);
      if (!header) return; // 地址和附加信息可能跨 WebSocket 消息。
      clearTimeout(headerTimer);
      headerBuffer = new Uint8Array(0);

      console.log('[vless] connecting TCP');
      socket = connect({ hostname: header.hostname, port: header.port });
      socket.closed.catch(fail);
      await withTimeout(socket.opened, TIMEOUT_MS);
      if (stopped) return;
      writer = socket.writable.getWriter();
      console.log('[vless] TCP connected');
      void readRemote(header.version).catch(fail);
      if (header.payload.byteLength) await writer.write(header.payload);
    }).catch(fail);
  });
  server.addEventListener('close', () => stop());
  server.addEventListener('error', () => fail(new Error('WebSocket error')));

  async function readRemote(version) {
    const reader = socket.readable.getReader();
    let needsResponseHeader = true;
    try {
      while (!stopped) {
        const { value, done } = await reader.read();
        if (done) break;
        if (!value?.byteLength) continue;
        if (stopped || server.readyState !== 1) break;
        // VLESS 响应必须带版本和附加信息长度；仅在首次响应中添加。
        server.send(needsResponseHeader
          ? concatBytes(new Uint8Array([version, 0]), value)
          : value);
        needsResponseHeader = false;
      }
    } finally {
      reader.releaseLock();
      try { writer?.releaseLock(); } catch {}
      stop();
    }
  }

  return new Response(null, { status: 101, webSocket: client });
}

function parseHeader(bytes, userID) {
  if (bytes.length < 18) return null;
  if (bytes[0] !== 0) throw new Error('Unsupported VLESS version');
  if (stringifyUUID(bytes.subarray(1, 17)) !== userID) {
    throw new Error('UUID authentication failed');
  }
  const commandIndex = 18 + bytes[17];
  if (bytes.length < commandIndex + 4) return null;
  if (bytes[commandIndex] !== 1) throw new Error('Only TCP is supported');

  const port = (bytes[commandIndex + 1] << 8) | bytes[commandIndex + 2];
  if (!port) throw new Error('Invalid destination port');
  const addressType = bytes[commandIndex + 3];
  let offset = commandIndex + 4;
  let hostname;

  if (addressType === 1) {
    if (bytes.length < offset + 4) return null;
    hostname = bytes.subarray(offset, offset + 4).join('.');
    offset += 4;
  } else if (addressType === 2) {
    if (bytes.length < offset + 1) return null;
    const length = bytes[offset++];
    if (!length) throw new Error('Empty destination hostname');
    if (bytes.length < offset + length) return null;
    hostname = new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(offset, offset + length));
    offset += length;
  } else if (addressType === 3) {
    if (bytes.length < offset + 16) return null;
    const groups = [];
    for (let i = 0; i < 16; i += 2) {
      groups.push(((bytes[offset + i] << 8) | bytes[offset + i + 1]).toString(16));
    }
    hostname = groups.join(':');
    offset += 16;
  } else {
    throw new Error('Unsupported address type');
  }
  return { version: bytes[0], hostname, port, payload: bytes.subarray(offset) };
}

async function toBytes(data) {
  if (data instanceof ArrayBuffer) return new Uint8Array(data);
  if (ArrayBuffer.isView(data)) return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  if (typeof Blob !== 'undefined' && data instanceof Blob) return new Uint8Array(await data.arrayBuffer());
  throw new Error('Binary WebSocket messages required');
}

function concatBytes(a, b) {
  const result = new Uint8Array(a.byteLength + b.byteLength);
  result.set(a);
  result.set(b, a.byteLength);
  return result;
}

function stringifyUUID(bytes) {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

async function withTimeout(promise, milliseconds) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error('TCP connect timeout')), milliseconds);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}
