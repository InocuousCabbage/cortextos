import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import { TelegramAPI, formatValidateError } from '../../../src/telegram/api';

// ---------------------------------------------------------------------------
// Fetch timeout tests (from main — pre-existing)
// ---------------------------------------------------------------------------
describe('TelegramAPI fetch timeout', () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('throws a timeout error when fetch hangs indefinitely', async () => {
    globalThis.fetch = vi.fn(
      (_input: any, init?: any) =>
        new Promise((_resolve, reject) => {
          init?.signal?.addEventListener('abort', () => {
            const err = new Error('aborted');
            err.name = 'AbortError';
            reject(err);
          });
        }),
    ) as any;

    const api = new TelegramAPI('123:TEST');
    await expect(api.getUpdates(0, 1)).rejects.toThrow(/timed out after 15s/);
  }, 20000);

  it('succeeds on normal fetch response', async () => {
    globalThis.fetch = vi.fn(async () =>
      new Response(JSON.stringify({ ok: true, result: [] }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    ) as any;

    const api = new TelegramAPI('123:TEST');
    const res = await api.getUpdates(0, 1);
    expect(res.ok).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// validateCredentials tests (from pr-58)
// Minimal fetch mock — each test queues up 1 or 2 responses (one for getMe,
// optionally one for getChat) and asserts the resulting ValidateCredentialsResult.
// ---------------------------------------------------------------------------
type MockResponse = { status: number; body: any } | { throws: Error };

let responseQueue: MockResponse[] = [];
let callLog: Array<{ url: string; body: any }> = [];

function queue(response: MockResponse): void {
  responseQueue.push(response);
}

describe('TelegramAPI.validateCredentials', () => {
  beforeEach(() => {
    responseQueue = [];
    callLog = [];
    vi.stubGlobal('fetch', vi.fn(async (url: string, init: RequestInit) => {
      const body = init?.body ? JSON.parse(String(init.body)) : {};
      callLog.push({ url, body });
      const next = responseQueue.shift();
      if (!next) {
        throw new Error('fetch called with no queued response');
      }
      if ('throws' in next) {
        throw next.throws;
      }
      return {
        ok: next.status === 200,
        status: next.status,
        json: async () => next.body,
      } as any;
    }));
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('happy path: valid token + reachable user chat returns ok=true', async () => {
    queue({ status: 200, body: { ok: true, result: { id: 111, username: 'my_test_bot' } } });
    queue({
      status: 200,
      body: { ok: true, result: { id: 222, type: 'private', first_name: 'Alice', is_bot: false } },
    });

    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('222');

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.botId).toBe(111);
      expect(result.botUsername).toBe('my_test_bot');
      expect(result.chatType).toBe('private');
      expect(result.chatTitle).toBe('Alice');
    }
    expect(callLog[0].url).toContain('/getMe');
    expect(callLog[1].url).toContain('/getChat');
    expect(callLog[1].body.chat_id).toBe('222');
  });

  it('bad_token: getMe returns 401 -> reason=bad_token', async () => {
    queue({ status: 401, body: { ok: false, error_code: 401, description: 'Unauthorized' } });

    const api = new TelegramAPI('999:BAD');
    const result = await api.validateCredentials('222');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('bad_token');
    }
    // Critical: error message must not leak any part of the token.
    if (!result.ok) {
      const msg = formatValidateError(result);
      expect(msg).not.toContain('999');
      expect(msg).not.toContain('BAD');
      expect(msg).toMatch(/401 Unauthorized/);
    }
    // Must NOT have attempted getChat once getMe failed.
    expect(callLog).toHaveLength(1);
    expect(callLog[0].url).toContain('/getMe');
  });

  it('self_chat: CHAT_ID equals getMe.id -> reason=self_chat (no getChat call)', async () => {
    queue({ status: 200, body: { ok: true, result: { id: 1234567890, username: 'self_chat_test_bot' } } });

    const api = new TelegramAPI('1234567890:AAF3-rr');
    const result = await api.validateCredentials('1234567890');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('self_chat');
      expect(result.detail).toBe('1234567890');
      const msg = formatValidateError(result);
      // The error message must name the trap, point at the fix, and NOT
      // leak any part of the token.
      expect(msg).toContain('1234567890');
      expect(msg).toContain('BOT_TOKEN prefix');
      expect(msg).toContain('/start');
      expect(msg).toContain('getUpdates');
      expect(msg).not.toContain('AAF3');
    }
    // self_chat is caught after getMe alone — getChat must not have been called.
    expect(callLog).toHaveLength(1);
  });

  it('chat_not_found: getChat returns 400 -> reason=chat_not_found', async () => {
    queue({ status: 200, body: { ok: true, result: { id: 111, username: 'my_test_bot' } } });
    queue({ status: 400, body: { ok: false, error_code: 400, description: 'Bad Request: chat not found' } });

    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('222');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('chat_not_found');
      expect(result.detail).toBe('222');
      const msg = formatValidateError(result);
      expect(msg).toContain('222');
      expect(msg).toContain('/start');
    }
    expect(callLog).toHaveLength(2);
  });

  it('bot_recipient: getChat returns a bot user -> reason=bot_recipient', async () => {
    queue({ status: 200, body: { ok: true, result: { id: 111, username: 'my_test_bot' } } });
    queue({
      status: 200,
      body: { ok: true, result: { id: 333, type: 'private', username: 'other_bot', is_bot: true } },
    });

    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('333');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('bot_recipient');
      const msg = formatValidateError(result);
      expect(msg).toContain('333');
      expect(msg).toContain('bot');
    }
    expect(callLog).toHaveLength(2);
  });

  it('bot_recipient: getChat throws 403 "bots cant send messages to bots" -> reason=bot_recipient', async () => {
    queue({ status: 200, body: { ok: true, result: { id: 111, username: 'my_test_bot' } } });
    queue({
      status: 403,
      body: { ok: false, error_code: 403, description: "Forbidden: bots can't send messages to bots" },
    });

    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('333');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('bot_recipient');
    }
  });

  it('network_error: fetch throws -> reason=network_error (caller treats as WARN)', async () => {
    // post() retries once on transport-level errors (undici keepalive fix).
    // A genuine DNS failure persists across attempts, so queue the same
    // ENOTFOUND for both calls; final wrapped error still preserves it.
    queue({ throws: new Error('getaddrinfo ENOTFOUND api.telegram.org') });
    queue({ throws: new Error('getaddrinfo ENOTFOUND api.telegram.org') });

    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('222');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('network_error');
      expect(result.detail).toContain('ENOTFOUND');
      const msg = formatValidateError(result);
      expect(msg).toMatch(/Telegram API/i);
    }
  });

  it('rate_limited: getMe 429 -> reason=rate_limited', async () => {
    queue({
      status: 429,
      body: { ok: false, error_code: 429, description: 'Too Many Requests: retry after 5' },
    });

    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('222');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('rate_limited');
    }
  });

  it('timeout: fetch never resolves -> reason=network_error with "timed out" detail', async () => {
    // Queue nothing — fetch will just hang. Then advance fake timers past 10s
    // and assert the validator bails with network_error.
    vi.useFakeTimers();
    try {
      // Override the stubbed fetch to return a never-resolving promise.
      vi.stubGlobal('fetch', vi.fn(() => new Promise(() => { /* never resolves */ })));

      const api = new TelegramAPI('111:AAA');
      const pending = api.validateCredentials('222');

      // Advance fake timers past the 10s withTimeout cap. The internal
      // setTimeout in withTimeout rejects, validateCredentials catches,
      // returns network_error.
      await vi.advanceTimersByTimeAsync(10_500);

      const result = await pending;
      expect(result.ok).toBe(false);
      if (!result.ok) {
        expect(result.reason).toBe('network_error');
        expect(result.detail).toMatch(/timed out after 10s/);
      }
    } finally {
      vi.useRealTimers();
    }
  });

  it('empty chat_id: reason=chat_not_found with no API calls', async () => {
    // Note: this must NOT call fetch at all, so queue nothing.
    const api = new TelegramAPI('111:AAA');
    const result = await api.validateCredentials('');

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('chat_not_found');
    }
    expect(callLog).toHaveLength(0);
  });
});

describe('formatValidateError', () => {
  it('bad_token: does not leak token or detail in user-facing text', () => {
    const msg = formatValidateError({
      ok: false,
      reason: 'bad_token',
      detail: 'Telegram API error: Unauthorized TOKEN_SECRET_123',
    });
    expect(msg).not.toContain('TOKEN_SECRET_123');
    expect(msg).toMatch(/invalid or revoked/);
  });

  it('self_chat: message includes concrete fix instructions', () => {
    const msg = formatValidateError({ ok: false, reason: 'self_chat', detail: '1234567890' });
    expect(msg).toContain('1234567890');
    expect(msg).toContain('BOT_TOKEN prefix');
    expect(msg).toContain('/start');
    expect(msg).toContain('getUpdates');
  });

  it('chat_not_found: suggests /start', () => {
    const msg = formatValidateError({ ok: false, reason: 'chat_not_found', detail: '222' });
    expect(msg).toContain('222');
    expect(msg).toContain('/start');
  });

  it('bot_recipient: explains the user-vs-bot distinction', () => {
    const msg = formatValidateError({ ok: false, reason: 'bot_recipient', detail: '333' });
    expect(msg).toMatch(/bot/i);
    expect(msg).toMatch(/user/i);
    expect(msg).toContain('333');
  });

  it('network_error: includes the underlying detail', () => {
    const msg = formatValidateError({
      ok: false,
      reason: 'network_error',
      detail: 'ENOTFOUND api.telegram.org',
    });
    expect(msg).toContain('ENOTFOUND');
  });

  it('rate_limited: mentions retry', () => {
    const msg = formatValidateError({
      ok: false,
      reason: 'rate_limited',
      detail: 'Too Many Requests: retry after 5',
    });
    expect(msg).toMatch(/retry/i);
  });
});

// ---------------------------------------------------------------------------
// sendPhoto + sendDocument single-retry on transport errors
//
// Mirrors the post() retry behavior at TelegramAPI.post (~line 614). FormData
// with a Blob body is single-use — the test exercises the per-attempt rebuild
// path by failing the first fetch with a transport-class error and confirming
// the second attempt succeeds with a freshly-built FormData payload.
// ---------------------------------------------------------------------------
describe('TelegramAPI.sendPhoto / sendDocument single-retry', () => {
  const originalFetch = globalThis.fetch;
  const tmpDir = require('os').tmpdir();
  const path = require('path');
  const fs = require('fs');
  let tmpFile: string;

  beforeEach(() => {
    tmpFile = path.join(tmpDir, `telegram-retry-test-${Date.now()}-${Math.random().toString(36).slice(2)}.bin`);
    fs.writeFileSync(tmpFile, Buffer.from([0xff, 0xd8, 0xff, 0xe0]));
  });

  afterEach(() => {
    try { fs.unlinkSync(tmpFile); } catch { /* ignore */ }
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('sendPhoto: retries once on transport error and succeeds', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn(async () => {
      callCount++;
      if (callCount === 1) {
        // Simulate undici keepalive socket reuse error class (transport-level,
        // body not yet written — same safety property as post() retry).
        const err = new TypeError('fetch failed');
        throw err;
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({ ok: true, result: { message_id: 42 } }),
      } as any;
    }) as any;

    const api = new TelegramAPI('123:TEST');
    const result = await api.sendPhoto('chat-1', tmpFile, 'caption');

    expect(callCount).toBe(2);
    expect(result.ok).toBe(true);
    expect(result.result.message_id).toBe(42);
  });

  it('sendPhoto: does NOT retry on Telegram API error (4xx-class semantics)', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn(async () => {
      callCount++;
      return {
        ok: false,
        status: 400,
        json: async () => ({ ok: false, description: 'Bad Request: photo dimensions invalid' }),
      } as any;
    }) as any;

    const api = new TelegramAPI('123:TEST');
    await expect(api.sendPhoto('chat-1', tmpFile)).rejects.toThrow(/Telegram API error/);
    expect(callCount).toBe(1);
  });

  it('sendPhoto: does NOT retry on timeout (write-may-have-completed semantics)', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn(async () => {
      callCount++;
      const err = new Error('aborted');
      err.name = 'TimeoutError';
      throw err;
    }) as any;

    const api = new TelegramAPI('123:TEST');
    await expect(api.sendPhoto('chat-1', tmpFile)).rejects.toThrow(/timed out after 60s/);
    expect(callCount).toBe(1);
  });

  it('sendDocument: retries once on transport error and succeeds', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn(async () => {
      callCount++;
      if (callCount === 1) {
        throw new TypeError('fetch failed');
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({ ok: true, result: { message_id: 99 } }),
      } as any;
    }) as any;

    const api = new TelegramAPI('123:TEST');
    const result = await api.sendDocument('chat-1', tmpFile, 'doc caption');

    expect(callCount).toBe(2);
    expect(result.ok).toBe(true);
    expect(result.result.message_id).toBe(99);
  });

  it('sendDocument: passes a fresh FormData on retry (not the consumed one)', async () => {
    const bodiesSeen: any[] = [];
    let callCount = 0;
    globalThis.fetch = vi.fn(async (_url: any, init: any) => {
      callCount++;
      bodiesSeen.push(init?.body);
      if (callCount === 1) throw new TypeError('fetch failed');
      return {
        ok: true,
        status: 200,
        json: async () => ({ ok: true, result: {} }),
      } as any;
    }) as any;

    const api = new TelegramAPI('123:TEST');
    await api.sendDocument('chat-1', tmpFile);

    expect(bodiesSeen.length).toBe(2);
    // Each attempt gets a distinct FormData instance — re-using the consumed
    // stream would silently fail (or hang) on undici. Identity check is the
    // load-bearing assertion here.
    expect(bodiesSeen[0]).not.toBe(bodiesSeen[1]);
  });

  it('sendPhoto: throws when retry also fails (no infinite loop)', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn(async () => {
      callCount++;
      throw new TypeError('fetch failed');
    }) as any;

    const api = new TelegramAPI('123:TEST');
    await expect(api.sendPhoto('chat-1', tmpFile)).rejects.toThrow(/request failed/);
    expect(callCount).toBe(2);
  });
});
