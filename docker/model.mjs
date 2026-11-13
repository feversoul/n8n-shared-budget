// Stand-in for a model API, in the OpenAI chat completions shape:
//   POST /v1/chat/completions  { model, messages, max_tokens }
//   -> { choices: [{ message: { content } }], usage: { prompt_tokens, completion_tokens, total_tokens } }
// It makes no model call. It answers with fixed text and counts every request
// and token it serves, so a test can compare "what the provider billed" with
// what the workflow's ledger says.
//
//   GET  /stats?match=<text>   requests whose prompt contains <text>
//   POST /control              { match, latency_ms, completion_tokens, fail }   a rule for prompts containing `match`
//                              fail: "http500" answers 500 (not billed)
//                              fail: "hang" answers after hang_ms (default 60 s), far past the caller's time-out,
//                                    and counts the tokens: the provider did the work, the caller never saw it
//   POST /reset                forget rules and requests

import { createServer } from 'node:http';

let rules = [];
let requests = [];

const read = (request) =>
  new Promise((resolve) => {
    let text = '';
    request.on('data', (chunk) => (text += chunk));
    request.on('end', () => {
      try {
        resolve(JSON.parse(text || '{}'));
      } catch {
        resolve(null);
      }
    });
  });
const send = (response, status, body) => {
  response.writeHead(status, { 'content-type': 'application/json' });
  response.end(JSON.stringify(body));
};
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

createServer(async (request, response) => {
  const url = new URL(request.url, 'http://model');

  if (request.method === 'GET' && url.pathname === '/health') return send(response, 200, { ok: true });

  if (request.method === 'POST' && url.pathname === '/reset') {
    rules = [];
    requests = [];
    return send(response, 200, { ok: true });
  }

  if (request.method === 'POST' && url.pathname === '/control') {
    const rule = await read(request);
    rules = rules.filter((existing) => existing.match !== rule.match);
    rules.unshift(rule);
    return send(response, 200, { ok: true });
  }

  if (request.method === 'GET' && url.pathname === '/stats') {
    const match = url.searchParams.get('match') ?? '';
    const mine = requests.filter((entry) => entry.prompt.includes(match));
    const billed = mine.filter((entry) => entry.billed);
    return send(response, 200, {
      requests: mine.length,
      billed: billed.length,
      prompt_tokens: billed.reduce((sum, entry) => sum + entry.prompt_tokens, 0),
      completion_tokens: billed.reduce((sum, entry) => sum + entry.completion_tokens, 0),
      // Most requests being served at the same moment.
      max_in_flight: mine.reduce((max, entry) => Math.max(max, entry.in_flight), 0),
      calls: mine.map(({ prompt, billed: wasBilled, prompt_tokens, completion_tokens }) => ({ prompt, billed: wasBilled, prompt_tokens, completion_tokens })),
    });
  }

  if (request.method === 'POST' && url.pathname === '/v1/chat/completions') {
    if (!/^Bearer \S+/.test(request.headers.authorization ?? '')) {
      return send(response, 401, { error: { message: 'Missing bearer token', type: 'invalid_request_error' } });
    }
    const body = await read(request);
    if (!body || !Array.isArray(body.messages) || !body.model) {
      return send(response, 400, { error: { message: 'model and messages are required', type: 'invalid_request_error' } });
    }
    const prompt = body.messages.map((message) => String(message.content ?? '')).join('\n');
    const rule = rules.find((candidate) => prompt.includes(candidate.match ?? '')) ?? {};
    const limit = Number(body.max_completion_tokens ?? body.max_tokens ?? 4096);
    const entry = {
      prompt,
      billed: false,
      prompt_tokens: Math.ceil(prompt.length / 4),
      completion_tokens: Math.min(limit, Number(rule.completion_tokens ?? 40)),
      in_flight: 0,
    };
    requests.push(entry);
    entry.in_flight = requests.filter((other) => other.open).length + 1;
    entry.open = true;

    if (rule.fail === 'http500') {
      await sleep(rule.latency_ms ?? 0);
      entry.open = false;
      return send(response, 500, { error: { message: 'The server had an error', type: 'server_error' } });
    }
    if (rule.fail === 'hang') {
      entry.billed = true;
      await sleep(rule.hang_ms ?? 60_000);
    } else {
      await sleep(rule.latency_ms ?? 0);
      entry.billed = true;
    }
    entry.open = false;
    return send(response, 200, {
      id: `chatcmpl-${requests.length}`,
      object: 'chat.completion',
      created: Math.floor(Date.now() / 1000),
      model: body.model,
      choices: [{ index: 0, message: { role: 'assistant', content: `stand-in answer ${requests.length}` }, finish_reason: 'stop' }],
      usage: {
        prompt_tokens: entry.prompt_tokens,
        completion_tokens: entry.completion_tokens,
        total_tokens: entry.prompt_tokens + entry.completion_tokens,
      },
    });
  }

  send(response, 404, { error: 'not found' });
}).listen(8080, () => console.log('model stand-in on :8080'));
