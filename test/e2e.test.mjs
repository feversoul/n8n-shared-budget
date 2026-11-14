// End-to-end tests: the real workflows on a real n8n (npm run test:e2e),
// called over HTTP. The model is docker/model.mjs, a stand-in that answers in
// the OpenAI shape and counts every request and token it serves. "Billed" in
// these tests is always what the stand-in counted, never what a workflow says.
//
// Part 1 starts several runs at once against one small budget, with four ways
// of keeping it. The two ledgers of this repo are asserted. The two counters
// without a reservation are measured and printed, not asserted, because how
// far they overshoot depends on timing.
// Part 2 tests the two ledgers: the three over-budget paths, settle, release,
// unknown, retries of the same item, and a run that dies after reserving.

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { after, before, test } from 'node:test';
import { setTimeout as sleep } from 'node:timers/promises';

const BASE = process.env.N8N_URL ?? 'http://localhost:55684';
const KEY = process.env.WEBHOOK_KEY ?? 'test-budget-key';
const MODEL = process.env.MODEL_URL ?? 'http://localhost:58086';
const BACKEND = process.env.BACKEND ?? 'n8n';
const COMPOSE = process.env.COMPOSE_ARGS ? JSON.parse(process.env.COMPOSE_ARGS) : null;
const RUN = Date.now().toString(36);

// The defaults in the Settings node: 2000 input tokens and 200 output tokens
// at 0.15 and 0.60 USD per million tokens. Amounts are micro-dollars.
const PRICE_IN = 0.15;
const PRICE_OUT = 0.6;
const RESERVE = Math.ceil(2000 * PRICE_IN + 200 * PRICE_OUT); // 420
const TTL_MS = Number(process.env.RESERVATION_TTL_SECONDS ?? 8) * 1000;
const LEDGERS = ['postgres', 'data-table'];

async function call(path, body, key = KEY) {
  const response = await fetch(`${BASE}/webhook/budget/${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(key ? { 'x-budget-key': key } : {}) },
    body: JSON.stringify(body),
  });
  const text = await response.text();
  let json = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* not JSON */
  }
  return { status: response.status, json, text };
}

const model = {
  rule: (rule) => fetch(`${MODEL}/control`, { method: 'POST', body: JSON.stringify(rule) }),
  // What the stand-in served for prompts that contain `match`, priced like the workflows price it.
  async stats(match) {
    const stats = await (await fetch(`${MODEL}/stats?match=${encodeURIComponent(match)}`)).json();
    const cost = stats.calls.filter((entry) => entry.billed).reduce((sum, entry) => sum + Math.ceil(entry.prompt_tokens * PRICE_IN + entry.completion_tokens * PRICE_OUT), 0);
    return { ...stats, cost };
  },
};

const times = (n, make) => Promise.all(Array.from({ length: n }, (_, index) => make(index)));
const items = (marker, n, from = 0) => Array.from({ length: n }, (_, index) => ({ id: `${marker}-${from + index}`, text: `${marker} item ${from + index}` }));
const usd = (micro) => micro / 1e6;

async function until(check, timeoutMs = 30_000, stepMs = 500) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const value = await check();
    if (value) return value;
    if (Date.now() > deadline) throw new Error('timed out waiting');
    await sleep(stepMs);
  }
}

const ready = () =>
  until(async () => {
    try {
      // While n8n starts, every path answers 200 with a "starting up" page, so wait for JSON.
      const answers = [await call('status/data-table', { budget_key: 'ready' }), await call('status/postgres', { budget_key: 'ready' })];
      return answers.every((answer) => answer.status === 200 && answer.json !== null);
    } catch {
      return false;
    }
  }, 240_000);

const run = (ledger, body) => call(`run/${ledger}`, body);
const status = async (ledger, budget_key) => (await call(`status/${ledger}`, { budget_key })).json;

before(ready);

// --- Part 1: overlapping runs --------------------------------------------------

const ROUNDS = Number(process.env.ROUNDS ?? 5);
const RUNS = Number(process.env.RUNS ?? 8);
const ITEMS = 12;
const CAP = 3000; // micro-dollars. One call costs about 124, so about 24 calls fit.
const report = [];

// `runs` runs at the same moment, `perRun` items each, one model call per item.
async function overlap(method, runs = RUNS, perRun = ITEMS) {
  const rounds = [];
  for (let round = 0; round < ROUNDS; round++) {
    const marker = `${RUN}-${method}-${runs}x-${round}`;
    await model.rule({ match: marker, completion_tokens: 200, latency_ms: 150 });
    let answers;
    let ledger = null;
    if (LEDGERS.includes(method)) {
      answers = await times(runs, (index) => run(method, { budget: marker, cap_usd: usd(CAP), on_over_budget: 'skip', items: items(`${marker}-r${index}`, perRun) }));
      const key = answers[0].json?.results?.[0]?.budget_key;
      ledger = await status(method, key);
    } else {
      await call('lab/open', { budget: marker });
      answers = await times(runs, (index) => call(`lab/${method}`, { budget: marker, cap_usd: usd(CAP), items: items(`${marker}-r${index}`, perRun) }));
      ledger = { held: (await call('lab/spent', { budget: marker })).json[method], open: 0 };
    }
    const billed = await model.stats(marker);
    rounds.push({
      errors: answers.filter((answer) => answer.status !== 200).length,
      called: answers.flatMap((answer) => answer.json?.results ?? []).filter((result) => result.ai).length,
      requests: billed.requests,
      billed: billed.cost,
      ledger: ledger.held,
      open: ledger.open,
    });
  }
  return rounds;
}

const line = (name, runs, rounds) => {
  const worst = rounds.reduce((a, b) => (b.billed > a.billed ? b : a));
  const over = rounds.filter((round) => round.billed > CAP).length;
  report.push(
    `${name.padEnd(20)} ${String(runs).padStart(2)} run(s)  over the cap in ${over}/${rounds.length} rounds  worst: billed ${worst.billed} of ${CAP} (${worst.requests} calls), ledger said ${worst.ledger}  calls per round: ${rounds.map((round) => round.requests).join(' ')}`,
  );
};

for (const method of ['dt-read-then-add', 'pg-check-then-add']) {
  test(`overlap, ${method}: one run alone stays under the cap`, async () => {
    // 40 items, more than the cap has room for.
    const rounds = await overlap(method, 1, 40);
    line(method, 1, rounds);
    for (const round of rounds) {
      assert.equal(round.errors, 0);
      assert.ok(round.requests < 40, 'the cap stopped the run');
      assert.ok(round.billed <= CAP, `billed ${round.billed}`);
      assert.equal(round.ledger, round.billed, 'alone, the counter matches the bill');
    }
  });

  test(`overlap, ${method}: ${RUNS} runs at once (measured, not asserted)`, async () => {
    const rounds = await overlap(method);
    line(method, RUNS, rounds);
    assert.equal(rounds.reduce((sum, round) => sum + round.errors, 0), 0);
  });
}

for (const ledger of LEDGERS) {
  test(`overlap, ${ledger} ledger: ${RUNS} runs at once never bill more than the cap`, async () => {
    const rounds = await overlap(ledger);
    line(`${ledger} ledger`, RUNS, rounds);
    for (const round of rounds) {
      assert.equal(round.errors, 0);
      assert.ok(round.billed <= CAP, `billed ${round.billed} of ${CAP}`);
      assert.equal(round.requests, round.called, 'every request was a reserved one');
      assert.equal(round.ledger, round.billed, 'after the runs the ledger holds exactly what was billed');
      assert.equal(round.open, 0, 'no reservation left open');
      // 21 fit one after the other. Runs that collide near the cap may leave a little unused.
      assert.ok(round.requests >= 10, `only ${round.requests} calls got through`);
    }
  });
}

after(() => {
  console.log(`\nOverlapping runs, n8n on ${BACKEND}: ${ITEMS} items per run (40 for one run alone), cap ${CAP} micro-dollars, ${ROUNDS} rounds`);
  for (const entry of report) console.log(`  ${entry}`);
});

// --- Part 2: the two ledgers ---------------------------------------------------

for (const ledger of LEDGERS) {
  const name = (title) => `${ledger} ledger: ${title}`;
  const marker = (tag) => `${RUN}-${ledger}-${tag}`;

  // Cap 700 with calls of about 124 that reserve 420 first: three calls fit
  // one after the other (248 + 420 <= 700, 372 + 420 > 700).
  for (const mode of ['skip', 'queue']) {
    test(name(`over budget, ${mode}`), async () => {
      const m = marker(mode);
      await model.rule({ match: m, completion_tokens: 200 });
      const { status: code, json } = await run(ledger, { budget: m, cap_usd: usd(700), on_over_budget: mode, items: items(m, 6) });
      assert.equal(code, 200);
      const results = json.results;
      assert.deepEqual(results.map((result) => result.ai), [true, true, true, false, false, false]);
      for (const result of results.slice(3)) {
        assert.equal(result.reason, 'over_budget');
        assert.equal(result.queued, mode === 'queue');
        assert.equal(result.category, 'needs_review', 'the item took the path without the model');
      }
      const billed = await model.stats(m);
      assert.equal(billed.requests, 3, 'the model was called for the three that fitted only');
      const state = await status(ledger, results[0].budget_key);
      // Settled to the real cost: three calls reserved 1260 and cost about 372.
      assert.equal(state.held, billed.cost);
      assert.equal(state.spent, billed.cost);
      assert.equal(results.reduce((sum, result) => sum + result.cost_micro_usd, 0), billed.cost);
      assert.ok(billed.cost < 3 * RESERVE);
      assert.equal(state.open, 0);
      assert.deepEqual([...state.queued].sort(), mode === 'queue' ? results.slice(3).map((result) => result.id).sort() : []);
    });
  }

  test(name('over budget, wait: the item goes through once another call has settled'), async () => {
    const m = marker('wait');
    await model.rule({ match: `${m}-slow`, completion_tokens: 200, latency_ms: 2000 });
    await model.rule({ match: `${m}-late`, completion_tokens: 200 });
    // Cap 700: the slow call holds 420, so a second 420 does not fit until the
    // first is settled to about 124.
    const slow = run(ledger, { budget: m, cap_usd: usd(700), on_over_budget: 'skip', items: items(`${m}-slow`, 1) });
    await sleep(500);
    const started = Date.now();
    const late = await run(ledger, { budget: m, cap_usd: usd(700), on_over_budget: 'wait', items: items(`${m}-late`, 1) });
    const waited = Date.now() - started;
    assert.equal((await slow).json.results[0].ai, true);
    assert.equal(late.json.results[0].ai, true, 'the waiting item got its call');
    assert.ok(waited >= 1000, `waited ${waited} ms`);
    const billed = await model.stats(m);
    assert.equal(billed.requests, 2);
    assert.equal(billed.max_in_flight, 1, 'the two calls did not overlap');
    const state = await status(ledger, late.json.results[0].budget_key);
    assert.equal(state.held, billed.cost);
    assert.equal(state.open, 0);
  });

  test(name('over budget, wait: gives up after MAX_WAITS and queues the item'), async () => {
    const m = marker('wait-out');
    // A cap below one reservation never has room.
    const started = Date.now();
    const { json } = await run(ledger, { budget: m, cap_usd: usd(RESERVE - 20), on_over_budget: 'wait', items: items(m, 1) });
    const waited = Date.now() - started;
    assert.equal(json.results[0].ai, false);
    assert.equal(json.results[0].queued, true);
    assert.ok(waited >= 5500, `waited ${waited} ms`); // 6 waits of 1 s in the test stack
    assert.equal((await model.stats(m)).requests, 0);
    const state = await status(ledger, json.results[0].budget_key);
    assert.equal(state.held, 0);
    assert.deepEqual(state.queued, [`${m}-0`]);
  });

  test(name('the API answers with an error: the reservation is released and the item can be retried'), async () => {
    const m = marker('http500');
    await model.rule({ match: m, fail: 'http500' });
    const body = { budget: m, cap_usd: usd(1000), on_over_budget: 'skip', items: items(m, 2) };
    const first = (await run(ledger, body)).json.results;
    assert.deepEqual(first.map((result) => result.reason), ['call_failed', 'call_failed']);
    let state = await status(ledger, first[0].budget_key);
    assert.equal(state.held, 0, 'nothing stays reserved');
    assert.deepEqual(state.items, {});
    assert.equal((await model.stats(m)).requests, 2, 'one request per item, no hidden retries');

    await model.rule({ match: m, completion_tokens: 40 });
    const second = (await run(ledger, body)).json.results;
    assert.deepEqual(second.map((result) => result.ai), [true, true]);
    const billed = await model.stats(m);
    state = await status(ledger, first[0].budget_key);
    assert.equal(state.held, billed.cost);
    assert.equal(billed.billed, 2);
  });

  test(name('no answer (time-out): the reservation stays, the item is not called again, the sweep leaves it'), async () => {
    const m = marker('hang');
    // The stand-in answers after 5 s, the workflow gives up after 3 s. The
    // stand-in counts the tokens: the provider did the work.
    await model.rule({ match: m, fail: 'hang', hang_ms: 5000, completion_tokens: 200 });
    const body = { budget: m, cap_usd: usd(1000), on_over_budget: 'skip', items: items(m, 1) };
    const first = (await run(ledger, body)).json.results[0];
    assert.equal(first.ai, false);
    assert.equal(first.reason, 'unknown');
    let state = await status(ledger, first.budget_key);
    assert.equal(state.held, RESERVE, 'the worst case stays reserved');
    assert.equal(state.unknown, 1);

    const again = (await run(ledger, body)).json.results[0];
    assert.equal(again.reason, 'duplicate', 'a retry of the run does not call the model a second time');
    const billed = await model.stats(m);
    assert.equal(billed.requests, 1);
    assert.ok(billed.cost > 0 && billed.cost <= RESERVE, 'what the provider billed is covered by what stays reserved');

    // Past the reservation's lifetime, with the sweep running every 2 s.
    await sleep(TTL_MS + 3000);
    state = await status(ledger, first.budget_key);
    assert.equal(state.held, RESERVE);
    assert.equal(state.unknown, 1);
  });

  test(name('the same item in 10 runs at once is called once'), async () => {
    const m = marker('same');
    await model.rule({ match: m, latency_ms: 400 });
    const body = { budget: m, cap_usd: usd(20 * RESERVE), on_over_budget: 'skip', items: items(m, 1) };
    const answers = await times(10, () => run(ledger, body));
    const results = answers.map((answer) => answer.json.results[0]);
    assert.equal(results.filter((result) => result.ai).length, 1);
    assert.equal(results.filter((result) => result.reason === 'duplicate').length, 9);
    // And once more after it is done.
    assert.equal((await run(ledger, body)).json.results[0].reason, 'duplicate');
    const billed = await model.stats(m);
    assert.equal(billed.requests, 1);
    const state = await status(ledger, results[0].budget_key);
    assert.equal(state.held, billed.cost);
    assert.equal(state.settled, 1);
    assert.equal(state.open, 0);
  });
}

test('no key: 403, and the model is not called', async () => {
  const m = `${RUN}-nokey`;
  for (const ledger of LEDGERS) {
    const answer = await call(`run/${ledger}`, { budget: m, cap_usd: 1, items: items(m, 1) }, null);
    assert.equal(answer.status, 403);
  }
  assert.equal((await model.stats(m)).requests, 0);
});

// Last, because it stops n8n: a run dies between "reserve" and "settle".
test('n8n is killed while calls are in flight: the dead reservations are given back after their lifetime', { skip: !COMPOSE && 'needs COMPOSE_ARGS (set by scripts/e2e.mjs)' }, async () => {
  const docker = (...args) => spawnSync('docker', [...COMPOSE, ...args], { stdio: 'ignore' }).status;
  const m = `${RUN}-crash`;
  await model.rule({ match: m, latency_ms: 2500, completion_tokens: 200 });
  const body = (ledger) => ({ budget: `${m}-${ledger}`, cap_usd: usd(RESERVE + 50), on_over_budget: 'skip', items: items(`${m}-${ledger}`, 1) });
  const dying = LEDGERS.map((ledger) => run(ledger, body(ledger)).catch(() => null));
  await until(async () => (await model.stats(m)).requests === 2, 10_000, 100);
  assert.equal(docker('kill', 'n8n'), 0);
  await Promise.all(dying);
  await sleep(3000); // the stand-in finishes both calls: the provider did the work
  const billed = await model.stats(m);
  assert.equal(billed.billed, 2);

  assert.equal(docker('start', 'n8n'), 0);
  await ready();
  const day = new Date().toISOString().slice(0, 10);
  for (const ledger of LEDGERS) {
    // The budget has room for one reservation only, so the item fits again
    // only once the dead reservation is gone.
    const key = `${m}-${ledger}:${day}`;
    await until(async () => (await status(ledger, key)).open === 0, TTL_MS + 30_000);
    const state = await status(ledger, key);
    assert.equal(state.held, 0, `${ledger}: the dead reservation was given back`);
    const again = (await run(ledger, body(ledger))).json.results[0];
    assert.equal(again.ai, true, `${ledger}: the item can run again`);
  }
  // What this costs: the provider billed the two calls that died, and the
  // ledger no longer knows about them.
  console.log(`\nCrash: the stand-in billed ${billed.cost} micro-dollars for the 2 calls that died; both ledgers gave the reservations back and show 0 for them.`);
});
