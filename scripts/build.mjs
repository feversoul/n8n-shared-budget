// Builds workflows/*.json.
//
// The workflows are plain n8n exports: import them and they run, no build
// needed. This script exists so the repeated parts are written once (the two
// ledgers share the model call, the over-budget path and the answers). After
// changing it, run:  node scripts/build.mjs

import { createHash } from 'node:crypto';
import { mkdirSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

export const IDS = {
  setupTables: 'sharedBudgetSetDt',
  setupPostgres: 'sharedBudgetSetPg',
  postgres: 'sharedBudgetPg001',
  tables: 'sharedBudgetDt001',
  run: 'sharedBudgetRun01',
  lab: 'sharedBudgetLab01',
};

// Credentials are matched by name on import.
const WEBHOOK_KEY = { httpHeaderAuth: { id: 'sharedBudgetKey', name: 'Shared Budget Key' } };
const MODEL_KEY = { openAiApi: { id: 'sharedBudgetModel', name: 'Shared Budget Model' } };
const POSTGRES = { postgres: { id: 'sharedBudgetPg', name: 'Shared Budget Postgres' } };

// Data tables.
const LEDGER = 'budget_ledger';
const LEDGER_COLUMNS = [
  ['budget_key', 'string'], // budget name + window, for example ai:2026-01-31
  ['item_key', 'string'],
  ['owner', 'string'], // token of the execution that holds the reservation
  ['amount', 'number'], // micro-dollars that count against the cap: the reservation, later the real cost
  ['reserved', 'number'], // what was reserved before the call
  ['status', 'string'], // reserved | settled | unknown
  ['expires_at', 'date'],
];
const QUEUE = 'budget_queue';
const QUEUE_COLUMNS = [
  ['budget_key', 'string'],
  ['item_key', 'string'],
  ['prompt', 'string'],
];
const LAB = 'budget_lab';
const LAB_COLUMNS = [
  ['budget_key', 'string'],
  ['spent', 'number'],
];

const uuid = (seed) => {
  const hex = createHash('sha1').update(seed).digest('hex');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-4${hex.slice(13, 16)}-8${hex.slice(17, 20)}-${hex.slice(20, 32)}`;
};

const tableRef = (name) => ({ __rl: true, mode: 'name', value: name });
const conditions = (list) => ({
  matchType: 'allConditions',
  filters: { conditions: list.map(([keyName, keyValue, condition = 'eq']) => ({ keyName, condition, keyValue })) },
});
const mapped = (columns, values) => ({
  mappingMode: 'defineBelow',
  value: values,
  matchingColumns: [],
  schema: columns
    .filter(([column]) => column in values)
    .map(([column, type]) => ({
      id: column,
      displayName: column,
      required: false,
      defaultMatch: false,
      display: true,
      type: type === 'date' ? 'dateTime' : type,
      canBeUsedToMatch: true,
    })),
  attemptToConvertTypes: false,
  convertFieldsToString: false,
});

function workflow(id, name, build) {
  const nodes = [];
  const connections = {};
  const api = {
    node(nodeName, type, typeVersion, position, parameters, extra = {}) {
      nodes.push({ parameters, id: uuid(`${id}/${nodeName}`), name: nodeName, type: `n8n-nodes-base.${type}`, typeVersion, position, ...extra });
      return nodeName;
    },
    note(nodeName, lines, position, width, height, color = 4) {
      return api.node(nodeName, 'stickyNote', 1, position, { content: lines.join('\n'), width, height, color });
    },
    link(from, to, output = 0) {
      connections[from] ??= { main: [] };
      while (connections[from].main.length <= output) connections[from].main.push([]);
      connections[from].main[output].push({ node: to, type: 'main', index: 0 });
    },
    // a -> b -> c ...
    chain(...names) {
      for (let i = 1; i < names.length; i++) api.link(names[i - 1], names[i]);
    },
    hook(nodeName, path, position) {
      return api.node(
        nodeName,
        'webhook',
        2,
        position,
        { httpMethod: 'POST', path: `budget/${path}`, authentication: 'headerAuth', responseMode: 'responseNode', options: {} },
        { webhookId: uuid(`${id}/webhook/${path}`), credentials: WEBHOOK_KEY },
      );
    },
    code(nodeName, position, lines) {
      return api.node(nodeName, 'code', 2, position, { jsCode: lines.join('\n') });
    },
    // An IF node on one JavaScript condition. Output 0 is true, 1 is false.
    when(nodeName, position, expression) {
      return api.node(nodeName, 'if', 2.2, position, {
        conditions: {
          options: { caseSensitive: true, leftValue: '', typeValidation: 'loose', version: 2 },
          conditions: [
            {
              id: uuid(`${id}/${nodeName}/condition`),
              leftValue: `={{ ${expression} }}`,
              rightValue: '',
              operator: { type: 'boolean', operation: 'true', singleValue: true },
            },
          ],
          combinator: 'and',
        },
        options: { looseTypeValidation: true },
      });
    },
    answer(nodeName, position, code, body) {
      return api.node(nodeName, 'respondToWebhook', 1.1, position, {
        respondWith: 'json',
        responseBody: `={{ ${body} }}`,
        options: { responseCode: code },
      });
    },
    // Data Table row operation. It always passes one item on (an empty one
    // when nothing came back), so the next node can tell the cases apart.
    rows(nodeName, position, table, operation, parameters, extra = {}) {
      return api.node(
        nodeName,
        'dataTable',
        1.1,
        position,
        { resource: 'row', operation, dataTableId: tableRef(table), ...parameters },
        { alwaysOutputData: true, ...extra },
      );
    },
    sql(nodeName, position, query, replacement, extra = {}) {
      return api.node(
        nodeName,
        'postgres',
        2.6,
        position,
        { operation: 'executeQuery', query: query.join('\n'), options: replacement ? { queryReplacement: `={{ ${replacement} }}` } : {} },
        { alwaysOutputData: true, credentials: POSTGRES, ...extra },
      );
    },
    createTable(nodeName, position, table, columns) {
      return api.node(nodeName, 'dataTable', 1.1, position, {
        resource: 'table',
        operation: 'create',
        tableName: table,
        columns: { column: columns.map(([name, type]) => ({ name, type })) },
        options: { createIfNotExists: true },
      });
    },
    called(nodeName, position) {
      return api.node(nodeName, 'executeWorkflowTrigger', 1.1, position, { inputSource: 'passthrough' });
    },
    sub(nodeName, position, workflowId) {
      return api.node(nodeName, 'executeWorkflow', 1.2, position, {
        workflowId: { __rl: true, mode: 'id', value: workflowId },
        workflowInputs: { mappingMode: 'defineBelow', value: {}, matchingColumns: [], schema: [], attemptToConvertTypes: false, convertFieldsToString: true },
        mode: 'each',
        options: {},
      });
    },
  };
  build(api);
  return { name, nodes, pinData: {}, connections, active: false, settings: { executionOrder: 'v1' }, id };
}

// --- shared pieces -------------------------------------------------------------

// The model and its price. Used by the two ledgers and by the lab.
const MODEL_SETTINGS = [
  "const MODEL = 'gpt-4o-mini';",
  "const MODEL_URL = 'https://api.openai.com/v1/chat/completions'; // any OpenAI-compatible chat completions address",
  'const PRICE_IN_PER_MTOK = 0.15; // USD per million input tokens',
  'const PRICE_OUT_PER_MTOK = 0.6; // USD per million output tokens',
  'const RESERVE_INPUT_TOKENS = 2000; // input tokens to reserve per call. A longer prompt reserves more (1 token per 2 characters).',
  'const MAX_OUTPUT_TOKENS = 200; // sent as max_tokens, so the output cannot cost more than this',
  'const CALL_TIMEOUT_SECONDS = 60;',
];
// Worst-case cost of one call, in micro-dollars. A price per million tokens
// is the same number as micro-dollars per token.
const RESERVE = 'Math.ceil(Math.max(RESERVE_INPUT_TOKENS, Math.ceil(prompt.length / 2)) * PRICE_IN_PER_MTOK + MAX_OUTPUT_TOKENS * PRICE_OUT_PER_MTOK)';
const MODEL_FIELDS = [
  '      model: MODEL,',
  '      model_url: MODEL_URL,',
  '      price_in: PRICE_IN_PER_MTOK,',
  '      price_out: PRICE_OUT_PER_MTOK,',
  '      max_output_tokens: MAX_OUTPUT_TOKENS,',
  '      call_timeout_ms: CALL_TIMEOUT_SECONDS * 1000,',
];

const SETTINGS = [
  '// Settings. Money is counted in micro-dollars (1 USD = 1,000,000), so every sum is a whole number.',
  "const BUDGET = 'ai'; // name of the shared budget. Every workflow that uses this name shares it.",
  "const WINDOW = 'day'; // 'day' | 'month' | 'none': when the budget starts again (UTC)",
  'const CAP_USD = 5; // the cap for one window',
  "const ON_OVER_BUDGET = 'queue'; // 'wait' | 'skip' | 'queue'. What happens to an item when the budget has no room.",
  'const WAIT_SECONDS = 30; // wait: seconds between two tries',
  'const MAX_WAITS = 4; // wait: tries before the item goes to the queue after all',
  'const RESERVATION_TTL_SECONDS = 300; // a reservation older than this belongs to a dead run. Must be longer than the time-out.',
  ...MODEL_SETTINGS,
  '',
  '// One item comes in: { item_key, prompt } and optionally { budget, cap_usd, on_over_budget }.',
  'const input = $input.first().json;',
  "const item_key = String(input.item_key ?? '').trim();",
  "if (!item_key) throw new Error('item_key is required: a stable id of this model call, the same on every retry');",
  "const prompt = String(input.prompt ?? '');",
  'const now = $now.toUTC();',
  "const windowKey = WINDOW === 'day' ? now.toFormat('yyyy-LL-dd') : WINDOW === 'month' ? now.toFormat('yyyy-LL') : 'all';",
  "const hex = () => Math.floor(Math.random() * 0x100000000).toString(16).padStart(8, '0');",
  'return [',
  '  {',
  '    json: {',
  '      item_key,',
  '      prompt,',
  '      budget_key: `${input.budget ?? BUDGET}:${windowKey}`,',
  '      cap: Math.round(Number(input.cap_usd ?? CAP_USD) * 1e6),',
  `      reserve: ${RESERVE},`,
  '      on_over_budget: input.on_over_budget ?? ON_OVER_BUDGET,',
  '      wait_seconds: WAIT_SECONDS,',
  '      max_waits: MAX_WAITS,',
  '      ttl_seconds: RESERVATION_TTL_SECONDS,',
  '      // The run token. Settle and release only touch a reservation that still carries it.',
  '      owner: `${$execution.id}-${hex()}${hex()}`,',
  ...MODEL_FIELDS,
  '    },',
  '  },',
  '];',
];

const S = "$('Settings').first().json";

// POST to the model. `from` is the expression that holds prompt and model settings.
function callModel(w, nodeName, position, from) {
  return w.node(
    nodeName,
    'httpRequest',
    4.2,
    position,
    {
      method: 'POST',
      url: `={{ ${from}.model_url }}`,
      authentication: 'predefinedCredentialType',
      nodeCredentialType: 'openAiApi',
      sendBody: true,
      specifyBody: 'json',
      jsonBody: `={{ JSON.stringify({ model: ${from}.model, messages: [{ role: 'user', content: ${from}.prompt }], max_tokens: ${from}.max_output_tokens }) }}`,
      options: { response: { response: { fullResponse: true, neverError: true } }, timeout: `={{ ${from}.call_timeout_ms }}` },
    },
    // No Retry On Fail: every request must pass the budget first.
    { onError: 'continueRegularOutput', credentials: MODEL_KEY },
  );
}

// What the call cost, from the usage block of the answer.
const OUTCOME = (from) => [
  `const s = ${from};`,
  'const answer = $input.first().json;',
  'const status = Number(answer.statusCode) || 0;',
  'const usage = answer.body?.usage;',
  "let outcome = 'unknown'; // no answer at all (time-out, connection lost): the provider may have done the work",
  'let cost = 0;',
  'if (status >= 200 && status < 300 && usage) {',
  "  outcome = 'ok';",
  '  cost = Math.ceil((usage.prompt_tokens ?? 0) * s.price_in + (usage.completion_tokens ?? 0) * s.price_out);',
  '} else if (status >= 400) {',
  "  outcome = 'failed'; // the API answered with an error: nothing was generated",
  '}',
  'return [',
  '  {',
  '    json: {',
  '      outcome,',
  '      cost,',
  '      status,',
  "      text: outcome === 'ok' ? (answer.body.choices?.[0]?.message?.content ?? '') : null,",
  '      prompt_tokens: usage?.prompt_tokens ?? 0,',
  '      completion_tokens: usage?.completion_tokens ?? 0,',
  '    },',
  '  },',
  '];',
];

// The item that goes back to the calling workflow.
function result(w, nodeName, position, fields) {
  return w.code(nodeName, position, [
    `const s = ${S};`,
    'return [',
    '  {',
    '    json: {',
    '      item_key: s.item_key,',
    '      budget_key: s.budget_key,',
    ...fields.map((line) => `      ${line},`),
    '    },',
    '  },',
    '];',
  ]);
}

// Everything after the reservation, the same for both ledgers. The ledger
// supplies its own nodes for the writes and gets the wiring.
function afterReservation(w, x, y, nodes) {
  callModel(w, 'Call the model', [x, y], S);
  w.code('What it cost', [x + 220, y], OUTCOME(S));
  w.when('Answered?', [x + 440, y], "$json.outcome === 'ok'");
  w.when('Refused by the API?', [x + 660, y + 200], "$json.outcome === 'failed'");
  const out = "$('What it cost').first().json";
  result(w, 'Done', [x + 1100, y - 100], ['ai: true', 'reason: null', 'queued: false', `text: ${out}.text`, `cost_micro_usd: ${out}.cost`, `prompt_tokens: ${out}.prompt_tokens`, `completion_tokens: ${out}.completion_tokens`]);
  result(w, 'Call failed', [x + 1100, y + 100], ['ai: false', "reason: 'call_failed'", 'queued: false', 'text: null', 'cost_micro_usd: 0', `status: ${out}.status`]);
  result(w, 'Unknown: kept reserved', [x + 1100, y + 300], ['ai: false', "reason: 'unknown'", 'queued: false', 'text: null', 'cost_micro_usd: 0']);
  w.chain('Call the model', 'What it cost', 'Answered?');
  w.link('Answered?', nodes.settle, 0);
  w.link('Answered?', 'Refused by the API?', 1);
  w.link('Refused by the API?', nodes.release, 0);
  w.link('Refused by the API?', nodes.unknown, 1);
  w.link(nodes.settle, 'Done');
  w.link(nodes.release, 'Call failed');
  w.link(nodes.unknown, 'Unknown: kept reserved');

  // No room.
  const oy = y + 560;
  w.when('Wait for room?', [x, oy], `${S}.on_over_budget === 'wait' && $runIndex < ${S}.max_waits`);
  w.node('Wait', 'wait', 1.1, [x + 220, oy - 100], { amount: `={{ ${S}.wait_seconds }}`, unit: 'seconds' }, { webhookId: uuid(`${nodes.reserve}/wait`) });
  w.when('Skip the AI step?', [x + 220, oy + 100], `${S}.on_over_budget === 'skip'`);
  result(w, 'Skipped', [x + 1100, oy], ['ai: false', "reason: 'over_budget'", 'queued: false', 'text: null', 'cost_micro_usd: 0']);
  result(w, 'Queued', [x + 1100, oy + 200], ['ai: false', "reason: 'over_budget'", 'queued: true', 'text: null', 'cost_micro_usd: 0']);
  w.link('Wait for room?', 'Wait', 0);
  w.link('Wait', nodes.reserve);
  w.link('Wait for room?', 'Skip the AI step?', 1);
  w.link('Skip the AI step?', 'Skipped', 0);
  w.link('Skip the AI step?', nodes.queue, 1);
  w.link(nodes.queue, 'Queued');

  result(w, 'Duplicate: not called again', [x + 1100, oy + 400], ['ai: false', "reason: 'duplicate'", 'queued: false', 'text: null', 'cost_micro_usd: 0']);
}

const OVER_NOTE = [
  '### When the budget has no room',
  '`ON_OVER_BUDGET` in **Settings**, or `on_over_budget` on the item:',
  '- `wait`: try again every `WAIT_SECONDS`, at most `MAX_WAITS` times, then queue. Room appears when other calls settle below what they reserved.',
  '- `skip`: answer `ai: false`. The calling workflow takes its path without the model.',
  '- `queue`: store the item for review or a later run and answer `ai: false, queued: true`.',
  '',
  '### After the call',
  '- Answer with usage: **settle**. The reservation becomes the real cost and the rest is free again.',
  '- Error answer from the API: **release**. The whole reservation is free again and the item may be tried again.',
  '- No answer (time-out): **unknown**. The provider may have done the work, so the reservation stays and the item is not called again.',
];

// --- 0. Setup ----------------------------------------------------------------

const setupTables = workflow(IDS.setupTables, 'Shared Budget · 0 Setup (Data Tables)', (w) => {
  w.note(
    'About',
    [
      '## Shared Budget · Setup (Data Tables)',
      `Run this once: open it and press **Execute workflow**. It creates the data tables \`${LEDGER}\` and \`${QUEUE}\` for the Data Table ledger and \`${LAB}\` for the lab. Running it again changes nothing.`,
      '',
      'The webhook does the same for scripted installs (`POST budget/setup/data-tables`). You can delete it.',
    ],
    [-40, -260],
    560,
    200,
  );
  w.node('Run once', 'manualTrigger', 1, [0, 0], {});
  w.hook('Or call', 'setup/data-tables', [0, 180]);
  w.createTable('Create ledger table', [240, 0], LEDGER, LEDGER_COLUMNS);
  w.createTable('Create queue table', [480, 0], QUEUE, QUEUE_COLUMNS);
  w.createTable('Create lab table', [720, 0], LAB, LAB_COLUMNS);
  w.answer('Done', [960, 0], 200, '{ ok: true }');
  w.link('Run once', 'Create ledger table');
  w.chain('Or call', 'Create ledger table', 'Create queue table', 'Create lab table', 'Done');
});

const setupPostgres = workflow(IDS.setupPostgres, 'Shared Budget · 0 Setup (Postgres)', (w) => {
  w.note(
    'About',
    [
      '## Shared Budget · Setup (Postgres)',
      'Run this once: open it and press **Execute workflow**. It creates `ai_budget`, `ai_reservations` and `ai_budget_queue` for the Postgres ledger and `ai_budget_lab` for the lab, in the database of the **Shared Budget Postgres** credential. Running it again changes nothing.',
    ],
    [-40, -220],
    560,
    160,
  );
  w.node('Run once', 'manualTrigger', 1, [0, 0], {});
  w.hook('Or call', 'setup/postgres', [0, 180]);
  w.sql('Create tables', [240, 0], [
    '-- One row per budget window. held = open reservations + money already spent.',
    'CREATE TABLE IF NOT EXISTS ai_budget (',
    '  budget_key text PRIMARY KEY,',
    '  cap        bigint NOT NULL,',
    '  held       bigint NOT NULL DEFAULT 0,',
    '  spent      bigint NOT NULL DEFAULT 0,',
    '  updated_at timestamptz NOT NULL DEFAULT now()',
    ');',
    '-- One row per model call. The primary key is what stops a retry from reserving twice.',
    'CREATE TABLE IF NOT EXISTS ai_reservations (',
    '  budget_key text NOT NULL,',
    '  item_key   text NOT NULL,',
    '  owner      text NOT NULL,',
    '  amount     bigint NOT NULL,',
    '  actual     bigint,',
    "  status     text NOT NULL DEFAULT 'reserved',",
    '  expires_at timestamptz NOT NULL,',
    '  created_at timestamptz NOT NULL DEFAULT now(),',
    '  PRIMARY KEY (budget_key, item_key)',
    ');',
    'CREATE TABLE IF NOT EXISTS ai_budget_queue (',
    '  budget_key text NOT NULL,',
    '  item_key   text NOT NULL,',
    '  prompt     text NOT NULL,',
    '  queued_at  timestamptz NOT NULL DEFAULT now(),',
    '  PRIMARY KEY (budget_key, item_key)',
    ');',
    'CREATE TABLE IF NOT EXISTS ai_budget_lab (',
    '  budget_key text PRIMARY KEY,',
    '  spent      bigint NOT NULL DEFAULT 0',
    ');',
  ]);
  w.answer('Done', [480, 0], 200, '{ ok: true }');
  w.link('Run once', 'Create tables');
  w.chain('Or call', 'Create tables', 'Done');
});

// --- 1. Postgres ledger --------------------------------------------------------

const ledgerPostgres = workflow(IDS.postgres, 'Shared Budget · 1 Budgeted model call (Postgres)', (w) => {
  w.note(
    'About',
    [
      '## Shared Budget · Budgeted model call, ledger in Postgres',
      'A sub-workflow. Call it with **Execute Sub-workflow** once per item: `{ item_key, prompt }`. It answers `{ ai: true, text, cost_micro_usd }` or `{ ai: false, reason }`.',
      '',
      '**Reserve** is one statement. It adds the worst-case cost of the call to `held` only while `held` stays under the cap, and stores the item key in the same transaction. Overlapping runs cannot pass it together: the database lets them through one at a time.',
      '',
      'The same item key never gets a second reservation, so a retry of the run does not call the model twice.',
      '',
      'All settings are at the top of the **Settings** node.',
    ],
    [-40, -420],
    640,
    340,
  );
  w.note('Paths', OVER_NOTE, [1540, -460], 700, 340, 5);
  w.called('When called by another workflow', [0, 0]);
  w.code('Settings', [220, 0], SETTINGS);
  w.sql(
    'Reserve',
    [440, 0],
    [
      'WITH existing AS (',
      '  SELECT status FROM ai_reservations WHERE budget_key = $1::text AND item_key = $4::text',
      '), budget AS (',
      '  INSERT INTO ai_budget (budget_key, cap, held)',
      '  SELECT $1::text, $2::bigint, $3::bigint',
      '  WHERE $3::bigint <= $2::bigint AND NOT EXISTS (SELECT 1 FROM existing)',
      '  ON CONFLICT (budget_key) DO UPDATE',
      '    SET held = ai_budget.held + EXCLUDED.held, cap = EXCLUDED.cap, updated_at = now()',
      '    WHERE ai_budget.held + EXCLUDED.held <= EXCLUDED.cap',
      '  RETURNING held',
      '), claim AS (',
      '  INSERT INTO ai_reservations (budget_key, item_key, owner, amount, expires_at)',
      '  SELECT $1::text, $4::text, $5::text, $3::bigint, now() + make_interval(secs => $6::int) FROM budget',
      '  RETURNING item_key',
      ')',
      'SELECT (SELECT status FROM existing) AS existing, (SELECT held FROM budget) AS held, (SELECT count(*) FROM claim)::int AS reserved;',
    ],
    `[${S}.budget_key, ${S}.cap, ${S}.reserve, ${S}.item_key, ${S}.owner, ${S}.ttl_seconds]`,
    // Two runs with the same new item key at the same moment: the second
    // insert hits the primary key, the whole statement is undone (budget
    // included) and the item leaves through the error output.
    { onError: 'continueErrorOutput', alwaysOutputData: false },
  );
  w.when('Has a reservation already?', [660, 0], '$json.existing != null');
  w.when('Reserved?', [880, 0], '$json.held != null && $json.reserved === 1');
  w.when('Same item at the same moment?', [660, 300], '/duplicate key|unique constraint/i.test(JSON.stringify($json))');
  w.node('Ledger error', 'stopAndError', 1, [880, 400], { errorMessage: '={{ "Budget ledger failed: " + JSON.stringify($json) }}' });

  const key = `[${S}.budget_key, ${S}.item_key, ${S}.owner]`;
  w.sql(
    'Settle',
    [1760, -100],
    [
      'WITH mine AS (',
      "  UPDATE ai_reservations SET status = 'settled', actual = $4::bigint",
      "  WHERE budget_key = $1::text AND item_key = $2::text AND owner = $3::text AND status IN ('reserved', 'unknown')",
      '  RETURNING amount',
      ')',
      'UPDATE ai_budget',
      'SET held = held - COALESCE((SELECT amount FROM mine), 0) + $4::bigint, spent = spent + $4::bigint, updated_at = now()',
      'WHERE budget_key = $1::text',
      'RETURNING held, spent;',
    ],
    `[${S}.budget_key, ${S}.item_key, ${S}.owner, $json.cost]`,
  );
  w.sql(
    'Release',
    [1980, 100],
    [
      'WITH mine AS (',
      "  DELETE FROM ai_reservations WHERE budget_key = $1::text AND item_key = $2::text AND owner = $3::text AND status = 'reserved'",
      '  RETURNING amount',
      ')',
      'UPDATE ai_budget SET held = held - mine.amount, updated_at = now() FROM mine WHERE budget_key = $1::text',
      'RETURNING held;',
    ],
    key,
  );
  w.sql(
    'Mark unknown',
    [1980, 300],
    ["UPDATE ai_reservations SET status = 'unknown'", "WHERE budget_key = $1::text AND item_key = $2::text AND owner = $3::text AND status = 'reserved'", 'RETURNING status;'],
    key,
  );
  w.sql(
    'Queue the item',
    [1540, 760],
    ['INSERT INTO ai_budget_queue (budget_key, item_key, prompt) VALUES ($1::text, $2::text, $3::text)', 'ON CONFLICT (budget_key, item_key) DO NOTHING', 'RETURNING item_key;'],
    `[${S}.budget_key, ${S}.item_key, ${S}.prompt]`,
  );
  afterReservation(w, 1100, 0, { reserve: 'Reserve', settle: 'Settle', release: 'Release', unknown: 'Mark unknown', queue: 'Queue the item' });

  w.chain('When called by another workflow', 'Settings', 'Reserve', 'Has a reservation already?');
  w.link('Has a reservation already?', 'Duplicate: not called again', 0);
  w.link('Has a reservation already?', 'Reserved?', 1);
  w.link('Reserved?', 'Call the model', 0);
  w.link('Reserved?', 'Wait for room?', 1);
  w.link('Reserve', 'Same item at the same moment?', 1);
  w.link('Same item at the same moment?', 'Duplicate: not called again', 0);
  w.link('Same item at the same moment?', 'Ledger error', 1);

  // Dead runs.
  w.note(
    'Sweep',
    [
      '### Reservations of dead runs',
      'A run that died after **Reserve** never settles. Every minute this gives back reservations that are older than `RESERVATION_TTL_SECONDS`.',
      '',
      'If you would rather keep that money reserved until someone has checked the provider, switch this trigger off: a dead reservation then stays `reserved` for the rest of the window.',
    ],
    [-40, 880],
    640,
    200,
    5,
  );
  w.node('Every minute', 'scheduleTrigger', 1.2, [0, 1120], { rule: { interval: [{ field: 'minutes', minutesInterval: 1 }] } });
  w.sql('Release dead reservations', [220, 1120], [
    'WITH dead AS (',
    "  DELETE FROM ai_reservations WHERE status = 'reserved' AND expires_at < now()",
    '  RETURNING budget_key, amount',
    '), freed AS (',
    '  SELECT budget_key, sum(amount) AS amount, count(*) AS released FROM dead GROUP BY budget_key',
    ')',
    'UPDATE ai_budget b SET held = b.held - freed.amount, updated_at = now() FROM freed WHERE b.budget_key = freed.budget_key',
    'RETURNING b.budget_key, freed.released;',
  ]);
  w.link('Every minute', 'Release dead reservations');
});

// --- 2. Data Table ledger ------------------------------------------------------

const ledgerTables = workflow(IDS.tables, 'Shared Budget · 2 Budgeted model call (Data Tables)', (w) => {
  const mine = "$('Add my reservation').first().json";
  w.note(
    'About',
    [
      '## Shared Budget · Budgeted model call, ledger in Data Tables',
      'A sub-workflow with the same input and answers as the Postgres one, and no database of your own.',
      '',
      'A Data Table update cannot add to the old value, and "read the total, then write it back" loses updates when runs overlap. So there is no total row at all. **Every reservation is its own row**:',
      '1. Insert a row with the worst-case cost of the call.',
      '2. Read all rows of this budget and add them up.',
      '3. If the sum, my row included, is within the cap, the reservation holds. If not, delete my row.',
      '',
      'Of two overlapping runs, at least one reads after the other has inserted, so both can never slip under the cap together. Runs that arrive together and would all step back are ordered by row id: the earlier row stays and looks again, the later one goes.',
      '',
      'Settling changes the amount of the own row, releasing deletes it.',
    ],
    [-40, -480],
    640,
    400,
  );
  w.note('Paths', OVER_NOTE, [1540, -460], 700, 340, 5);
  w.called('When called by another workflow', [0, 0]);
  w.code('Settings', [220, 0], SETTINGS);
  w.rows('Add my reservation', [440, 0], LEDGER, 'insert', {
    columns: mapped(LEDGER_COLUMNS, {
      budget_key: `={{ ${S}.budget_key }}`,
      item_key: `={{ ${S}.item_key }}`,
      owner: `={{ ${S}.owner }}`,
      amount: `={{ ${S}.reserve }}`,
      reserved: `={{ ${S}.reserve }}`,
      status: 'reserved',
      expires_at: `={{ $now.plus({ seconds: ${S}.ttl_seconds }).toISO() }}`,
    }),
    options: {},
  });
  w.rows('Rows of this budget', [660, 0], LEDGER, 'get', { ...conditions([['budget_key', `={{ ${S}.budget_key }}`]]), returnAll: true });
  w.code('Add them up', [880, 0], [
    'const MAX_LOOKS = 6; // how often a run looks again before it gives the room up',
    `const s = ${S};`,
    `const mine = ${mine};`,
    'const rows = $input.all().map((item) => item.json).filter((row) => row.id !== undefined);',
    '// The same item with a lower row id was here first.',
    'const earlier = rows.find((row) => row.item_key === s.item_key && row.id < mine.id);',
    '// Every row counts, also rows of runs that are still deciding. Only this',
    '// sum may let a call through.',
    'const total = rows.reduce((sum, row) => sum + Number(row.amount ?? 0), 0);',
    '// Runs that arrive together would all see each other and all step back.',
    '// So the row id breaks the tie: if I fit together with the rows before',
    '// mine, the rows after mine have to go, and I look again in a moment.',
    'const ahead = rows.filter((row) => row.id <= mine.id).reduce((sum, row) => sum + Number(row.amount ?? 0), 0);',
    "const before = $('Short pause').isExecuted ? $('Short pause').first().json : null;",
    'const looks = before && before.my_id === mine.id ? before.looks + 1 : 1;',
    'const reserved = !earlier && total <= s.cap;',
    'return [{ json: { my_id: mine.id, duplicate: !!earlier, total, reserved, looks, look_again: !reserved && !earlier && ahead <= s.cap && looks < MAX_LOOKS } }];',
  ]);
  w.when('Only later rows in the way?', [1100, 200], '$json.look_again === true');
  w.node('Short pause', 'wait', 1.1, [880, 200], { amount: '={{ 0.1 + Math.random() * 0.3 }}', unit: 'seconds' }, { webhookId: uuid('tables/short-pause') });
  w.when('Reserved?', [1100, 0], '$json.reserved === true');
  w.rows('Remove my reservation', [880, 420], LEDGER, 'deleteRows', { ...conditions([['id', `={{ ${mine}.id }}`]]), options: {} });
  w.when('Has a reservation already?', [1100, 420], "$('Add them up').first().json.duplicate === true");

  const myRow = [
    ['id', `={{ ${mine}.id }}`],
    ['owner', `={{ ${S}.owner }}`],
  ];
  w.rows('Settle', [1760, -100], LEDGER, 'update', {
    ...conditions(myRow),
    columns: mapped(LEDGER_COLUMNS, { amount: '={{ $json.cost }}', status: 'settled' }),
    options: {},
  });
  w.rows('Release', [1980, 100], LEDGER, 'deleteRows', { ...conditions(myRow), options: {} });
  w.rows('Mark unknown', [1980, 300], LEDGER, 'update', { ...conditions(myRow), columns: mapped(LEDGER_COLUMNS, { status: 'unknown' }), options: {} });
  w.rows('Queue the item', [1540, 760], QUEUE, 'insert', {
    columns: mapped(QUEUE_COLUMNS, { budget_key: `={{ ${S}.budget_key }}`, item_key: `={{ ${S}.item_key }}`, prompt: `={{ ${S}.prompt }}` }),
    options: {},
  });
  afterReservation(w, 1320, 0, { reserve: 'Add my reservation', settle: 'Settle', release: 'Release', unknown: 'Mark unknown', queue: 'Queue the item' });

  w.chain('When called by another workflow', 'Settings', 'Add my reservation', 'Rows of this budget', 'Add them up', 'Reserved?');
  w.link('Reserved?', 'Call the model', 0);
  w.link('Reserved?', 'Only later rows in the way?', 1);
  w.link('Only later rows in the way?', 'Short pause', 0);
  w.link('Short pause', 'Rows of this budget');
  w.link('Only later rows in the way?', 'Remove my reservation', 1);
  w.link('Remove my reservation', 'Has a reservation already?');
  w.link('Has a reservation already?', 'Duplicate: not called again', 0);
  w.link('Has a reservation already?', 'Wait for room?', 1);

  w.note(
    'Sweep',
    [
      '### Reservations of dead runs',
      'A run that died after **Add my reservation** never settles. Every minute this deletes rows that are still `reserved` after `RESERVATION_TTL_SECONDS`.',
      '',
      'If you would rather keep that money reserved until someone has checked the provider, switch this trigger off.',
    ],
    [-40, 880],
    640,
    180,
    5,
  );
  w.node('Every minute', 'scheduleTrigger', 1.2, [0, 1120], { rule: { interval: [{ field: 'minutes', minutesInterval: 1 }] } });
  w.rows('Delete dead reservations', [220, 1120], LEDGER, 'deleteRows', {
    ...conditions([
      ['status', 'reserved'],
      ['expires_at', '={{ $now.toISO() }}', 'lt'],
    ]),
    options: {},
  });
  w.link('Every minute', 'Delete dead reservations');
});

// --- 3. Example run ------------------------------------------------------------

const exampleRun = workflow(IDS.run, 'Shared Budget · 3 Example run', (w) => {
  w.note(
    'About',
    [
      '## Shared Budget · Example run',
      'A stand-in for your own workflow: a run gets a list of items and makes one short model call per item, through the budgeted sub-workflow.',
      '',
      '`POST budget/run/postgres` or `budget/run/data-table` with `{ "items": [{ "id": "a1", "text": "..." }] }`. Optional: `budget`, `cap_usd`, `on_over_budget`.',
      '',
      '**Use the answer** is where your workflow goes on. `ai: false` means the item took the path without the model.',
      '',
      '`POST budget/status/postgres` or `budget/status/data-table` with `{ "budget_key": "ai:2026-01-31" }` shows what the ledger holds.',
    ],
    [-40, -420],
    640,
    340,
  );
  w.hook('Run on Postgres ledger', 'run/postgres', [0, -80]);
  w.hook('Run on Data Table ledger', 'run/data-table', [0, 100]);
  w.code('Items', [240, 0], [
    'const hook = $input.first().json;',
    'const body = hook.body ?? {};',
    '// One model call per item. item_key must be the same when the run is retried.',
    'return (body.items ?? []).map((item) => ({',
    '  json: {',
    '    item_key: String(item.id),',
    '    prompt: `Answer with one word, the category of this message: ${item.text}`,',
    '    budget: body.budget,',
    '    cap_usd: body.cap_usd,',
    '    on_over_budget: body.on_over_budget,',
    "    ledger: String(hook.webhookUrl).endsWith('/postgres') ? 'postgres' : 'data-table',",
    '  },',
    '}));',
  ]);
  w.when('Postgres ledger?', [460, 0], "$json.ledger === 'postgres'");
  w.sub('Budgeted call (Postgres)', [680, -80], IDS.postgres);
  w.sub('Budgeted call (Data Tables)', [680, 100], IDS.tables);
  w.code('Use the answer', [900, 0], [
    'return [',
    '  {',
    '    json: {',
    '      results: $input.all().map(({ json }) => ({',
    '        id: json.item_key,',
    '        ai: json.ai,',
    '        // The deterministic path for items that got no model call.',
    "        category: json.ai ? json.text : 'needs_review',",
    '        reason: json.reason,',
    '        queued: json.queued,',
    '        cost_micro_usd: json.cost_micro_usd,',
    '        budget_key: json.budget_key,',
    '      })),',
    '    },',
    '  },',
    '];',
  ]);
  w.answer('Answer', [1120, 0], 200, '$json');
  w.link('Run on Postgres ledger', 'Items');
  w.link('Run on Data Table ledger', 'Items');
  w.link('Items', 'Postgres ledger?');
  w.link('Postgres ledger?', 'Budgeted call (Postgres)', 0);
  w.link('Postgres ledger?', 'Budgeted call (Data Tables)', 1);
  w.link('Budgeted call (Postgres)', 'Use the answer');
  w.link('Budgeted call (Data Tables)', 'Use the answer');
  w.link('Use the answer', 'Answer');

  // Status.
  w.hook('Status of Postgres ledger', 'status/postgres', [0, 420]);
  w.sql(
    'Read ledger',
    [240, 420],
    [
      'SELECT',
      '  (SELECT held FROM ai_budget WHERE budget_key = $1::text) AS held,',
      '  (SELECT spent FROM ai_budget WHERE budget_key = $1::text) AS spent,',
      "  (SELECT COALESCE(json_agg(json_build_object('item_key', item_key, 'status', status, 'amount', amount, 'actual', actual)), '[]'::json) FROM ai_reservations WHERE budget_key = $1::text) AS reservations,",
      "  (SELECT COALESCE(json_agg(item_key), '[]'::json) FROM ai_budget_queue WHERE budget_key = $1::text) AS queued;",
    ],
    '[$json.body.budget_key]',
  );
  const summary = [
    'const count = (status) => reservations.filter((row) => row.status === status).length;',
    'return [',
    '  {',
    '    json: {',
    '      held,',
    '      spent,',
    "      open: count('reserved'),",
    "      unknown: count('unknown'),",
    "      settled: count('settled'),",
    '      items: Object.fromEntries(reservations.map((row) => [row.item_key, row.status])),',
    '      queued,',
    '    },',
    '  },',
    '];',
  ];
  w.code('Summary', [460, 420], [
    'const row = $input.first().json;',
    'const reservations = row.reservations ?? [];',
    'const held = Number(row.held ?? 0);',
    'const spent = Number(row.spent ?? 0);',
    'const queued = row.queued ?? [];',
    ...summary,
  ]);
  w.answer('Status', [680, 420], 200, '$json');
  w.chain('Status of Postgres ledger', 'Read ledger', 'Summary', 'Status');

  w.hook('Status of Data Table ledger', 'status/data-table', [0, 620]);
  w.rows('Read rows', [240, 620], LEDGER, 'get', { ...conditions([['budget_key', '={{ $json.body.budget_key }}']]), returnAll: true });
  w.rows(
    'Read queue',
    [240, 800],
    QUEUE,
    'get',
    { ...conditions([['budget_key', "={{ $('Status of Data Table ledger').first().json.body.budget_key }}"]]), returnAll: true },
    { executeOnce: true },
  );
  w.code('Summary of rows', [460, 620], [
    "const reservations = $('Read rows').all().map((item) => item.json).filter((row) => row.id !== undefined);",
    "const queued = $('Read queue').all().map((item) => item.json).filter((row) => row.id !== undefined).map((row) => row.item_key);",
    '// No total row: what is held is the sum of the rows.',
    'const held = reservations.reduce((sum, row) => sum + Number(row.amount ?? 0), 0);',
    "const spent = reservations.filter((row) => row.status === 'settled').reduce((sum, row) => sum + Number(row.amount ?? 0), 0);",
    ...summary,
  ]);
  w.answer('Status of rows', [680, 620], 200, '$json');
  w.chain('Status of Data Table ledger', 'Read rows', 'Read queue', 'Summary of rows', 'Status of rows');
});

// --- 9. Lab --------------------------------------------------------------------

// The counters people reach for first. Each run takes its items one by one:
// read what was spent, call the model if there is room, add the cost.
export const NAIVE = [
  ['dt-read-then-add', 'Data Table: read spent, check, call, read again, write spent + cost'],
  ['pg-check-then-add', 'Postgres: SELECT spent, check, call, UPDATE spent = spent + cost'],
];

const lab = workflow(IDS.lab, 'Shared Budget · 9 Lab', (w) => {
  w.note(
    'About',
    [
      '## Shared Budget · Lab',
      'Two counters without a reservation, for measuring. Not for production.',
      '',
      ...NAIVE.map(([path, title]) => `- \`budget/lab/${path}\`: ${title}`),
      '',
      'Same body as the example run. `budget/lab/open` creates the counter rows, `budget/lab/spent` reads them. The tests start several runs at once and compare what the model stand-in billed with the cap.',
    ],
    [-40, -420],
    700,
    300,
  );

  NAIVE.forEach(([method], index) => {
    const y = index * 520;
    const n = (name) => `${method} · ${name}`;
    const item = `$('${n('Loop')}').first().json`;
    w.hook(method, `lab/${method}`, [0, y]);
    w.code(n('Items'), [220, y], [
      ...MODEL_SETTINGS,
      'const body = $input.first().json.body ?? {};',
      'return (body.items ?? []).map((entry) => {',
      '  const prompt = `Answer with one word, the category of this message: ${entry.text}`;',
      '  return {',
      '    json: {',
      '      item_key: String(entry.id),',
      '      prompt,',
      '      budget_key: body.budget,',
      '      cap: Math.round(Number(body.cap_usd) * 1e6),',
      `      reserve: ${RESERVE},`,
      ...MODEL_FIELDS,
      '    },',
      '  };',
      '});',
    ]);
    w.node(n('Loop'), 'splitInBatches', 3, [440, y], { batchSize: 1, options: {} });
    if (method === 'dt-read-then-add') {
      w.rows(n('Read spent'), [660, y + 80], LAB, 'get', { ...conditions([['budget_key', '={{ $json.budget_key }}']]), returnAll: false, limit: 1 });
    } else {
      w.sql(n('Read spent'), [660, y + 80], ['SELECT spent FROM ai_budget_lab WHERE budget_key = $1::text;'], '[$json.budget_key]');
    }
    w.when(n('Room?'), [880, y + 80], `Number($json.spent ?? 0) + ${item}.reserve <= ${item}.cap`);
    callModel(w, n('Call the model'), [1100, y], item);
    w.code(n('What it cost'), [1320, y], OUTCOME(item));
    if (method === 'dt-read-then-add') {
      w.rows(n('Read spent again'), [1540, y], LAB, 'get', { ...conditions([['budget_key', `={{ ${item}.budget_key }}`]]), returnAll: false, limit: 1 });
      w.rows(n('Write spent'), [1760, y], LAB, 'update', {
        ...conditions([['budget_key', `={{ ${item}.budget_key }}`]]),
        columns: mapped(LAB_COLUMNS, { spent: `={{ Number($json.spent ?? 0) + $('${n('What it cost')}').first().json.cost }}` }),
        options: {},
      });
      w.chain(n('What it cost'), n('Read spent again'), n('Write spent'));
    } else {
      w.sql(n('Write spent'), [1760, y], ['UPDATE ai_budget_lab SET spent = spent + $2::bigint WHERE budget_key = $1::text', 'RETURNING spent;'], `[${item}.budget_key, $json.cost]`);
      w.link(n('What it cost'), n('Write spent'));
    }
    w.code(n('Called'), [1980, y], [`return [{ json: { id: ${item}.item_key, ai: true } }];`]);
    w.code(n('Over budget'), [1100, y + 200], [`return [{ json: { id: ${item}.item_key, ai: false } }];`]);
    w.code(n('Results'), [660, y - 120], ['return [{ json: { results: $input.all().map((entry) => entry.json) } }];']);
    w.answer(n('Answer'), [880, y - 120], 200, '$json');

    w.chain(method, n('Items'), n('Loop'));
    w.link(n('Loop'), n('Results'), 0);
    w.link(n('Results'), n('Answer'));
    w.link(n('Loop'), n('Read spent'), 1);
    w.link(n('Read spent'), n('Room?'));
    w.link(n('Room?'), n('Call the model'), 0);
    w.link(n('Room?'), n('Over budget'), 1);
    w.link(n('Call the model'), n('What it cost'));
    w.link(n('Write spent'), n('Called'));
    w.link(n('Called'), n('Loop'));
    w.link(n('Over budget'), n('Loop'));
  });

  const y = NAIVE.length * 520;
  w.hook('open', 'lab/open', [0, y]);
  w.rows('open · data table row', [220, y], LAB, 'insert', { columns: mapped(LAB_COLUMNS, { budget_key: '={{ $json.body.budget }}', spent: 0 }), options: {} });
  w.sql('open · postgres row', [440, y], ['INSERT INTO ai_budget_lab (budget_key) VALUES ($1::text) ON CONFLICT DO NOTHING RETURNING budget_key;'], "[$('open').first().json.body.budget]");
  w.answer('open · answer', [660, y], 200, '{ ok: true }');
  w.chain('open', 'open · data table row', 'open · postgres row', 'open · answer');

  w.hook('spent', 'lab/spent', [0, y + 200]);
  w.rows('spent · data table row', [220, y + 200], LAB, 'get', { ...conditions([['budget_key', '={{ $json.body.budget }}']]), returnAll: false, limit: 1 });
  w.sql('spent · postgres row', [440, y + 200], ['SELECT spent FROM ai_budget_lab WHERE budget_key = $1::text;'], "[$('spent').first().json.body.budget]");
  w.answer('spent · answer', [660, y + 200], 200, "{ 'dt-read-then-add': Number($('spent · data table row').first().json.spent ?? 0), 'pg-check-then-add': Number($json.spent ?? 0) }");
  w.chain('spent', 'spent · data table row', 'spent · postgres row', 'spent · answer');
});
lab.settings.saveDataSuccessExecution = 'none';

// --- write -------------------------------------------------------------------

export const workflows = {
  '0-setup-data-tables.json': setupTables,
  '0-setup-postgres.json': setupPostgres,
  '1-budget-postgres.json': ledgerPostgres,
  '2-budget-data-table.json': ledgerTables,
  '3-example-run.json': exampleRun,
  '9-lab.json': lab,
};

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const out = join(root, 'workflows');
  mkdirSync(out, { recursive: true });
  for (const [file, definition] of Object.entries(workflows)) {
    writeFileSync(join(out, file), `${JSON.stringify(definition, null, 2)}\n`);
    console.log(`workflows/${file}: ${definition.nodes.length} nodes`);
  }
}
