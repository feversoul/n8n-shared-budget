// Used by bootstrap.sh inside the n8n container: writes the import files for
// the n8n CLI from the environment, so no secret is stored in the repository.
//   node prepare.mjs <workflows dir> <output dir>

import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const [source, out] = process.argv.slice(2);
const env = process.env;

const credentials = [
  {
    id: 'sharedBudgetKey',
    name: 'Shared Budget Key',
    type: 'httpHeaderAuth',
    data: { name: 'X-Budget-Key', value: env.WEBHOOK_KEY },
  },
  {
    id: 'sharedBudgetModel',
    name: 'Shared Budget Model',
    type: 'openAiApi',
    data: { apiKey: env.MODEL_API_KEY || 'local-stand-in', header: false },
  },
  {
    id: 'sharedBudgetPg',
    name: 'Shared Budget Postgres',
    type: 'postgres',
    data: {
      host: env.BUDGET_DB_HOST || 'postgres',
      port: Number(env.BUDGET_DB_PORT || 5432),
      database: env.BUDGET_DB_NAME || 'budget',
      user: env.BUDGET_DB_USER || 'budget',
      password: env.BUDGET_DB_PASSWORD,
      ssl: 'disable',
    },
  },
];

mkdirSync(join(out, 'workflows'), { recursive: true });
writeFileSync(join(out, 'credentials.json'), JSON.stringify(credentials));

// Settings that can be overridden from the environment: the model address,
// and shorter waits for the tests. Each is a `const NAME = value;` line in a
// Settings node.
const numbers = ['WAIT_SECONDS', 'MAX_WAITS', 'RESERVATION_TTL_SECONDS', 'CALL_TIMEOUT_SECONDS'];

for (const file of readdirSync(source).filter((name) => name.endsWith('.json'))) {
  const workflow = JSON.parse(readFileSync(join(source, file), 'utf8'));
  for (const node of workflow.nodes) {
    if (typeof node.parameters.jsCode === 'string') {
      let code = node.parameters.jsCode;
      if (env.MODEL_URL) code = code.replace(/const MODEL_URL = '[^']*';/, `const MODEL_URL = '${env.MODEL_URL}';`);
      for (const name of numbers) {
        if (Number(env[name]) > 0) code = code.replace(new RegExp(`const ${name} = [0-9.]+;`), `const ${name} = ${Number(env[name])};`);
      }
      node.parameters.jsCode = code;
    }
    const seconds = Number(env.SWEEP_INTERVAL_SECONDS || 0);
    if (seconds > 0 && node.type === 'n8n-nodes-base.scheduleTrigger') {
      node.parameters.rule = { interval: [{ field: 'seconds', secondsInterval: seconds }] };
    }
  }
  writeFileSync(join(out, 'workflows', file), JSON.stringify(workflow));
}
