# n8n shared budget

[![test](https://github.com/feversoul/n8n-shared-budget/actions/workflows/test.yml/badge.svg)](https://github.com/feversoul/n8n-shared-budget/actions/workflows/test.yml)

Several n8n runs make one short model call per item. Each run stays inside its own limit, and together they still spend more than the shared budget. This repo measures how far two common counters overshoot when runs overlap, and ships a budgeted model call as a sub-workflow that does not: reserve before the call, settle to the real cost after it, release on failure, give back the reservations of dead runs. One version keeps the ledger in PostgreSQL, one in n8n **Data Tables** only (no database of your own, so it can run on n8n Cloud).

Everything here was run on a real n8n 2.47.7. The model is a local stand-in that answers in the OpenAI chat completions shape with a `usage` block and counts every request and token it serves. No real model was called.

| Question                                             | Answer in this repo                                                                                                                                                                                                 |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Where do you enforce the cap?                        | In one shared ledger, right before the model node, by reserving the worst-case cost of the call. A sub-workflow does it, so every workflow that calls the model goes through the same place.                        |
| Does a counter that is checked and then bumped hold? | No. With 8 runs at once it went over the cap in every round, also when the bump itself was atomic. See [What was measured](#what-was-measured).                                                                     |
| Does it need Postgres or Redis?                      | No. A Data Table cannot add to an old value, so the ledger has no total row: every reservation is its own row and the total is the sum of the rows. It held in every round. Postgres is faster and wastes less.     |
| Wait, skip the AI step, or review queue?             | A setting, per workflow or per item: `wait`, `skip` or `queue`. All three are tested. See [When the budget has no room](#when-the-budget-has-no-room).                                                              |
| What about retries, failed calls and crashed runs?   | The same item never gets a second reservation. An error answer releases it. No answer at all keeps it reserved. A run that died gives its reservation back after a lifetime. See [After the call](#after-the-call). |

## What was measured

One budget of 3000 micro-dollars (0.003 USD). One call costs about 124 and reserves 420 first, so 21 calls fit when they come one after the other. Then 8 runs start at the same moment with 12 items each, 96 calls wanted, 5 rounds. "Billed" is what the stand-in counted.

| How the budget is kept                                                       | One run alone       | 8 runs at once, n8n on SQLite                                              | 8 runs at once, n8n on PostgreSQL                                               |
| ---------------------------------------------------------------------------- | ------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------------------------- |
| Data Table counter: read spent, check, call, read again, write spent + cost  | 21 calls, under cap | **over in 5 of 5 rounds**, worst 8308 billed (67 calls), counter said 2604 | **over in 5 of 5 rounds**, worst 11904 billed (all 96 calls), counter said 2232 |
| Postgres counter: `SELECT spent`, check, call, `UPDATE spent = spent + cost` | 21 calls, under cap | **over in 5 of 5 rounds**, worst 3472 billed (28 calls), counter said 3472 | **over in 5 of 5 rounds**, worst 3348 billed (27 calls), counter said 3348      |
| **Postgres ledger** of this repo: reserve in one conditional statement       |                     | under in 5 of 5, 21 calls every round, ledger = billed                     | under in 5 of 5, 21 calls every round, ledger = billed                          |
| **Data Table ledger** of this repo: one row per reservation, sum the rows    |                     | under in 5 of 5, 18 to 21 calls per round, ledger = billed                 | under in 5 of 5, 20 or 21 calls per round, ledger = billed                      |

"n8n on SQLite / PostgreSQL" is the database n8n itself runs on, which is where its Data Tables live. The two Postgres rows use a separate budget database in both runs.

Notes on the rows:

- **Both counters are correct for one run.** The overshoot only appears when runs overlap, which is why it is easy to miss.
- **The Data Table counter** loses updates: two runs read the same total and each writes its own sum back. With n8n on PostgreSQL it let all 96 calls through, four times the cap, and its own number still looked fine (2232 of 3000).
- **The Postgres counter** adds atomically, so its number is right. It still overshoots, because the check happens before the call and the cost is added after it: every run that is in the middle of a call is invisible to the others. The overshoot is about one call per overlapping run.
- **The Postgres ledger** let exactly 21 calls through in every round, the same as one run alone.
- **The Data Table ledger** never went over, and let 18 to 21 of the 21 possible calls through. Near the cap it can refuse a call that would have fitted, because rows of runs that are still deciding count too. It errs on the safe side. A second pass with 24 runs at once, 8 rounds, n8n on PostgreSQL: never over, 15 to 21 calls per round (Postgres ledger: 21 every round).

## How it works

Both ledgers are a sub-workflow with the same input and answer. Money is counted in micro-dollars (1 USD = 1,000,000), so every sum is a whole number.

```
in:  { item_key, prompt }            optional: { budget, cap_usd, on_over_budget }
out: { ai: true,  text, cost_micro_usd, prompt_tokens, completion_tokens }
     { ai: false, reason: "over_budget" | "duplicate" | "call_failed" | "unknown", queued }
```

`item_key` is a stable id of this model call. It must be the same when the run is retried.

The reservation is the worst case of one call: `RESERVE_INPUT_TOKENS` input tokens plus `MAX_OUTPUT_TOKENS` output tokens at your prices. `MAX_OUTPUT_TOKENS` is sent as `max_tokens`, so the output cannot cost more.

**1 Budgeted model call (Postgres)**, `workflows/1-budget-postgres.json`. The reservation is one statement. `held` is open reservations plus money already spent:

```sql
WITH existing AS (
  SELECT status FROM ai_reservations WHERE budget_key = $1 AND item_key = $4
), budget AS (
  INSERT INTO ai_budget (budget_key, cap, held)
  SELECT $1, $2, $3
  WHERE $3 <= $2 AND NOT EXISTS (SELECT 1 FROM existing)
  ON CONFLICT (budget_key) DO UPDATE
    SET held = ai_budget.held + EXCLUDED.held, cap = EXCLUDED.cap, updated_at = now()
    WHERE ai_budget.held + EXCLUDED.held <= EXCLUDED.cap
  RETURNING held
), claim AS (
  INSERT INTO ai_reservations (budget_key, item_key, owner, amount, expires_at)
  SELECT $1, $4, $5, $3, now() + make_interval(secs => $6) FROM budget
  RETURNING item_key
)
SELECT (SELECT status FROM existing) AS existing, (SELECT held FROM budget) AS held, (SELECT count(*) FROM claim) AS reserved;
```

No `held` in the answer means no room. The first call of a new window creates the budget row in the same statement. The item key is stored in the same transaction as the budget change: if two runs bring the same new item at the same moment, the second insert hits the primary key, the whole statement is undone and that run leaves through the node's error output as a duplicate.

**2 Budgeted model call (Data Tables)**, `workflows/2-budget-data-table.json`. A Data Table update cannot compute from the old value, and "read the total, then write it back" is the counter that lost updates above. So there is no total row:

1. Insert a row with the worst-case cost of the call.
2. Read all rows of this budget and add them up.
3. If the sum, the own row included, is within the cap, the reservation holds.
4. If not, but the own row fits together with the rows before it (lower row id), wait a moment and read again, up to 6 times. Otherwise delete the own row.

Why this holds: every run inserts before it reads. Of two overlapping runs, at least one reads after the other has inserted, so that one sees both amounts. The run that reads last sees every reservation that holds, and it only holds itself if the sum of all rows is within the cap. That part does not depend on row ids or on the order in which rows become visible.

Step 4 is there because of what the first version did. Without it, runs that arrive together all see each other and all step back: with n8n on PostgreSQL the 8 runs got 0, 17, 10, 13 and 4 calls through in five rounds, where 21 fitted. The row id only decides who steps back. It never lets a call through.

Settling writes the real cost into the own row, releasing deletes it.

### When the budget has no room

`ON_OVER_BUDGET` in the **Settings** node, or `on_over_budget` on the item:

| Value   | What happens to the item                                                                                                                                                         |
| ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `wait`  | Tries again every `WAIT_SECONDS`, at most `MAX_WAITS` times, then goes to the queue. Room appears when other calls settle below what they reserved, or when a new window starts. |
| `skip`  | Answers `ai: false`. The calling workflow takes its path without the model.                                                                                                      |
| `queue` | Stores the item (`ai_budget_queue` or the data table `budget_queue`) and answers `ai: false, queued: true`. The default.                                                         |

Waiting keeps an execution open and only helps when room will appear soon. For a daily budget that is used up, `queue` or `skip` is the honest answer. Nothing in this repo empties the queue: review it, or feed it to a later run.

### After the call

| What came back                         | What the ledger does                                                                                                                                |
| -------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| An answer with `usage`                 | **Settle**: the reservation becomes the real cost, the rest is free again.                                                                          |
| An error answer from the API (4xx/5xx) | **Release**: the whole reservation is free again and the item may be tried again.                                                                   |
| No answer (time-out, connection lost)  | **Unknown**: the provider may have done the work. The reservation stays, the item is not called again, and the sweep leaves it. Resolve it by hand. |
| The run died before any of this        | The sweep gives the reservation back once it is older than `RESERVATION_TTL_SECONDS`.                                                               |

The last row is the default, and it is a choice. The test shows its price: n8n was killed during two calls, the stand-in finished and billed both (248 micro-dollars), and after the lifetime both ledgers showed 0 for them. If you would rather keep that money reserved until someone has checked the provider, switch the **Every minute** trigger off. A dead reservation then stays `reserved` for the rest of the window.

The model node has no Retry On Fail, on purpose: one reservation pays for one request. If you use a chain or agent node instead, set the model's Max Retries to 0, or every hidden retry is a call the budget did not see.

### Which one on n8n Cloud

- **You have a Postgres you can reach from Cloud** (Supabase, Neon, RDS): use the Postgres ledger. One statement, 21 of 21 calls in the test, and the time per call does not grow.
- **You do not:** the Data Table ledger holds the cap with nothing outside n8n. It costs more Data Table operations per call, it can refuse a call that would have fitted when runs collide near the cap, and it reads all rows of the window to add them up. Measured by hand with n8n on SQLite: about 50 ms per call on an empty window, about 60 ms with 1000 rows, about 130 ms with 4000 rows (the Postgres ledger stayed near 30 ms). For more than a few thousand calls per window, use a shorter `WINDOW` or Postgres.

## Add it to your n8n

For n8n Cloud or an n8n you already run (2.x).

1. Create a credential of type **OpenAI** named exactly `Shared Budget Model` with your API key. The address of the API is `MODEL_URL` in **Settings**: any OpenAI-compatible chat completions endpoint that returns `usage`.
2. Pick one ledger:
   - **Postgres**: create a **Postgres** credential named exactly `Shared Budget Postgres`. Import `0-setup-postgres.json` and `1-budget-postgres.json`.
   - **Data Tables**: import `0-setup-data-tables.json` and `2-budget-data-table.json`.
3. The setup workflow has a webhook for scripted installs that wants a **Header Auth** credential named `Shared Budget Key`. Create the credential, or delete that webhook node together with the **Done** node behind it (without the webhook, a run ends red on **Done** with "No Webhook node found", after the tables were created). Then press **Execute workflow**. It creates the tables.
4. Open the budgeted workflow and set the values at the top of **Settings**: `BUDGET`, `WINDOW`, `CAP_USD`, `ON_OVER_BUDGET`, the model, its two prices and the token limits. Publish it, so the sweep runs.
5. In your own workflow, where the model node was, add **Execute Sub-workflow**, pick the budgeted workflow, mode **Run once for each item**, and pass `item_key` and `prompt`. Then branch on `ai`.

On n8n Cloud the sweep is one execution a minute once the budgeted workflow is published, about 43,200 a month. That is more than the Starter plan (2,500 a month) or the Pro plan (10,000) includes, as listed on the Cloud plan page on 2026-11-14. Open **Every minute** and set a longer interval: a dead run's reservation is given back by the first sweep after `RESERVATION_TTL_SECONDS`, so an hourly sweep means up to an hour. Neither ledger was run on n8n Cloud.

`3-example-run.json` shows step 5 (it also needs the `Shared Budget Key` credential for its webhooks). `9-lab.json` is only needed to repeat the measurements.

The model call is an HTTP Request node, because it returns `usage` and the reservation needs the real cost. Replace it with your own node if you like, as long as **What it cost** still gets the token counts.

## Try it with Docker

```bash
git clone https://github.com/feversoul/n8n-shared-budget.git
cd n8n-shared-budget
cp .env.example .env      # fill in three random strings
docker compose up -d

# 6 runs at once, 10 items each, against a budget of 0.002 USD
for r in 1 2 3 4 5 6; do
  curl -s -o /dev/null -X POST localhost:5678/webhook/budget/run/data-table \
    -H "X-Budget-Key: <your key>" -H "Content-Type: application/json" \
    -d "{\"budget\":\"demo\",\"cap_usd\":0.002,\"on_over_budget\":\"skip\",
         \"items\":[$(for i in 1 2 3 4 5 6 7 8 9 10; do printf '{"id":"r%s-%s","text":"demo message %s"},' $r $i $i; done | sed 's/,$//')]}" &
done; wait

curl "localhost:8080/stats?match=demo"    # what the stand-in billed
curl -X POST localhost:5678/webhook/budget/status/data-table -H "X-Budget-Key: <your key>" \
  -H "Content-Type: application/json" -d "{\"budget_key\":\"demo:$(date -u +%F)\"}"
```

The compose file starts n8n, a PostgreSQL for the Postgres ledger and the model stand-in. Replace `data-table` with `postgres` for the other ledger.

## What was tested

`npm run test:e2e` starts a real n8n 2.47.7, imports these workflows and calls them over HTTP. `npm run test:e2e:postgres` does the same with n8n itself on PostgreSQL. Both pass: 22 tests each.

| Test, for both ledgers                          | Result with n8n on SQLite and on PostgreSQL                                                                                |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------- |
| 8 runs at once, 12 items each, 5 rounds         | never billed more than the cap, every request was a reserved one, afterwards the ledger holds exactly what was billed      |
| Over budget, `skip` and `queue`                 | 3 of 6 items fit and are called, 3 get `ai: false`; with `queue` they are in the queue; 3 calls reserved 1260 and cost 372 |
| Over budget, `wait`                             | the item waits while another call holds the room and goes through once that call has settled; the two calls do not overlap |
| Over budget, `wait`, no room ever               | gives up after `MAX_WAITS` and queues the item, no model call                                                              |
| The API answers 500                             | reservation released, one request per item, the retry of the run succeeds                                                  |
| No answer (the API replies after the time-out)  | reservation stays as `unknown`, the retry of the run is refused as `duplicate`, the sweep leaves it                        |
| The same item in 10 runs at once                | one model call, nine `duplicate`                                                                                           |
| n8n killed during two calls, then started again | both reservations are given back after their lifetime and the items can run again                                          |
| No key                                          | `403`, no model call                                                                                                       |

What was not tested, and other limits:

- **A real model API.** The stand-in follows the OpenAI chat completions shape (`model`, `messages`, `max_tokens` in, `choices` and `usage` out). Newer OpenAI models want `max_completion_tokens`; change the body in **Call the model**. Cached-token discounts and reasoning tokens are not priced.
- **n8n Cloud.** The tests ran on self-hosted n8n in Docker. To run them against your own instance: `N8N_URL=https://you.app.n8n.cloud WEBHOOK_KEY=... MODEL_URL=... node --test test/e2e.test.mjs`, with a reachable copy of `docker/model.mjs` and the test values from `test/test.env` in the Settings nodes. The crash test is skipped there.
- **Queue mode with several workers.** Every test ran on one n8n process. The argument for the Data Table ledger does not depend on one process, but it was not run on several.
- **A call that costs more than it reserved.** Settle books the real cost, so the cap can be passed by that difference. `max_tokens` bounds the output; a prompt longer than `RESERVE_INPUT_TOKENS` reserves one token per two characters, which is a guess, not a tokenizer.
- **A run slower than `RESERVATION_TTL_SECONDS`.** The sweep takes its reservation and its cost is not booked (Data Tables) or booked on top (Postgres). Keep the lifetime well above the time-out. Not tested.
- **The duplicate check in the Data Table ledger** goes by the lowest row id for the item key. With n8n on PostgreSQL and several workers, two runs could both think they are first and both call the model. The cap still holds; the item is called twice.
- **Several items of one run are budgeted one after the other.** The sub-workflow must run once for each item.
- **Old rows are never deleted.** Add a scheduled cleanup for windows that are over.
- **The numbers are counts from these runs, not rates.** A counter that races overshoots by a different amount each time.

## Change the workflows

The files in `workflows/` are generated by [`scripts/build.mjs`](scripts/build.mjs), because the two ledgers share most of their nodes. Edit them in the n8n editor like any workflow, or change the script and run `npm run build`.

MIT licence.
