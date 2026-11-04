// k6 load test for the query API: holds a fixed request rate (an open model, so a
// slow server shows up as latency instead of quietly lowering the load) and
// reports latency percentiles. Usually driven by run_api_load.py, which steps the
// rate up; it can also be run on its own:
//
//   docker run --rm -i --network fraud-detection_default -v "$PWD/scripts/loadtest:/scripts" \
//     -e CLIENT_SECRET=local-dev-analyst-secret -e ENDPOINT=get -e RATE=200 \
//     grafana/k6 run /scripts/api.js
//
// ENDPOINT: get | list | evaluate | mixed (70% get, 25% list, 5% evaluate)

import http from "k6/http";
import { check, fail } from "k6";

const API_URL = __ENV.API_URL || "http://api:8000";
const TOKEN_URL = __ENV.TOKEN_URL || "http://keycloak:8080/realms/fraud/protocol/openid-connect/token";
const CLIENT_ID = __ENV.CLIENT_ID || "fraud-analyst";
const CLIENT_SECRET = __ENV.CLIENT_SECRET;
const ENDPOINT = __ENV.ENDPOINT || "mixed";
const RATE = Number(__ENV.RATE || 100);
const DURATION = __ENV.DURATION || "30s";

export const options = {
  discardResponseBodies: true,
  scenarios: {
    load: {
      executor: "constant-arrival-rate",
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Math.max(20, RATE),
      maxVUs: Math.max(200, RATE * 4),
    },
  },
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(95)", "p(99)", "max"],
  // Always-true thresholds make k6 report these sub-metrics: the load scenario's
  // requests only, without the token and seeding requests made in setup().
  thresholds: {
    "http_reqs{scenario:load}": ["count>=0"],
    "http_req_duration{scenario:load}": ["max>=0"],
    "http_req_failed{scenario:load}": ["rate>=0"],
  },
};

export function setup() {
  if (!CLIENT_SECRET) fail("CLIENT_SECRET is required (the fraud-analyst client secret)");
  const token = http.post(
    TOKEN_URL,
    { grant_type: "client_credentials", client_id: CLIENT_ID, client_secret: CLIENT_SECRET },
    { responseType: "text" },
  );
  if (token.status !== 200) fail(`token request failed: ${token.status} ${token.body}`);
  const headers = { Authorization: `Bearer ${token.json("access_token")}` };

  // Real ids to read back, so "get" measures found rows rather than 404s.
  const ids = [];
  const users = new Set();
  let cursor = null;
  while (ids.length < 1000) {
    const page = http.get(`${API_URL}/api/v1/transactions?limit=100${cursor ? `&cursor=${cursor}` : ""}`, {
      headers,
      responseType: "text",
    });
    if (page.status !== 200) fail(`seed listing failed: ${page.status} ${page.body}`);
    for (const row of page.json("data")) {
      ids.push(row.transaction_id);
      users.add(row.user_id);
    }
    cursor = page.json("next_cursor");
    if (!cursor) break;
  }
  if (ids.length === 0) fail("no transactions to read; run pipeline_load.py first to seed data");
  return { headers, ids, users: [...users] };
}

function pick(list) {
  return list[Math.floor(Math.random() * list.length)];
}

const requests = {
  get: (data) =>
    http.get(`${API_URL}/api/v1/transactions/${pick(data.ids)}`, {
      headers: data.headers,
      tags: { endpoint: "get" },
    }),
  list: (data) =>
    http.get(`${API_URL}/api/v1/transactions?user_id=${pick(data.users)}&limit=50`, {
      headers: data.headers,
      tags: { endpoint: "list" },
    }),
  evaluate: (data) =>
    http.post(
      `${API_URL}/api/v1/rule-sets/fraud-default/evaluate`,
      JSON.stringify({ user_id: pick(data.users), amount: "1500.00", currency: "ZAR", category: "GROCERIES" }),
      { headers: { ...data.headers, "Content-Type": "application/json" }, tags: { endpoint: "evaluate" } },
    ),
};

export default function (data) {
  let name = ENDPOINT;
  if (ENDPOINT === "mixed") {
    const roll = Math.random();
    name = roll < 0.7 ? "get" : roll < 0.95 ? "list" : "evaluate";
  }
  const response = requests[name](data);
  check(response, { "status is 200": (r) => r.status === 200 });
}
