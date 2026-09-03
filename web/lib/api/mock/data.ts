import type {
  Account,
  AccountType,
  Direction,
  Entry,
  Money,
  ReconciliationException,
  ReconciliationRun,
  Saga,
  SagaAttempt,
  SagaStatus,
  Transaction,
  TransactionType,
} from "@/lib/api/types";

// A small, self-consistent synthetic ledger: every account's balance is
// actually the sum of its entries (signed by its own normal balance, exactly
// as D13 defines), every saga step names a transaction that exists, and every
// reconciliation exception names a transaction or saga that exists. Built
// once per server process with a seeded PRNG, so the same request produces
// the same page on every reload rather than a new random dataset each time.

function mulberry32(seed: number) {
  let a = seed;
  return function rand() {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = mulberry32(0x1ed6e5c0);

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(rand() * arr.length)];
}

function fakeId(category: number, n: number): string {
  const cat = category.toString(16).padStart(8, "0");
  const seq = n.toString(16).padStart(12, "0");
  return `${cat}-0000-7000-8000-${seq}`;
}

function money(amountMinor: number, currency: string, scale = 2): Money {
  return { amount: String(amountMinor), currency, scale };
}

function daysAgo(d: number, jitterMinutes = 0): Date {
  const base = Date.now() - d * 24 * 60 * 60 * 1000;
  return new Date(base - jitterMinutes * 60 * 1000 * rand());
}

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

interface SeedAccount {
  ref: string;
  type: AccountType;
  normal: Direction;
  currency: string;
  owner?: string;
  allowNegative: boolean;
  status?: Account["status"];
}

const CUSTOMERS = ["cust_amara", "cust_ravi", "cust_lin", "cust_sofia", "cust_omar", "cust_priya", "cust_jules"];

const seedAccounts: SeedAccount[] = [
  { ref: "platform_float_inr", type: "ASSET", normal: "DEBIT", currency: "INR", allowNegative: true },
  { ref: "platform_float_usd", type: "ASSET", normal: "DEBIT", currency: "USD", allowNegative: true },
  { ref: "platform_suspense_inr", type: "ASSET", normal: "DEBIT", currency: "INR", allowNegative: true },
  { ref: "fee_revenue_inr", type: "REVENUE", normal: "CREDIT", currency: "INR", allowNegative: true },
  { ref: "fee_revenue_usd", type: "REVENUE", normal: "CREDIT", currency: "USD", allowNegative: true },
  { ref: "gateway_fees_expense", type: "EXPENSE", normal: "DEBIT", currency: "INR", allowNegative: true },
  { ref: "merchant_payable_bazaar", type: "LIABILITY", normal: "CREDIT", currency: "INR", allowNegative: false },
  { ref: "merchant_payable_northwind", type: "LIABILITY", normal: "CREDIT", currency: "INR", allowNegative: false },
  { ref: "frozen_investigation_hold", type: "LIABILITY", normal: "CREDIT", currency: "INR", allowNegative: false, status: "FROZEN" },
  { ref: "closed_legacy_wallet", type: "LIABILITY", normal: "CREDIT", currency: "INR", allowNegative: false, status: "CLOSED" },
  ...CUSTOMERS.map((owner, i): SeedAccount => ({
    ref: `wallet_${owner}`,
    type: "LIABILITY",
    normal: "CREDIT",
    currency: i % 5 === 0 ? "USD" : "INR",
    owner,
    allowNegative: false,
  })),
];

export const mockAccounts: Account[] = seedAccounts.map((s, i) => {
  const createdAt = daysAgo(90 - i, 600);
  return {
    id: fakeId(1, i + 1),
    external_ref: s.ref,
    account_type: s.type,
    normal_balance: s.normal,
    currency: s.currency,
    owner_id: s.owner,
    allow_negative: s.allowNegative,
    status: s.status ?? "ACTIVE",
    created_at: createdAt.toISOString(),
    updated_at: createdAt.toISOString(),
  };
});

function accountByRef(ref: string): Account {
  const found = mockAccounts.find((a) => a.external_ref === ref);
  if (!found) throw new Error(`mock data: no seed account named ${ref}`);
  return found;
}

const float = accountByRef("platform_float_inr");
const floatUSD = accountByRef("platform_float_usd");
const feeRevenue = accountByRef("fee_revenue_inr");
const wallets = CUSTOMERS.map((c) => accountByRef(`wallet_${c}`));

// ---------------------------------------------------------------------------
// Transactions and entries
// ---------------------------------------------------------------------------

interface BuiltTransaction {
  tx: Transaction;
  entries: Entry[];
}

const builtTransactions: BuiltTransaction[] = [];
let txCounter = 0;
let entryCounter = 0;

function post(
  type: TransactionType,
  legs: { account: Account; direction: Direction; minor: number }[],
  opts: { externalRef?: string; daysBack: number; status?: Transaction["status"] } = { daysBack: 0 },
): BuiltTransaction {
  txCounter += 1;
  const id = fakeId(2, txCounter);
  const createdAt = daysAgo(opts.daysBack, 1200);
  const currency = legs[0].account.currency;

  const entries: Entry[] = legs.map((leg, i) => {
    entryCounter += 1;
    return {
      id: fakeId(5, entryCounter),
      account_id: leg.account.id,
      direction: leg.direction,
      amount: money(leg.minor, currency),
      entry_seq: i,
      created_at: createdAt.toISOString(),
    };
  });

  const tx: Transaction = {
    id,
    type,
    status: opts.status ?? "POSTED",
    external_ref: opts.externalRef,
    metadata: opts.externalRef ? { channel: pick(["upi", "card", "netbanking", "wallet"]) } : undefined,
    created_at: createdAt.toISOString(),
    posted_at: createdAt.toISOString(),
    entries,
  };

  const built = { tx, entries };
  builtTransactions.push(built);
  return built;
}

// A spread of ordinary transfers over the last 30 days.
for (let i = 0; i < 90; i++) {
  const wallet = pick(wallets);
  const minor = 500 + Math.floor(rand() * 500_00);
  const daysBack = Math.floor(rand() * 30);
  const inbound = rand() > 0.4;

  post(
    "TRANSFER",
    inbound
      ? [
          { account: wallet.currency === "USD" ? floatUSD : float, direction: "DEBIT", minor },
          { account: wallet, direction: "CREDIT", minor },
        ]
      : [
          { account: wallet, direction: "DEBIT", minor },
          { account: wallet.currency === "USD" ? floatUSD : float, direction: "CREDIT", minor },
        ],
    { externalRef: `gw_${fakeId(9, i).slice(-8)}`, daysBack },
  );
}

// A handful of fee-bearing payouts (three legs).
for (let i = 0; i < 10; i++) {
  const wallet = pick(wallets.filter((w) => w.currency === "INR"));
  const gross = 5_000_00 + Math.floor(rand() * 20_000_00);
  const fee = Math.floor(gross * 0.02);
  const net = gross - fee;
  post(
    "PAYOUT",
    [
      { account: wallet, direction: "DEBIT", minor: gross },
      { account: float, direction: "CREDIT", minor: net },
      { account: feeRevenue, direction: "CREDIT", minor: fee },
    ],
    { externalRef: `payout_${fakeId(9, 100 + i).slice(-8)}`, daysBack: Math.floor(rand() * 20) },
  );
}

// A few reversed transactions, each with its own REVERSAL posting.
const reversedIds = new Set<string>();
for (let i = 0; i < 6; i++) {
  const wallet = pick(wallets);
  const minor = 1_000 + Math.floor(rand() * 200_00);
  const original = post(
    "TRANSFER",
    [
      { account: wallet.currency === "USD" ? floatUSD : float, direction: "DEBIT", minor },
      { account: wallet, direction: "CREDIT", minor },
    ],
    { externalRef: `gw_reversed_${i}`, daysBack: 10 + i },
  );
  original.tx.status = "REVERSED";
  reversedIds.add(original.tx.id);

  post(
    "REVERSAL",
    [
      { account: wallet, direction: "DEBIT", minor },
      { account: wallet.currency === "USD" ? floatUSD : float, direction: "CREDIT", minor },
    ],
    { daysBack: 10 + i - 0.2 },
  );
}

export const mockTransactions: Transaction[] = builtTransactions.map((b) => b.tx).sort((a, b) => (a.id < b.id ? 1 : -1));

export function mockEntriesForTransaction(id: string): Entry[] {
  return builtTransactions.find((b) => b.tx.id === id)?.entries ?? [];
}

// ---------------------------------------------------------------------------
// Balances and statements, derived from entries so the numbers reconcile.
// ---------------------------------------------------------------------------

export interface MockBalance {
  accountId: string;
  availableMinor: number;
  version: number;
  updatedAt: string;
}

export const mockBalances: Map<string, MockBalance> = (() => {
  const balances = new Map<string, MockBalance>();
  for (const account of mockAccounts) {
    balances.set(account.id, { accountId: account.id, availableMinor: 0, version: 0, updatedAt: account.created_at });
  }

  // Oldest first, so the running version and updated_at genuinely progress
  // forward in time the way the real posting path's version bump does.
  const chronological = [...builtTransactions].sort((a, b) => a.tx.created_at.localeCompare(b.tx.created_at));

  for (const { tx, entries } of chronological) {
    for (const entry of entries) {
      const account = mockAccounts.find((a) => a.id === entry.account_id)!;
      const bal = balances.get(account.id)!;
      const signed = entry.direction === account.normal_balance ? 1 : -1;
      bal.availableMinor += signed * Number(entry.amount.amount);
      bal.version += 1;
      bal.updatedAt = tx.created_at;
    }
  }

  return balances;
})();

// ---------------------------------------------------------------------------
// Sagas
// ---------------------------------------------------------------------------

const sagaStatuses: { status: SagaStatus; weight: number }[] = [
  { status: "COMPLETED", weight: 10 },
  { status: "COMPLETED", weight: 10 },
  { status: "COMPENSATED", weight: 2 },
  { status: "GATEWAY_PENDING", weight: 2 },
  { status: "NEEDS_MANUAL_REVIEW", weight: 3 },
];

function weightedSagaStatus(): SagaStatus {
  const total = sagaStatuses.reduce((s, x) => s + x.weight, 0);
  let r = rand() * total;
  for (const s of sagaStatuses) {
    if (r < s.weight) return s.status;
    r -= s.weight;
  }
  return "COMPLETED";
}

export const mockSagas: Saga[] = Array.from({ length: 18 }, (_, i) => {
  const status = weightedSagaStatus();
  const wallet = pick(wallets.filter((w) => w.currency === "INR"));
  const createdAt = daysAgo(status === "NEEDS_MANUAL_REVIEW" || status === "GATEWAY_PENDING" ? rand() * 3 : rand() * 20, 800);

  const terminal = status === "COMPLETED" || status === "COMPENSATED";
  const stuck = status === "NEEDS_MANUAL_REVIEW" || status === "GATEWAY_PENDING";
  // Clamped to "now": createdAt can be as recent as a few hours ago, and
  // adding up to 42 hours on top of it would otherwise occasionally land in
  // the future, which formatDistanceToNow renders as "in about N hours" --
  // a stuck saga is stuck as of now, never later than now.
  const updatedAtCandidate = stuck
    ? new Date(createdAt.getTime() + (2 + rand() * 40) * 60 * 60 * 1000)
    : new Date(createdAt.getTime() + rand() * 4000);
  const updatedAt = updatedAtCandidate.getTime() > Date.now() ? new Date() : updatedAtCandidate;

  const reserveTx = post(
    "PAYOUT",
    [
      { account: wallet, direction: "DEBIT", minor: 2_000_00 + Math.floor(rand() * 10_000_00) },
      { account: accountByRef("platform_suspense_inr"), direction: "CREDIT", minor: 2_000_00 },
    ],
    { daysBack: 0 },
  );
  reserveTx.tx.created_at = createdAt.toISOString();
  reserveTx.tx.posted_at = createdAt.toISOString();

  const attempts: SagaAttempt[] = [
    {
      step: "RESERVE",
      direction: "FORWARD",
      attempt: 1,
      status: "SUCCEEDED",
      transaction_id: reserveTx.tx.id,
      started_at: createdAt.toISOString(),
      finished_at: createdAt.toISOString(),
    },
  ];

  if (status !== "PENDING" && status !== "RESERVED") {
    const gwStatus = status === "GATEWAY_PENDING" ? "ATTEMPTED" : status === "GATEWAY_FAILED" ? "FAILED" : "SUCCEEDED";
    attempts.push({
      step: "GATEWAY",
      direction: "FORWARD",
      attempt: 1,
      status: gwStatus,
      error: gwStatus === "FAILED" ? "gateway timeout after 30s" : undefined,
      started_at: new Date(createdAt.getTime() + 2000).toISOString(),
      finished_at: gwStatus === "ATTEMPTED" ? undefined : new Date(createdAt.getTime() + 4000).toISOString(),
    });
  }

  if (terminal || status === "NEEDS_MANUAL_REVIEW") {
    attempts.push({
      step: status === "COMPENSATED" || status === "NEEDS_MANUAL_REVIEW" ? "SETTLE" : "SETTLE",
      direction: status === "COMPENSATED" ? "COMPENSATION" : "FORWARD",
      attempt: 1,
      status: status === "NEEDS_MANUAL_REVIEW" ? "FAILED" : "SUCCEEDED",
      error: status === "NEEDS_MANUAL_REVIEW" ? "gateway outcome unresolved after 3 probes" : undefined,
      started_at: new Date(createdAt.getTime() + 6000).toISOString(),
      finished_at: status === "NEEDS_MANUAL_REVIEW" ? undefined : new Date(createdAt.getTime() + 7000).toISOString(),
    });
  }

  return {
    id: fakeId(3, i + 1),
    saga_type: "payout",
    status,
    current_step: terminal ? "DONE" : status === "GATEWAY_PENDING" ? "GATEWAY" : "SETTLE",
    retry_count: stuck ? 1 + Math.floor(rand() * 3) : Math.floor(rand() * 2),
    last_error: status === "NEEDS_MANUAL_REVIEW" ? "gateway outcome unresolved after 3 probes" : undefined,
    created_at: createdAt.toISOString(),
    updated_at: updatedAt.toISOString(),
    attempts,
  };
});

// ---------------------------------------------------------------------------
// Reconciliation runs
// ---------------------------------------------------------------------------

const exceptionCategories: ReconciliationException["category"][] = [
  "AMOUNT_MISMATCH",
  "MISSING_IN_LEDGER",
  "MISSING_IN_PSP",
  "STATUS_MISMATCH",
  "TIMING_DIFFERENCE",
  "DUPLICATE",
];

export const mockReconciliationRuns: ReconciliationRun[] = Array.from({ length: 6 }, (_, i): ReconciliationRun => {
  const startedAt = daysAgo(i, 60);
  const finishedAt = new Date(startedAt.getTime() + 4 * 60 * 1000);
  const pspRows = 400 + Math.floor(rand() * 200);

  const byCategory: Record<string, number> = {};
  const exceptions: ReconciliationException[] = [];
  const exceptionCount = i === 0 ? 4 : Math.floor(rand() * 3);

  for (let e = 0; e < exceptionCount; e++) {
    const category = pick(exceptionCategories);
    byCategory[category] = (byCategory[category] ?? 0) + 1;
    const linkedTx = pick(mockTransactions.filter((t) => t.external_ref));
    const autoResolved = category === "TIMING_DIFFERENCE" && rand() > 0.5;

    exceptions.push({
      id: fakeId(4, i * 10 + e + 1),
      external_ref: linkedTx.external_ref!,
      category,
      status: autoResolved ? "AUTO_RESOLVED" : rand() > 0.6 ? "RESOLVED" : "OPEN",
      ledger_transaction_id: linkedTx.id,
      ledger_amount_minor: Number(linkedTx.entries[0]?.amount.amount ?? 0),
      psp_amount_minor: Number(linkedTx.entries[0]?.amount.amount ?? 0) + (category === "AMOUNT_MISMATCH" ? 500 : 0),
      currency: linkedTx.entries[0]?.amount.currency ?? "INR",
      ledger_status: linkedTx.status,
      psp_status: category === "STATUS_MISMATCH" ? "FAILED" : "SETTLED",
      details: category === "TIMING_DIFFERENCE" ? { gap_seconds: 45 } : undefined,
      created_at: startedAt.toISOString(),
      resolved_at: autoResolved || rand() > 0.6 ? finishedAt.toISOString() : undefined,
    });
  }

  const autoResolvedCount = exceptions.filter((e) => e.status === "AUTO_RESOLVED").length;

  return {
    id: fakeId(4, 1000 + i),
    source: `s3://ledger-recon/statements/${startedAt.toISOString().slice(0, 10)}.csv`,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    status: "COMPLETED",
    psp_row_count: pspRows,
    matched_count: pspRows - exceptions.length,
    auto_resolved_count: autoResolvedCount,
    exception_count: exceptions.length,
    by_category: byCategory,
    exceptions,
  };
}).sort((a, b) => (a.started_at < b.started_at ? 1 : -1));
