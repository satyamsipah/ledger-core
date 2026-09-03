// Mock implementations of every read this dashboard needs, filtering and
// paginating the synthetic dataset the same way the real API documents its
// own behaviour (id-descending keyset pages, ILIKE-style substring match on
// external_ref) so that switching LEDGER_DATA_MODE from mock to live changes
// nothing about how a page consumes the result.

import type {
  Account,
  AccountList,
  AccountSearchParams,
  Balance,
  BalanceAsOf,
  ReconciliationRun,
  ReconciliationRunList,
  Saga,
  SagaList,
  SagaStatus,
  Statement,
  StatementLine,
  Transaction,
  TransactionList,
  TransactionSearchParams,
} from "@/lib/api/types";
import { ApiError } from "@/lib/api/types";
import {
  mockAccounts,
  mockBalances,
  mockEntriesForTransaction,
  mockReconciliationRuns,
  mockSagas,
  mockTransactions,
} from "@/lib/api/mock/data";

const DEFAULT_LIMIT = 100;

function encodeCursor(id: string): string {
  return Buffer.from(id, "utf8").toString("base64url");
}
function decodeCursor(token: string): string {
  return Buffer.from(token, "base64url").toString("utf8");
}

function contains(haystack: string | undefined, needle: string): boolean {
  return (haystack ?? "").toLowerCase().includes(needle.toLowerCase());
}

async function delay() {
  // A small, fixed latency so loading states are actually visible in mock
  // mode -- real network calls are never instant, and a UI whose skeletons
  // only ever appear against a live backend is a UI nobody verified.
  await new Promise((resolve) => setTimeout(resolve, 120));
}

export async function mockSearchTransactions(params: TransactionSearchParams): Promise<TransactionList> {
  await delay();

  let results = mockTransactions.filter((t) => {
    if (params.external_ref && !contains(t.external_ref, params.external_ref)) return false;
    if (params.status && t.status !== params.status) return false;
    if (params.type && t.type !== params.type) return false;
    if (params.account_id && !t.entries.some((e) => e.account_id === params.account_id)) return false;
    if (params.from && t.created_at < params.from) return false;
    if (params.to && t.created_at > params.to) return false;
    return true;
  });

  if (params.cursor) {
    const after = decodeCursor(params.cursor);
    results = results.filter((t) => t.id < after);
  }

  const limit = params.limit ?? DEFAULT_LIMIT;
  const page = results.slice(0, limit);
  const nextCursor = results.length > limit ? encodeCursor(page[page.length - 1].id) : null;

  return {
    transactions: page.map((t) => ({ ...t, entries: [] })),
    next_cursor: nextCursor,
  };
}

export async function mockGetTransaction(id: string): Promise<Transaction> {
  await delay();
  const tx = mockTransactions.find((t) => t.id === id);
  if (!tx) {
    throw new ApiError(404, { type: "transaction-not-found", title: "No such transaction", status: 404 });
  }
  return { ...tx, entries: mockEntriesForTransaction(id) };
}

export async function mockSearchAccounts(params: AccountSearchParams): Promise<AccountList> {
  await delay();

  let results = mockAccounts.filter((a) => {
    if (params.external_ref && !contains(a.external_ref, params.external_ref)) return false;
    if (params.owner_id && a.owner_id !== params.owner_id) return false;
    if (params.currency && a.currency !== params.currency) return false;
    return true;
  });

  if (params.cursor) {
    const after = decodeCursor(params.cursor);
    results = results.filter((a) => a.id < after);
  }

  const limit = params.limit ?? DEFAULT_LIMIT;
  const page = results.slice(0, limit);
  const nextCursor = results.length > limit ? encodeCursor(page[page.length - 1].id) : null;

  return { accounts: page, next_cursor: nextCursor };
}

export async function mockGetAccount(id: string): Promise<Account> {
  await delay();
  const account = mockAccounts.find((a) => a.id === id);
  if (!account) {
    throw new ApiError(404, { type: "account-not-found", title: "No such account", status: 404 });
  }
  return account;
}

export async function mockGetBalance(id: string): Promise<Balance> {
  await delay();
  const account = mockAccounts.find((a) => a.id === id);
  const bal = mockBalances.get(id);
  if (!account || !bal) {
    throw new ApiError(404, { type: "account-not-found", title: "No such account", status: 404 });
  }
  return {
    account_id: id,
    available: { amount: String(bal.availableMinor), currency: account.currency, scale: 2 },
    version: bal.version,
    updated_at: bal.updatedAt,
  };
}

export async function mockGetBalanceAsOf(id: string, asOf: string): Promise<BalanceAsOf> {
  await delay();
  const account = mockAccounts.find((a) => a.id === id);
  if (!account) {
    throw new ApiError(404, { type: "account-not-found", title: "No such account", status: 404 });
  }

  let minor = 0;
  for (const t of mockTransactions) {
    if (t.created_at > asOf) continue;
    for (const e of mockEntriesForTransaction(t.id)) {
      if (e.account_id !== id) continue;
      const signed = e.direction === account.normal_balance ? 1 : -1;
      minor += signed * Number(e.amount.amount);
    }
  }

  return { account_id: id, as_of: asOf, balance: { amount: String(minor), currency: account.currency, scale: 2 } };
}

export async function mockGetStatement(
  id: string,
  params: { from?: string; to?: string; limit?: number; cursor?: string },
): Promise<Statement> {
  await delay();
  const account = mockAccounts.find((a) => a.id === id);
  if (!account) {
    throw new ApiError(404, { type: "account-not-found", title: "No such account", status: 404 });
  }

  const from = params.from ?? new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
  const to = params.to ?? new Date().toISOString();

  const allLines = mockTransactions
    .flatMap((t) => mockEntriesForTransaction(t.id).map((e) => ({ tx: t, entry: e })))
    .filter((x) => x.entry.account_id === id)
    .sort((a, b) => (a.entry.created_at < b.entry.created_at ? -1 : 1));

  let openingMinor = 0;
  for (const { entry } of allLines) {
    if (entry.created_at >= from) break;
    const signed = entry.direction === account.normal_balance ? 1 : -1;
    openingMinor += signed * Number(entry.amount.amount);
  }

  let windowed = allLines.filter((x) => x.entry.created_at >= from && x.entry.created_at <= to);
  if (params.cursor) {
    const after = decodeCursor(params.cursor);
    windowed = windowed.filter((x) => x.entry.created_at > after);
  }

  const limit = params.limit ?? 100;
  const page = windowed.slice(0, limit);
  const rest = windowed.slice(limit);

  let running = openingMinor;
  const lines: StatementLine[] = page.map(({ entry }) => {
    const signed = entry.direction === account.normal_balance ? 1 : -1;
    const signedMinor = signed * Number(entry.amount.amount);
    running += signedMinor;
    return {
      entry,
      signed: { amount: String(signedMinor), currency: account.currency, scale: 2 },
      running_balance: { amount: String(running), currency: account.currency, scale: 2 },
    };
  });

  return {
    account_id: id,
    currency: account.currency,
    from,
    to,
    opening: { amount: String(openingMinor), currency: account.currency, scale: 2 },
    closing: { amount: String(running), currency: account.currency, scale: 2 },
    lines,
    next_cursor: rest.length > 0 ? encodeCursor(page[page.length - 1].entry.created_at) : null,
  };
}

export async function mockListSagas(status: SagaStatus | undefined, limit: number): Promise<SagaList> {
  await delay();
  const effectiveStatus = status ?? "NEEDS_MANUAL_REVIEW";
  const matches = mockSagas
    .filter((s) => s.status === effectiveStatus)
    .sort((a, b) => (a.updated_at < b.updated_at ? 1 : -1))
    .slice(0, limit)
    .map((s) => ({ ...s, attempts: undefined }));

  return { status: effectiveStatus, sagas: matches };
}

export async function mockGetSaga(id: string): Promise<Saga> {
  await delay();
  const saga = mockSagas.find((s) => s.id === id);
  if (!saga) {
    throw new ApiError(404, { type: "saga-not-found", title: "Saga not found", status: 404 });
  }
  return saga;
}

export async function mockListReconciliationRuns(limit: number): Promise<ReconciliationRunList> {
  await delay();
  return { runs: mockReconciliationRuns.slice(0, limit).map((r) => ({ ...r, exceptions: undefined, by_category: r.by_category })) };
}

export async function mockGetReconciliationRun(id: string): Promise<ReconciliationRun> {
  await delay();
  const run = mockReconciliationRuns.find((r) => r.id === id);
  if (!run) {
    throw new ApiError(404, { type: "reconciliation-run-not-found", title: "Reconciliation run not found", status: 404 });
  }
  return run;
}
