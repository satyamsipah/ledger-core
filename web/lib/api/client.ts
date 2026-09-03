import "server-only";

import type {
  Account,
  AccountList,
  AccountSearchParams,
  Balance,
  BalanceAsOf,
  Problem,
  ReconciliationRun,
  ReconciliationRunList,
  Saga,
  SagaList,
  SagaStatus,
  Statement,
  Transaction,
  TransactionList,
  TransactionSearchParams,
} from "@/lib/api/types";
import { ApiError } from "@/lib/api/types";
import * as mock from "@/lib/api/mock/queries";

// The single seam between "the dashboard" and "where its data comes from".
//
// LEDGER_DATA_MODE=mock (the default, so `pnpm dev` works with no backend
// running) serves the synthetic dataset in lib/api/mock. LEDGER_DATA_MODE=live
// calls the real API at LEDGER_API_URL, authenticated with LEDGER_API_KEY --
// both read server-side only (this module is `server-only`), so the key is
// never sent to the browser. Every function below has the identical
// signature and throws the identical ApiError in both modes, so no page
// component needs to know which one is active.
function dataMode(): "mock" | "live" {
  return process.env.LEDGER_DATA_MODE === "live" ? "live" : "mock";
}

function apiBaseURL(): string {
  const url = process.env.LEDGER_API_URL;
  if (!url) {
    throw new Error("LEDGER_API_URL must be set when LEDGER_DATA_MODE=live");
  }
  return url.replace(/\/+$/, "");
}

function apiKey(): string {
  const key = process.env.LEDGER_API_KEY;
  if (!key) {
    throw new Error("LEDGER_API_KEY must be set when LEDGER_DATA_MODE=live");
  }
  return key;
}

async function liveFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${apiBaseURL()}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${apiKey()}`,
      ...init?.headers,
    },
    // Dashboard reads are allowed to be a little stale rather than hammer the
    // API on every render; individual pages that need fresher data pass
    // their own `cache`/`next` options by calling liveFetch's caller with
    // stricter revalidation, but the default here favors not overwhelming a
    // small admin API with duplicate requests during development.
    next: { revalidate: 10 },
  });

  if (!res.ok) {
    let problem: Problem | undefined;
    try {
      problem = (await res.json()) as Problem;
    } catch {
      // Body wasn't problem+json (a 5xx from something in front of the API,
      // for instance); fall through with no parsed problem.
    }
    throw new ApiError(res.status, problem);
  }

  return (await res.json()) as T;
}

function qs(params: Record<string, unknown>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") search.set(key, String(value));
  }
  const s = search.toString();
  return s ? `?${s}` : "";
}

export async function searchTransactions(params: TransactionSearchParams): Promise<TransactionList> {
  if (dataMode() === "mock") return mock.mockSearchTransactions(params);
  return liveFetch(`/v1/transactions${qs({ ...params })}`);
}

export async function getTransaction(id: string): Promise<Transaction> {
  if (dataMode() === "mock") return mock.mockGetTransaction(id);
  return liveFetch(`/v1/transactions/${id}`);
}

export async function searchAccounts(params: AccountSearchParams): Promise<AccountList> {
  if (dataMode() === "mock") return mock.mockSearchAccounts(params);
  return liveFetch(`/v1/accounts${qs({ ...params })}`);
}

export async function getAccount(id: string): Promise<Account> {
  if (dataMode() === "mock") return mock.mockGetAccount(id);
  return liveFetch(`/v1/accounts/${id}`);
}

export async function getBalance(id: string): Promise<Balance> {
  if (dataMode() === "mock") return mock.mockGetBalance(id);
  return liveFetch(`/v1/accounts/${id}/balance`);
}

export async function getBalanceAsOf(id: string, asOf: string): Promise<BalanceAsOf> {
  if (dataMode() === "mock") return mock.mockGetBalanceAsOf(id, asOf);
  return liveFetch(`/v1/accounts/${id}/balance${qs({ as_of: asOf })}`);
}

export async function getStatement(
  id: string,
  params: { from?: string; to?: string; limit?: number; cursor?: string },
): Promise<Statement> {
  if (dataMode() === "mock") return mock.mockGetStatement(id, params);
  return liveFetch(`/v1/accounts/${id}/statement${qs({ ...params })}`);
}

export async function listSagas(status?: SagaStatus, limit = 100): Promise<SagaList> {
  if (dataMode() === "mock") return mock.mockListSagas(status, limit);
  return liveFetch(`/v1/sagas${qs({ status, limit })}`);
}

export async function getSaga(id: string): Promise<Saga> {
  if (dataMode() === "mock") return mock.mockGetSaga(id);
  return liveFetch(`/v1/sagas/${id}`);
}

export async function listReconciliationRuns(limit = 100): Promise<ReconciliationRunList> {
  if (dataMode() === "mock") return mock.mockListReconciliationRuns(limit);
  return liveFetch(`/v1/reconciliation/runs${qs({ limit })}`);
}

export async function getReconciliationRun(id: string): Promise<ReconciliationRun> {
  if (dataMode() === "mock") return mock.mockGetReconciliationRun(id);
  return liveFetch(`/v1/reconciliation/runs/${id}`);
}

export { ApiError };
export { dataMode };
