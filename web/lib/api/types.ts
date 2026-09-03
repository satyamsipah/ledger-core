// Wire types mirrored from api/openapi.yaml. Kept as plain interfaces rather
// than generated, since the backend surface this dashboard reads is small and
// stable enough that hand-mirroring is cheaper than a codegen step to keep in
// sync -- but every field name here must match the OpenAPI schema exactly.

export interface Money {
  /** Decimal string of minor units. Never parse this as a JS number. */
  amount: string;
  currency: string;
  scale?: number;
}

export type Direction = "DEBIT" | "CREDIT";

export type TransactionType =
  | "TRANSFER"
  | "PAYIN"
  | "PAYOUT"
  | "FEE"
  | "FX"
  | "REVERSAL"
  | "ADJUSTMENT";

export type TransactionStatus = "PENDING" | "POSTED" | "REVERSED";

export interface Entry {
  id: string;
  account_id: string;
  direction: Direction;
  amount: Money;
  entry_seq: number;
  created_at: string;
}

export interface Transaction {
  id: string;
  type: TransactionType;
  status: TransactionStatus;
  idempotency_key?: string;
  external_ref?: string;
  metadata?: Record<string, unknown>;
  created_at: string;
  posted_at?: string;
  entries: Entry[];
}

export interface TransactionList {
  transactions: Transaction[];
  next_cursor: string | null;
}

export interface TransactionSearchParams {
  external_ref?: string;
  status?: TransactionStatus;
  type?: TransactionType;
  account_id?: string;
  from?: string;
  to?: string;
  limit?: number;
  cursor?: string;
}

export type AccountType = "ASSET" | "LIABILITY" | "EQUITY" | "REVENUE" | "EXPENSE";
export type AccountStatus = "ACTIVE" | "FROZEN" | "CLOSED";

export interface Account {
  id: string;
  external_ref: string;
  account_type: AccountType;
  normal_balance: Direction;
  currency: string;
  owner_id?: string;
  allow_negative: boolean;
  status: AccountStatus;
  created_at: string;
  updated_at: string;
}

export interface AccountList {
  accounts: Account[];
  next_cursor: string | null;
}

export interface AccountSearchParams {
  external_ref?: string;
  owner_id?: string;
  currency?: string;
  limit?: number;
  cursor?: string;
}

export interface Balance {
  account_id: string;
  available: Money;
  pending?: Money;
  version: number;
  updated_at: string;
}

export interface BalanceAsOf {
  account_id: string;
  as_of: string;
  balance: Money;
}

export interface StatementLine {
  entry: Entry;
  signed: Money;
  running_balance: Money;
}

export interface Statement {
  account_id: string;
  currency: string;
  from: string;
  to: string;
  opening: Money;
  closing: Money;
  lines: StatementLine[];
  next_cursor: string | null;
}

export type SagaStatus =
  | "PENDING"
  | "RESERVED"
  | "GATEWAY_PENDING"
  | "GATEWAY_SUCCEEDED"
  | "GATEWAY_FAILED"
  | "COMPENSATING"
  | "COMPLETED"
  | "COMPENSATED"
  | "FAILED"
  | "NEEDS_MANUAL_REVIEW";

export type SagaStep = "RESERVE" | "GATEWAY" | "SETTLE" | "DONE";

export interface SagaAttempt {
  step: "RESERVE" | "GATEWAY" | "SETTLE";
  direction: "FORWARD" | "COMPENSATION";
  attempt: number;
  status: "ATTEMPTED" | "SUCCEEDED" | "FAILED";
  transaction_id?: string;
  error?: string;
  started_at: string;
  finished_at?: string;
}

export interface Saga {
  id: string;
  saga_type: string;
  status: SagaStatus;
  current_step: SagaStep;
  retry_count: number;
  last_error?: string;
  created_at: string;
  updated_at: string;
  attempts?: SagaAttempt[];
}

export interface SagaList {
  status: string;
  sagas: Saga[];
}

export type ReconciliationRunStatus = "RUNNING" | "COMPLETED" | "FAILED";

export type ExceptionCategory =
  | "MISSING_IN_LEDGER"
  | "MISSING_IN_PSP"
  | "AMOUNT_MISMATCH"
  | "STATUS_MISMATCH"
  | "TIMING_DIFFERENCE"
  | "DUPLICATE";

export type ExceptionStatus = "OPEN" | "AUTO_RESOLVED" | "RESOLVED";

export interface ReconciliationException {
  id: string;
  external_ref: string;
  category: ExceptionCategory;
  status: ExceptionStatus;
  ledger_transaction_id?: string;
  saga_id?: string;
  ledger_amount_minor?: number;
  psp_amount_minor?: number;
  currency?: string;
  ledger_status?: string;
  psp_status?: string;
  details?: Record<string, unknown>;
  created_at: string;
  resolved_at?: string;
}

export interface ReconciliationRun {
  id: string;
  source: string;
  started_at: string;
  finished_at?: string;
  status: ReconciliationRunStatus;
  psp_row_count: number;
  matched_count: number;
  auto_resolved_count: number;
  exception_count: number;
  error?: string;
  by_category?: Record<string, number>;
  exceptions?: ReconciliationException[];
}

export interface ReconciliationRunList {
  runs: ReconciliationRun[];
}

export interface Problem {
  type: string;
  title: string;
  status: number;
  detail?: string;
  instance?: string;
  request_id?: string;
}

/** Thrown by the API client on a non-2xx response, carrying the parsed problem. */
export class ApiError extends Error {
  readonly status: number;
  readonly problem?: Problem;

  constructor(status: number, problem?: Problem) {
    super(problem?.title ?? `request failed with status ${status}`);
    this.name = "ApiError";
    this.status = status;
    this.problem = problem;
  }
}
