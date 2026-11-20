// API client for the renderer. Every call goes through the preload bridge to the main process,
// which adds the bearer token; this module adds request ids for commands and typed errors.

/** Raw response passed back by the main process (or an unreachable marker). */
type RawResponse =
  | { unreachable: true; message: string }
  | { unreachable?: false; status: number; body: string; receiptId: string | null; replayed: boolean }

/** One SSE frame or terminal notice forwarded by the main process. */
export type StreamFrame =
  | { handle: number; kind: 'frame'; event: string; id: string; data: string }
  | { handle: number; kind: 'end' }
  | { handle: number; kind: 'error'; status: number; code: string }

/** The window.synch bridge defined in preload.cjs. */
type Bridge = {
  getSettings(): Promise<{ url: string; hasToken: boolean }>
  saveSettings(url: string, token: string): Promise<void>
  forgetToken(): Promise<void>
  request(req: { method: string; path: string; body?: unknown; requestId?: string }): Promise<RawResponse>
  openStream(stream: string, after: number): Promise<number>
  closeStream(handle: number): Promise<void>
  onFrame(cb: (frame: StreamFrame) => void): () => void
}

declare global {
  interface Window {
    synch: Bridge
  }
}

/** bridge returns the preload bridge, the only way the renderer reaches the daemon. */
export const bridge = (): Bridge => window.synch

/** Problem is an RFC 9457 body with the API's stable code. */
export type Problem = { title?: string; status: number; code: string; detail?: string }

/** ApiError is thrown for an unreachable daemon (status 0) or a problem+json answer. */
export class ApiError extends Error {
  // constructor keeps the HTTP status and problem code next to a readable message.
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
  ) {
    super(message)
  }
}

/** unreachable reports whether an error means the daemon could not be reached at all. */
export const unreachable = (err: unknown): boolean => err instanceof ApiError && err.status === 0

// parse turns a raw response into JSON or throws an ApiError carrying the problem code.
function parse<T>(raw: RawResponse): { data: T; replayed: boolean } {
  if (raw.unreachable) throw new ApiError(0, 'unreachable', `Daemon unreachable (${raw.message})`)
  let data: unknown = null
  try {
    data = raw.body ? JSON.parse(raw.body) : null
  } catch {
    data = null
  }
  if (raw.status >= 400) {
    const p = (data ?? {}) as Partial<Problem>
    throw new ApiError(raw.status, p.code ?? 'http_error', p.detail || p.title || `HTTP ${raw.status}`)
  }
  return { data: data as T, replayed: raw.replayed }
}

/** get reads one JSON resource; TanStack Query fetchers call it. */
export async function get<T>(path: string): Promise<T> {
  return parse<T>(await bridge().request({ method: 'GET', path })).data
}

/** newRequestId makes a fresh Synch-Request-Id; the receipt makes a retry with the same id safe. */
export function newRequestId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(12))
  return 'desk-' + Array.from(bytes, (b) => b.toString(16).padStart(2, '0')).join('')
}

/** Receipt is the stored outcome of an accepted command (contracts/receipts.md). */
export type Receipt = {
  receipt_id: string
  request_id: string
  command: string
  status: 'accepted' | 'rejected'
  http_status: number
  thread_id?: string
  event_seq?: number
  result?: { thread_id?: string; submission?: { brief?: string } }
}

/** CommandResult is what the UI shows after a command: the receipt plus its request id. */
export type CommandResult = { receipt: Receipt; requestId: string; replayed: boolean }

/** command POSTs one mutating call with a new request id and returns the receipt. */
export async function command(path: string, body?: unknown): Promise<CommandResult> {
  const requestId = newRequestId()
  const res = parse<Receipt>(await bridge().request({ method: 'POST', path, body, requestId }))
  return { receipt: res.data, requestId, replayed: res.replayed }
}

// --- API shapes the desktop reads (contracts/api/openapi.yaml) ---

/** Health is the daemon status from GET /v1/health. */
export type Health = { status: 'ok' | 'degraded'; api_version: string; log_epoch: string; head_global_seq: number; disk_pressure: boolean }

/** ThreadView is one thread from GET /v1/threads, including the derived waiting_for note (card 113). */
export type ThreadView = {
  id: string
  project_id: string
  role: string
  state: string
  depth: number
  pause_reason?: string
  revision?: string
  plan_digest?: string
  harness?: string
  model?: string
  execution?: 'direct' | 'remote' | 'sandbox'
  audit_status?: string
  waiting_for?: string
  last_seq: number
}

/** Event is one persisted event envelope (events/catalogue.schema.json). */
export type Event = {
  id: string
  stream: string
  seq: number
  global_seq: number
  type: string
  recorded_at: string
  thread_id?: string
  request_id?: string
  payload: Record<string, unknown>
}
