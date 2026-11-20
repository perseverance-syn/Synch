// Live store: follows the `all` SSE stream and folds events into a thread model.
// The thread tree comes from events because GET /v1/threads lists root submissions only;
// children exist only as thread.created events with parent_thread_id (catalogue 2.0.0).
// Follow rules implement openapi.yaml streamEvents: resume from the last contiguous cursor L,
// drop and reconnect on a gap, reconnect on overflow, resync from 0 on a new log epoch or cursor_ahead.
import { useSyncExternalStore } from 'react'
import { bridge, type Event, type StreamFrame } from './api'

/** CheckStatus is one of the five check words; the UI always shows exactly these. */
export type CheckStatus = 'passed' | 'failed' | 'error' | 'partial' | 'not_run'

/** OutputLine is one line of live output, attributed to its thread. */
export type OutputLine = { seq: number; at: string; threadId: string; text: string; tone: 'plain' | 'warn' | 'error' }

/** ThreadModel is everything the event log says about one thread. */
export type ThreadModel = {
  id: string
  projectId: string
  parentId?: string
  role: string
  depth: number
  state: string
  pauseReason?: string
  requestId?: string
  title?: string
  audit?: string
  latest?: string
  checks: Map<string, { status: CheckStatus; reason?: string }>
  output: OutputLine[]
}

/** Live is the store snapshot shared by every view. */
export type Live = {
  threads: Map<string, ThreadModel>
  order: string[] // creation order
  output: OutputLine[] // all threads, newest last
  cursor: number // last contiguous global_seq (L)
  streaming: boolean
}

const MAX_LINES = 1000 // ponytail: ring buffer per thread and globally; page older history from events/page if needed

let live: Live = { threads: new Map(), order: [], output: [], cursor: 0, streaming: false }
let version = 0
const listeners = new Set<() => void>()

// changed bumps the snapshot version and notifies subscribers.
function changed() {
  version++
  listeners.forEach((l) => l())
}

/** useLive subscribes a component to the live store and returns the current snapshot. */
export function useLive(): Live {
  useSyncExternalStore(
    (l) => {
      listeners.add(l)
      return () => listeners.delete(l)
    },
    () => version,
  )
  return live
}

/** onLiveChange registers a plain callback for store changes (used to refresh queries). */
export function onLiveChange(cb: () => void): () => void {
  listeners.add(cb)
  return () => listeners.delete(cb)
}

// --- Fold: one event into the model ---

// thread returns the model for an id, creating a placeholder for events that arrive before thread.created.
function thread(id: string): ThreadModel {
  let t = live.threads.get(id)
  if (!t) {
    t = { id, projectId: '', role: 'coordinator', depth: 0, state: 'draft', checks: new Map(), output: [] }
    live.threads.set(id, t)
    live.order.push(id)
  }
  return t
}

// str narrows an unknown payload field to a string.
const str = (v: unknown): string | undefined => (typeof v === 'string' ? v : undefined)

// say records one output line on the thread and in the global feed and makes it the latest output.
function say(t: ThreadModel, e: Event, text: string, tone: OutputLine['tone'] = 'plain') {
  const line = { seq: e.global_seq, at: e.recorded_at, threadId: t.id, text, tone }
  t.output.push(line)
  if (t.output.length > MAX_LINES) t.output.shift()
  live.output.push(line)
  if (live.output.length > MAX_LINES) live.output.shift()
  t.latest = text.split('\n').filter(Boolean).pop() ?? t.latest
}

/** apply folds one event into the model. Unknown types change nothing but still advance the cursor. */
export function apply(e: Event) {
  const p = e.payload ?? {}
  const id = e.thread_id ?? str(p.thread_id) ?? (e.stream.startsWith('thread:') ? e.stream.slice(7) : '')
  switch (e.type) {
    case 'request.accepted': {
      if (!id) break
      const t = thread(id)
      if (p.command === 'createThread') t.requestId = e.request_id
      else say(t, e, `${String(p.command)} accepted · request ${e.request_id ?? ''}`)
      break
    }
    case 'thread.created': {
      const t = thread(str(p.thread_id) ?? id)
      t.projectId = str(p.project_id) ?? t.projectId
      t.parentId = str(p.parent_thread_id)
      t.role = str(p.role) ?? t.role
      t.depth = typeof p.depth === 'number' ? p.depth : t.depth
      say(t, e, `thread created (${t.role})`)
      break
    }
    case 'thread.state_changed': {
      const t = thread(id)
      t.state = str(p.to) ?? t.state
      t.pauseReason = str(p.pause_reason)
      const why = t.pauseReason ? ` (${t.pauseReason.replaceAll('_', ' ')})` : ''
      const tone = t.state === 'failed' ? 'error' : t.state === 'paused' ? 'warn' : 'plain'
      say(t, e, str(p.detail) ?? `${str(p.from)?.replaceAll("_", " ")} → ${t.state.replaceAll("_", " ")}${why}`, tone)
      break
    }
    case 'child.assignment_recorded': {
      const child = thread(str(p.child_thread_id) ?? '')
      child.title = str(p.purpose)
      child.role = str(p.role) ?? child.role
      child.parentId = str(p.parent_thread_id) ?? child.parentId
      break
    }
    case 'engine.output':
      say(thread(id), e, (str(p.chunk) ?? '').trimEnd(), p.channel === 'stderr' ? 'error' : 'plain')
      break
    case 'check.result': {
      const t = thread(id)
      const status = (str(p.status) ?? 'not_run') as CheckStatus
      t.checks.set(str(p.check) ?? '?', { status, reason: str(p.reason) })
      say(t, e, `check ${str(p.check)}: ${status.replace('_', ' ')}`, status === 'passed' ? 'plain' : 'warn')
      break
    }
    case 'audit.status':
      thread(id).audit = str(p.status)
      break
    default:
      if (id && live.threads.has(id)) say(thread(id), e, e.type)
  }
}

// --- Follow: the `all` stream with resume, gap, overflow and resync rules ---

let handle = 0
let epoch = ''
let retry = 0
let stopped = true
let timer: ReturnType<typeof setTimeout> | undefined

// reset drops all stream-derived state before a resync from cursor 0.
function reset() {
  live = { threads: new Map(), order: [], output: [], cursor: 0, streaming: live.streaming }
  changed()
}

// open starts one SSE connection resuming after the current cursor L.
async function open() {
  clearTimeout(timer)
  if (stopped) return
  handle = await bridge().openStream('all', live.cursor)
}

// reconnect closes the current connection and opens a new one from L after a backoff (0.5 s doubling to 5 s).
function reconnect(delay = Math.min(5000, 500 * 2 ** retry++)) {
  if (handle) void bridge().closeStream(handle)
  handle = 0
  if (live.streaming) {
    live.streaming = false
    changed()
  }
  clearTimeout(timer)
  timer = setTimeout(open, delay)
}

// onFrame handles one frame: hello checks the epoch, and an event must be exactly L+1.
function onFrame(f: StreamFrame) {
  if (f.handle !== handle) return // frame from a connection we already closed
  if (f.kind === 'error') {
    if (f.code === 'cursor_ahead') {
      epoch = ''
      reset()
      return reconnect(0)
    }
    return reconnect()
  }
  if (f.kind === 'end') return reconnect()
  if (f.event === 'hello') {
    const hello = JSON.parse(f.data) as { log_epoch: string; head: number }
    if (epoch && hello.log_epoch !== epoch) {
      // A new log lineage: never jump to head; rebuild from cursor 0.
      epoch = hello.log_epoch
      reset()
      return reconnect(0)
    }
    epoch = hello.log_epoch
    retry = 0
    live.streaming = true
    return changed()
  }
  if (f.event === 'overflow') return reconnect(0)
  const cursor = Number(f.id)
  if (!Number.isInteger(cursor) || cursor <= 0) return // not an event frame
  if (cursor <= live.cursor) return // duplicate: already processed
  if (cursor !== live.cursor + 1) return reconnect(0) // gap: discard the frame, reconnect with L
  try {
    apply(JSON.parse(f.data) as Event)
  } catch {
    // A malformed event still occupies its cursor; moving past it keeps the stream contiguous.
  }
  live.cursor = cursor
  scheduleChanged()
}

let pending = false
// scheduleChanged batches notifications so a replay of thousands of events renders once per frame.
function scheduleChanged() {
  if (pending) return
  pending = true
  requestAnimationFrame(() => {
    pending = false
    changed()
  })
}

/** startFollow begins following the `all` stream; calling it again does nothing. */
export function startFollow() {
  if (!stopped) return
  stopped = false
  bridge().onFrame(onFrame)
  void open()
}

/** reconnectNow retries the stream at once (the banner's Retry button). */
export function reconnectNow() {
  retry = 0
  if (!stopped && !live.streaming) reconnect(0)
}
