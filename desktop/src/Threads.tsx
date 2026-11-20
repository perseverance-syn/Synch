// Threads view (docs/design-reference/threads.png): summary chips, department filter, thread tree
// table, right detail panel with pause box, grant wait, commands, checks and live output, and the
// New task dialog. State, tree, checks and output come from the live event store; titles come from
// the creating receipt (root) or the child assignment purpose; harness/model/waiting_for from GET /v1/threads.
import { useEffect, useLayoutEffect, useRef, useState, type FormEvent } from 'react'
import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError, command, get, type CommandResult, type Receipt, type ThreadView } from './api'
import { onLiveChange, useLive, type CheckStatus, type OutputLine, type ThreadModel } from './store'

// --- Small presentational helpers shared with the shell ---

const ROLE_COLORS: Record<string, string> = {
  coordinator: '#c86bd8',
  ops: '#35b6dc',
  builder: '#97c94a',
  security: '#7c80f2',
  auditor: '#e0a33a',
}

/** roleColor returns the department colour dot for a role. */
export const roleColor = (role: string): string => ROLE_COLORS[role] ?? '#8b8d93'

// cap capitalises a role for chips.
const cap = (s: string) => s[0].toUpperCase() + s.slice(1)

const STATE_PILLS: Record<string, { icon: string; label: string; tone: string }> = {
  draft: { icon: '○', label: 'draft', tone: 'plain' },
  plan_review: { icon: '◇', label: 'plan review', tone: 'amber' },
  queued: { icon: '…', label: 'queued', tone: 'dashed' },
  running: { icon: '●', label: 'running', tone: 'green' },
  waiting_children: { icon: '↳', label: 'waiting on children', tone: 'plain' },
  awaiting_decision: { icon: '?', label: 'awaiting decision', tone: 'amber' },
  paused: { icon: '‖', label: 'paused', tone: 'amber' },
  rollover: { icon: '↻', label: 'rollover', tone: 'violet' },
  verifying: { icon: '◐', label: 'verifying', tone: 'violet' },
  awaiting_acceptance: { icon: '◇', label: 'awaiting acceptance', tone: 'amber' },
  completed: { icon: '✓', label: 'completed', tone: 'plain' },
  failed: { icon: '✕', label: 'failed', tone: 'red' },
  cancelled: { icon: '✕', label: 'cancelled', tone: 'muted' },
}

/** StatePill renders a thread state with its icon; paused threads show the reason ("paused · operator"). */
export function StatePill({ state, reason }: { state: string; reason?: string }) {
  const s = STATE_PILLS[state] ?? { icon: '○', label: state, tone: 'plain' }
  const label = state === 'paused' && reason ? `paused · ${reason.replaceAll('_', ' ')}` : s.label
  return (
    <span className={`pill state ${s.tone}`}>
      <span className="pill-icon">{s.icon}</span>
      {label}
    </span>
  )
}

const EXECUTION: Record<string, string> = { direct: 'Host access', remote: 'Remote', sandbox: 'Sandbox' }
const AUDIT: Record<string, string> = {
  off: 'Audit off',
  pending: 'Audit pending',
  running: 'Audit running',
  pass: 'Audit pass',
  corrections_requested: 'Corrections requested',
  error: 'Audit error',
}

const CHECKS: Record<CheckStatus, { icon: string; label: string; tone: string }> = {
  passed: { icon: '✓', label: 'passed', tone: 'green' },
  failed: { icon: '✕', label: 'failed', tone: 'red' },
  error: { icon: '!', label: 'error', tone: 'red' },
  partial: { icon: '◐', label: 'partial', tone: 'amber' },
  not_run: { icon: '○', label: 'not run', tone: 'plain' },
}

const PAUSE_TEXT: Record<string, string> = {
  operator: 'Paused by an operator. Resume when ready.',
  harness_unavailable: 'The selected harness is unavailable. The thread resumes when it is back, or select another harness.',
  no_eligible_pair: 'No harness and model pair is eligible for this work. Install or qualify a harness, then resume.',
  disk_pressure: 'The daemon paused work because the disk is nearly full. Free space, then resume.',
  reconciliation:
    'The outcome of an external request is unknown. Check the target system, then resolve it. The thread can’t resume until every external request is resolved.',
  disagreement: 'The audit disagrees with the result. Review the findings before resuming.',
  mode_switch: 'An execution mode change is settling. The thread resumes when the switch is active.',
  runner_disconnected: 'The remote runner disconnected. The thread resumes when it reconnects.',
  daemon_recovery: 'The daemon restarted and is recovering interrupted work.',
}

// clock formats an event time as HH:MM:SS.mmm in local time.
const clock = (iso: string) => {
  const d = new Date(iso)
  return Number.isNaN(d.getTime()) ? '' : d.toTimeString().slice(0, 8) + '.' + String(d.getMilliseconds()).padStart(3, '0')
}

// --- Data: tree rows with titles and API views ---

type Row = { t: ThreadModel; view?: ThreadView; title: string; level: number; subtasks: number }

// useThreadViews reads GET /v1/threads and refreshes it shortly after live events arrive.
function useThreadViews(): Map<string, ThreadView> {
  const qc = useQueryClient()
  const q = useQuery({
    queryKey: ['threads'],
    queryFn: () => get<{ threads: ThreadView[] }>('/v1/threads'),
    refetchInterval: 15000,
    retry: false,
  })
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | undefined
    const off = onLiveChange(() => {
      clearTimeout(timer)
      timer = setTimeout(() => void qc.invalidateQueries({ queryKey: ['threads'] }), 300)
    })
    return () => {
      off()
      clearTimeout(timer)
    }
  }, [qc])
  return new Map((q.data?.threads ?? []).map((v) => [v.id, v]))
}

// useTitles looks up root thread titles: the brief in the receipt of the createThread request.
function useTitles(threads: ThreadModel[]): Map<string, string> {
  const roots = threads.filter((t) => !t.title && t.requestId)
  const results = useQueries({
    queries: roots.map((t) => ({
      queryKey: ['receipt', t.requestId],
      queryFn: () => get<Receipt>(`/v1/receipts/${encodeURIComponent(t.requestId ?? '')}`),
      staleTime: Infinity,
      retry: false,
    })),
  })
  const titles = new Map<string, string>()
  roots.forEach((t, i) => {
    const brief = results[i].data?.result?.submission?.brief
    if (brief) titles.set(t.id, brief.split('\n')[0])
  })
  return titles
}

// treeRows orders threads depth-first under their parents and counts each thread's descendants.
function treeRows(threads: ThreadModel[], views: Map<string, ThreadView>, titles: Map<string, string>): Row[] {
  const ids = new Set(threads.map((t) => t.id))
  const kids = new Map<string, ThreadModel[]>()
  const roots: ThreadModel[] = []
  for (const t of threads) {
    if (t.parentId && ids.has(t.parentId)) kids.set(t.parentId, [...(kids.get(t.parentId) ?? []), t])
    else roots.push(t)
  }
  const rows: Row[] = []
  // walk appends a thread and its subtree and returns how many descendants it added.
  const walk = (t: ThreadModel, level: number): number => {
    const title = t.title ?? titles.get(t.id) ?? `Thread ${t.id.slice(-6)}`
    const row: Row = { t, view: views.get(t.id), title, level, subtasks: 0 }
    rows.push(row)
    for (const c of kids.get(t.id) ?? []) row.subtasks += 1 + walk(c, level + 1)
    return row.subtasks
  }
  roots.forEach((r) => walk(r, 0))
  return rows
}

// --- View ---

/** ThreadsView is the Threads page for one project: chips, filters, tree table and detail panel. */
export function ThreadsView({ project, search }: { project: string; search: string }) {
  const live = useLive()
  const views = useThreadViews()
  const threads = live.order.map((id) => live.threads.get(id)!).filter((t) => t.projectId === project)
  const titles = useTitles(threads)
  const all = treeRows(threads, views, titles)
  const [dept, setDept] = useState('all')
  const [selected, setSelected] = useState('')
  const [creating, setCreating] = useState(false)

  const needle = search.trim().toLowerCase()
  const rows = all.filter(
    (r) =>
      (dept === 'all' || r.t.role === dept) &&
      (!needle || `${r.title} ${r.t.id} ${r.t.latest ?? ''} ${r.t.state}`.toLowerCase().includes(needle)),
  )
  const count = (s: string) => threads.filter((t) => t.state === s).length
  const roles = ['coordinator', 'ops', 'builder', 'security', 'auditor'].filter((r) => r !== 'auditor' || threads.some((t) => t.role === r))
  const current = all.find((r) => r.t.id === selected) ?? all[0]

  return (
    <section className="page threads">
      <div className="page-head">
        <h1>Threads</h1>
        <span className="sub mono">
          {threads.length} threads · {live.streaming ? 'live' : 'reconnecting'}
        </span>
        <span className="grow" />
        <span className="pill green">● {count('running')} running</span>
        <span className="pill amber">‖ {count('paused')} paused</span>
        <span className="pill amber">? {count('awaiting_decision')} decision</span>
        <span className="pill amber">◇ {count('awaiting_acceptance')} to accept</span>
        <span className="pill red">✕ {count('failed')} failed</span>
        <button className="btn primary" onClick={() => setCreating(true)}>
          + New task
        </button>
      </div>
      <div className="filters">
        <button className={`chip ${dept === 'all' ? 'on' : ''}`} onClick={() => setDept('all')}>
          <i className="sq" style={{ background: '#8b8d93' }} />
          All <span className="mono">{threads.length}</span>
        </button>
        {roles.map((r) => (
          <button key={r} className={`chip ${dept === r ? 'on' : ''}`} onClick={() => setDept(r)}>
            <i className="sq" style={{ background: roleColor(r) }} />
            {cap(r)} <span className="mono">{threads.filter((t) => t.role === r).length}</span>
          </button>
        ))}
      </div>
      <div className="split">
        <div className="table-wrap">
          <table className="grid">
            <thead>
              <tr>
                <th>Thread</th>
                <th>Dept</th>
                <th>State</th>
                <th>Execution</th>
                <th>Audit</th>
                <th>Latest output</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={r.t.id} className={r.t.id === current?.t.id ? 'selected' : ''} onClick={() => setSelected(r.t.id)}>
                  <td className="title-cell">
                    <span className="tree" style={{ paddingLeft: r.level ? 25 + (r.level - 1) * 25 : 0 }}>
                      {r.level > 0 && <span className="elbow" />}
                      <span className="name">{r.title}</span>
                      {r.subtasks > 0 && (
                        <span className="subtasks">
                          ↳ {r.subtasks} sub-task{r.subtasks > 1 ? 's' : ''}
                        </span>
                      )}
                    </span>
                  </td>
                  <td>
                    <span className="dept mono">
                      <i className="sq" style={{ background: roleColor(r.t.role) }} />
                      {r.t.role}
                    </span>
                  </td>
                  <td>
                    <StatePill state={r.t.state} reason={r.t.pauseReason} />
                  </td>
                  <td>
                    {r.view?.execution ? <span className="pill tag">{EXECUTION[r.view.execution]}</span> : <span className="muted">—</span>}
                  </td>
                  <td>
                    <AuditPill status={r.t.audit ?? r.view?.audit_status} />
                  </td>
                  <td className="latest mono">{r.view?.waiting_for ? 'Waiting for an operator grant' : r.t.latest}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && (
            <div className="empty">
              {threads.length === 0 ? 'No threads in this project yet. Start one with New task.' : 'No threads match.'}
            </div>
          )}
        </div>
        {current && <Detail row={current} streaming={live.streaming} />}
      </div>
      {creating && <NewTask project={project} onClose={() => setCreating(false)} onCreated={(id) => setSelected(id)} />}
    </section>
  )
}

// AuditPill shows the audit status in words; error is red.
function AuditPill({ status }: { status?: string }) {
  if (!status) return <span className="muted">—</span>
  return <span className={`pill tag ${status === 'error' ? 'red-text' : ''}`}>{AUDIT[status] ?? `Audit ${status}`}</span>
}

// --- Detail panel ---

// Detail is the right panel for the selected thread.
function Detail({ row, streaming }: { row: Row; streaming: boolean }) {
  const { t, view } = row
  const qc = useQueryClient()
  const [last, setLast] = useState<{ ok: boolean; text: string } | null>(null)
  useEffect(() => setLast(null), [t.id])
  const send = useMutation({
    mutationFn: (cmd: 'pause' | 'resume' | 'cancel') => command(`/v1/threads/${t.id}/commands`, { command: cmd }),
    onSuccess: (r: CommandResult) => {
      setLast({ ok: true, text: `${r.receipt.command} accepted · receipt ${r.receipt.receipt_id} · request ${r.requestId}` })
      void qc.invalidateQueries({ queryKey: ['threads'] })
    },
    onError: (e: Error) => setLast({ ok: false, text: e instanceof ApiError ? `${e.code}: ${e.message}` : e.message }),
  })
  const terminal = ['completed', 'failed', 'cancelled'].includes(t.state)
  const meta = [view?.harness, view?.model, view?.revision ? `rev ${view.revision.slice(0, 7)}` : '', t.id].filter(Boolean)
  const grant = view?.waiting_for?.match(/`([^`]+)`/)?.[1]
  const checks = [...t.checks]
  const receipt = last && <p className={`receipt mono ${last.ok ? '' : 'error-text'}`}>{last.text}</p>

  return (
    <aside className="detail">
      <div className="detail-head">
        <span className="dept mono">
          <i className="sq" style={{ background: roleColor(t.role) }} />
          {t.role}
        </span>
        <h2>{row.title}</h2>
        <div className="pills">
          <StatePill state={t.state} reason={t.pauseReason} />
          {view?.execution && <span className="pill tag">{EXECUTION[view.execution]}</span>}
          {(t.audit ?? view?.audit_status) && <AuditPill status={t.audit ?? view?.audit_status} />}
        </div>
        <p className="meta mono">{meta.join(' · ')}</p>
      </div>

      {view?.waiting_for && (
        <div className="callout">
          <h3>‖ Waiting for a grant</h3>
          <p>{cap(view.waiting_for.replace(/:?\s*run `[^`]+`/, ''))}. Run:</p>
          {grant && (
            <div className="cmd">
              <code className="mono">{grant}</code>
              <button className="btn small" onClick={() => void navigator.clipboard.writeText(grant)}>
                Copy
              </button>
            </div>
          )}
        </div>
      )}

      {t.state === 'paused' && (
        <div className="callout">
          <h3>‖ Paused for {t.pauseReason?.replaceAll('_', ' ') ?? 'unknown reason'}</h3>
          <p>{PAUSE_TEXT[t.pauseReason ?? ''] ?? 'The thread is paused.'}</p>
          <div className="actions">
            <button className="btn primary" disabled={send.isPending} onClick={() => send.mutate('resume')}>
              Resume
            </button>
            <button className="btn danger" disabled={send.isPending} onClick={() => send.mutate('cancel')}>
              Cancel thread
            </button>
          </div>
          {receipt}
        </div>
      )}
      {t.state !== 'paused' && !terminal && (
        <div className="actions pad-x">
          <button className="btn" disabled={send.isPending} onClick={() => send.mutate('pause')}>
            Pause
          </button>
          <button className="btn danger" disabled={send.isPending} onClick={() => send.mutate('cancel')}>
            Cancel thread
          </button>
        </div>
      )}
      {t.state !== 'paused' && <div className="pad-x">{receipt}</div>}

      <h6 className="section">Checks</h6>
      <div className="checks">
        {checks.length === 0 && <p className="muted small">No checks recorded yet.</p>}
        {checks.map(([name, c]) => (
          <div className="check" key={name}>
            <div className="check-row">
              <span className="mono">{name}</span>
              <span className={`pill ${CHECKS[c.status]?.tone ?? 'plain'}`}>
                {CHECKS[c.status]?.icon} {CHECKS[c.status]?.label ?? c.status}
              </span>
            </div>
            {c.reason && <p className="muted small">{c.reason}</p>}
          </div>
        ))}
      </div>

      <LiveOutput lines={t.output} streaming={streaming} />
    </aside>
  )
}

/** LiveOutput shows streamed lines with a Following toggle that keeps the view pinned to the newest line. */
export function LiveOutput({ lines, streaming, tall }: { lines: OutputLine[]; streaming: boolean; tall?: boolean }) {
  const [following, setFollowing] = useState(true)
  const box = useRef<HTMLDivElement>(null)
  const last = lines[lines.length - 1]?.seq
  useLayoutEffect(() => {
    if (following && box.current) box.current.scrollTop = box.current.scrollHeight
  }, [following, last, lines])
  return (
    <div className={`live ${tall ? 'tall' : ''}`}>
      <div className="live-head">
        <h3>Live output</h3>
        <span className={`pill ${streaming ? 'green' : 'amber'}`}>● {streaming ? 'streaming' : 'reconnecting'}</span>
        <span className="grow" />
        <button className={`btn small ${following ? 'on' : ''}`} aria-pressed={following} onClick={() => setFollowing(!following)}>
          {following ? 'Following' : 'Follow'}
        </button>
      </div>
      <div
        className="live-body mono"
        ref={box}
        onWheel={(e) => {
          // Scrolling up to read history stops following, like a terminal.
          if (e.deltaY < 0) setFollowing(false)
        }}
      >
        {lines.length === 0 && <p className="muted">Waiting for output…</p>}
        {lines.map((l) => (
          <div className={`line ${l.tone}`} key={`${l.threadId}-${l.seq}`}>
            <span className="ts">{clock(l.at)}</span>
            <span className="text">{l.text}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// --- New task ---

// NewTask is the dialog that submits a brief through POST /v1/threads (createThread).
function NewTask({ project, onClose, onCreated }: { project: string; onClose: () => void; onCreated: (id: string) => void }) {
  const ref = useRef<HTMLDialogElement>(null)
  const live = useLive()
  const [form, setForm] = useState({ project, brief: '', criteria: '', review: false })
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  useEffect(() => ref.current?.showModal(), [])
  const projects = [...new Set([...live.threads.values()].map((t) => t.projectId).filter(Boolean))]

  // submit sends the brief with a fresh request id; the thread then appears through the live stream.
  async function submit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError('')
    try {
      const criteria = form.criteria
        .split('\n')
        .map((c) => c.trim())
        .filter(Boolean)
      const r = await command('/v1/threads', {
        project_id: form.project.trim(),
        brief: form.brief.trim(),
        ...(criteria.length ? { criteria } : {}),
        review_plan_first: form.review,
      })
      const id = r.receipt.thread_id ?? r.receipt.result?.thread_id
      if (id) onCreated(id)
      onClose()
    } catch (err) {
      setError(err instanceof ApiError ? `${err.code}: ${err.message}` : String(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <dialog ref={ref} className="dialog" onClose={onClose}>
      <form onSubmit={submit}>
        <h2>New task</h2>
        <label>
          Project
          <input className="mono" list="projects" value={form.project} onChange={(e) => setForm({ ...form, project: e.target.value })} required />
          <datalist id="projects">
            {projects.map((p) => (
              <option key={p} value={p} />
            ))}
          </datalist>
        </label>
        <label>
          Brief
          <textarea rows={5} value={form.brief} onChange={(e) => setForm({ ...form, brief: e.target.value })} required autoFocus />
        </label>
        <label>
          <span>
            Acceptance criteria <span className="muted">(one per line)</span>
          </span>
          <textarea rows={3} value={form.criteria} onChange={(e) => setForm({ ...form, criteria: e.target.value })} />
        </label>
        <label className="check-label">
          <input type="checkbox" checked={form.review} onChange={(e) => setForm({ ...form, review: e.target.checked })} />
          Review the plan before work starts
        </label>
        {error && <p className="error-text">{error}</p>}
        <div className="actions end">
          <button type="button" className="btn" onClick={onClose}>
            Cancel
          </button>
          <button className="btn primary" disabled={busy || !form.brief.trim() || !form.project.trim()}>
            {busy ? 'Submitting…' : 'Submit task'}
          </button>
        </div>
      </form>
    </dialog>
  )
}
