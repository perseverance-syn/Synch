// Diffs, Machines and Settings views (cards 202b, 202c). Diffs read GET /v1/threads/{id}/diff and
// GET /v1/outputs/{ref}/search; Machines read GET /v1/execution-drivers; Settings read GET /v1/health.
import { useState, type FormEvent } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ApiError, bridge, get, type Diff, type ExecutionDriver, type Health, type ThreadView } from './api'
import { useLive } from './store'
import { EXECUTION, StatePill, roleColor } from './Threads'

// short abbreviates a digest or id for display.
const short = (s?: string) => (!s ? '—' : s.length > 22 ? `${s.slice(0, 14)}…${s.slice(-4)}` : s)

// useThreads reads GET /v1/threads (shared cache key with the Threads view).
function useThreads(): ThreadView[] {
  const q = useQuery({ queryKey: ['threads'], queryFn: () => get<{ threads: ThreadView[] }>('/v1/threads'), refetchInterval: 15000, retry: false })
  return q.data?.threads ?? []
}

// patchClass colours one unified-diff line: added, removed, hunk header or file header.
function patchClass(line: string): string {
  if (line.startsWith('+++') || line.startsWith('---') || line.startsWith('diff ') || line.startsWith('index ')) return 'diff-file'
  if (line.startsWith('@@')) return 'diff-hunk'
  if (line.startsWith('+')) return 'diff-add'
  if (line.startsWith('-')) return 'diff-del'
  return ''
}

/** DiffsView lists the threads and shows the selected thread's worktree diff with a retained-output search. */
export function DiffsView() {
  // GET /v1/threads lists root submissions only, so the thread tree (children hold the worktrees) comes from the live store.
  const live = useLive()
  const threads = live.order.map((id) => live.threads.get(id)!)
  const [sel, setSel] = useState('')
  const cur = threads.find((t) => t.id === sel) ?? threads.find((t) => t.parentId) ?? threads[0]
  return (
    <section className="page">
      <div className="page-head">
        <h1>Diffs</h1>
        <span className="sub mono">worktree changes since the thread base</span>
      </div>
      <div className="split">
        <div className="table-wrap">
          <table className="grid">
            <thead>
              <tr>
                <th>Thread</th>
                <th>Role</th>
                <th>State</th>
              </tr>
            </thead>
            <tbody>
              {threads.map((t) => (
                <tr key={t.id} className={t.id === cur?.id ? 'selected' : ''} onClick={() => setSel(t.id)}>
                  <td>
                    <span className="name">{t.title ?? `Thread ${t.id.slice(-6)}`}</span> <span className="mono dim small">{short(t.id)}</span>
                  </td>
                  <td className="dept">
                    <i className="sq" style={{ background: roleColor(t.role) }} />
                    {t.role}
                  </td>
                  <td>
                    <StatePill state={t.state} reason={t.pauseReason} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {threads.length === 0 && <p className="empty">No threads yet.</p>}
        </div>
        {cur ? <DiffPanel key={cur.id} threadId={cur.id} /> : <aside className="detail" />}
      </div>
    </section>
  )
}

// DiffPanel shows one thread's diff; a thread with no worktree is a 404, shown as an empty state.
function DiffPanel({ threadId }: { threadId: string }) {
  const q = useQuery({ queryKey: ['diff', threadId], queryFn: () => get<Diff>(`/v1/threads/${threadId}/diff`), retry: false, refetchInterval: 15000 })
  const d = q.data
  if (q.error instanceof ApiError && q.error.status === 404) {
    return (
      <aside className="detail">
        <p className="empty">This thread has no worktree, so there is no diff.</p>
      </aside>
    )
  }
  if (q.isError) {
    return (
      <aside className="detail">
        <p className="empty error-text">{q.error instanceof ApiError ? `${q.error.code}: ${q.error.message}` : 'Diff could not be loaded.'}</p>
      </aside>
    )
  }
  if (!d) {
    return (
      <aside className="detail">
        <p className="empty">Loading diff…</p>
      </aside>
    )
  }
  return (
    <aside className="detail diff-panel">
      <div className="detail-head">
        <p className="meta mono">
          base {d.base.slice(0, 12)} → revision {d.revision.slice(0, 12)} · {d.bytes} bytes · {short(d.digest)}
        </p>
      </div>
      {d.truncated && (
        <div className="callout">
          <h3>Patch capped</h3>
          <p>
            Only the first part is shown. The full redacted diff ({d.bytes} bytes) is retained as <code className="mono">{d.ref}</code>; search it below.
          </p>
        </div>
      )}
      <Search threadId={threadId} reference={d.ref} />
      <pre className="diff mono small">
        {d.patch === '' && <span className="muted">No tracked changes.</span>}
        {d.patch.split('\n').map((line, i) => (
          <div key={i} className={patchClass(line)}>
            {line || ' '}
          </div>
        ))}
      </pre>
    </aside>
  )
}

// Search runs a RE2 pattern over the retained full diff with GET /v1/outputs/{ref}/search.
function Search({ threadId, reference }: { threadId: string; reference: string }) {
  const [pattern, setPattern] = useState('')
  const [result, setResult] = useState<{ matches: { line: number; text: string }[]; truncated: boolean } | { error: string } | null>(null)
  // run sends the pattern; an invalid pattern comes back as a 400 problem and is shown as is.
  async function run(e: FormEvent) {
    e.preventDefault()
    if (!pattern) return
    try {
      setResult(await get(`/v1/outputs/${reference}/search?thread=${threadId}&pattern=${encodeURIComponent(pattern)}`))
    } catch (err) {
      setResult({ error: err instanceof ApiError ? `${err.code}: ${err.message}` : String(err) })
    }
  }
  return (
    <form className="pad-x" onSubmit={run}>
      <input className="mono" value={pattern} onChange={(e) => setPattern(e.target.value)} placeholder="Search the retained diff (regular expression)" aria-label="Search the retained diff" />
      {result && 'error' in result && <p className="error-text small">{result.error}</p>}
      {result && 'matches' in result && (
        <div className="search-results mono small">
          {result.matches.length === 0 && <p className="muted">No matching lines.</p>}
          {result.matches.map((m) => (
            <div key={m.line}>
              <span className="muted">{m.line}</span> {m.text}
            </div>
          ))}
          {result.truncated && <p className="muted">More matches exist; refine the pattern.</p>}
        </div>
      )}
    </form>
  )
}

/** MachinesView lists the registered execution drivers and the execution mode of every thread. */
export function MachinesView() {
  const threads = useThreads()
  const drivers = useQuery({ queryKey: ['drivers'], queryFn: () => get<{ drivers: ExecutionDriver[] }>('/v1/execution-drivers'), refetchInterval: 15000, retry: false })
  const rows = drivers.data?.drivers ?? []
  const modes = Object.keys(EXECUTION).map((m) => [m, threads.filter((t) => t.execution === m).length] as const)
  return (
    <section className="page">
      <div className="page-head">
        <h1>Machines</h1>
        <span className="sub mono">
          {rows.length} execution driver{rows.length === 1 ? '' : 's'}
        </span>
      </div>
      <h6 className="section pad">Execution drivers</h6>
      <div className="table-wrap fit">
        <table className="grid">
          <thead>
            <tr>
              <th>Kind</th>
              <th>Description</th>
              <th>Runner status</th>
              <th>Evidence integrity</th>
              <th>Certified configurations</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((d) => (
              <tr key={d.kind}>
                <td className="mono strong">{d.kind}</td>
                <td>{d.description}</td>
                <td>
                  <span className={`pill ${d.status === 'available' || d.status === 'ready' || d.status === 'connected' ? 'green' : 'amber'}`}>● {d.status.replaceAll('_', ' ')}</span>
                </td>
                <td className="mono dim small">{d.evidence_integrity.replaceAll('_', ' ')}</td>
                <td className="mono dim small">{d.certified_configs.length ? d.certified_configs.join(', ') : 'none'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length === 0 && <p className="empty">{drivers.isError ? 'Execution drivers could not be loaded.' : 'No execution drivers are registered.'}</p>}
      </div>
      <h6 className="section pad">Execution in use</h6>
      <div className="pills pad modes">
        {modes.map(([m, n]) => (
          <span key={m} className="pill">
            {EXECUTION[m]}{' '}
            <span className="mono muted">
              {n} thread{n === 1 ? '' : 's'}
            </span>
          </span>
        ))}
      </div>
    </section>
  )
}

/** SettingsView shows the daemon URL, connection status, app version and the change-token action. */
export function SettingsView({ url }: { url: string }) {
  const health = useQuery({ queryKey: ['health'], queryFn: () => get<Health>('/v1/health'), refetchInterval: 5000, retry: false })
  const version = useQuery({ queryKey: ['version'], queryFn: () => bridge().getVersion(), staleTime: Infinity })
  const live = useLive()
  const status = health.data ? (health.data.status === 'ok' && !health.data.disk_pressure ? 'Connected' : 'Connected, degraded') : health.isError ? 'Not connected' : 'Connecting'
  return (
    <section className="page">
      <div className="page-head">
        <h1>Settings</h1>
      </div>
      <div className="panel-box settings-grid">
        <div className="kv">
          <span>Daemon URL</span>
          <div className="mono">{url}</div>
        </div>
        <div className="kv">
          <span>Connection</span>
          <div>
            <span className={`pill ${health.data ? 'green' : 'red'}`}>● {status}</span>{' '}
            {health.data && (
              <span className="mono muted small">
                api {health.data.api_version} · head seq {health.data.head_global_seq} · event stream {live.streaming ? 'following' : 'reconnecting'}
              </span>
            )}
          </div>
        </div>
        <div className="kv">
          <span>Token</span>
          <div className="settings-row">
            <span className="muted">Encrypted with OS secure storage (Electron safeStorage); never shown.</span>
            <button className="btn" onClick={() => void bridge().forgetToken().then(() => location.reload())}>
              Change token
            </button>
          </div>
        </div>
        <div className="kv">
          <span>App version</span>
          <div className="mono">{version.data ?? '…'}</div>
        </div>
      </div>
      <p className="muted pad small">Closing the desktop leaves the daemon running.</p>
    </section>
  )
}
