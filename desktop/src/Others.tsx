// Diffs, Machines and Settings views (card 202b). Each shows what the API and event stream expose and
// names the missing API where they do not; nothing here is invented.
import { useQuery } from '@tanstack/react-query'
import { bridge, get, type Health, type ThreadView } from './api'
import { useLive } from './store'
import { EXECUTION, StatePill, roleColor } from './Threads'

// short abbreviates a digest or id for display.
const short = (s?: string) => (!s ? '—' : s.length > 22 ? `${s.slice(0, 14)}…${s.slice(-4)}` : s)

// useThreads reads GET /v1/threads (shared cache key with the Threads view).
function useThreads(): ThreadView[] {
  const q = useQuery({ queryKey: ['threads'], queryFn: () => get<{ threads: ThreadView[] }>('/v1/threads'), refetchInterval: 15000, retry: false })
  return q.data?.threads ?? []
}

// Gap is the styled empty state that names a missing API.
function Gap({ title, children }: { title: string; children: string }) {
  return (
    <div className="gap-box">
      <div className="coming-icon">◇</div>
      <h2>{title}</h2>
      <p className="muted">{children}</p>
    </div>
  )
}

/** DiffsView lists each thread's revision and worktree digest; diff content is not exposed by the API yet. */
export function DiffsView() {
  const threads = useThreads()
  return (
    <section className="page">
      <div className="page-head">
        <h1>Diffs</h1>
        <span className="sub mono">revision and worktree digest per thread</span>
      </div>
      <div className="table-wrap fit">
        <table className="grid">
          <thead>
            <tr>
              <th>Thread</th>
              <th>Role</th>
              <th>State</th>
              <th>Revision</th>
              <th>Worktree digest</th>
              <th>Plan digest</th>
            </tr>
          </thead>
          <tbody>
            {threads.map((t) => (
              <tr key={t.id}>
                <td className="mono">{short(t.id)}</td>
                <td className="dept">
                  <i className="sq" style={{ background: roleColor(t.role) }} />
                  {t.role}
                </td>
                <td>
                  <StatePill state={t.state} reason={t.pause_reason} />
                </td>
                <td className="mono">{t.revision ? t.revision.slice(0, 12) : '—'}</td>
                <td className="mono dim small">{short(t.worktree_digest)}</td>
                <td className="mono dim small">{short(t.plan_digest)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {threads.length === 0 && <p className="empty">No threads yet.</p>}
      </div>
      <Gap title="Diff content is not exposed by the API">
        The daemon returns each thread's revision and worktree digest, but no endpoint or event carries the changed files or patch text (open question: a read endpoint such as GET /v1/threads/:id/diff). The view will render it here when it exists.
      </Gap>
    </section>
  )
}

/** MachinesView shows remote runners from runner.connection events and the execution mode of every thread. */
export function MachinesView() {
  const live = useLive()
  const threads = useThreads()
  const runners = [...live.runners.values()].sort((a, b) => a.name.localeCompare(b.name))
  const modes = Object.keys(EXECUTION).map((m) => [m, threads.filter((t) => t.execution === m).length] as const)
  return (
    <section className="page">
      <div className="page-head">
        <h1>Machines</h1>
        <span className="sub mono">
          {runners.length} remote runner{runners.length === 1 ? '' : 's'}
        </span>
      </div>
      <h6 className="section pad">Remote runners</h6>
      <div className="table-wrap fit">
        <table className="grid">
          <thead>
            <tr>
              <th>Runner</th>
              <th>Connection</th>
              <th>Last change</th>
              <th>Replayed journal range</th>
            </tr>
          </thead>
          <tbody>
            {runners.map((r) => (
              <tr key={r.name}>
                <td className="mono strong">{r.name}</td>
                <td>
                  <span className={`pill ${r.state === 'connected' ? 'green' : 'amber'}`}>● {r.state}</span>
                </td>
                <td className="mono dim small">{r.at.replace(/.d+Z$/, "Z")}</td>
                <td className="mono dim small">{r.replayed ? `${r.replayed[0]} → ${r.replayed[1]}` : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {runners.length === 0 && <p className="empty">No runner has connected. Runners appear when the daemon records runner.connection.</p>}
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
      <Gap title="Execution drivers are not listed by the API">
        No endpoint returns the registered execution drivers (direct host, remote runner, sandbox VM) or a runner's capabilities (open question: GET /v1/machines). Connection state above comes from the event stream.
      </Gap>
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
