// App shell shared by every view (card 202a; 202b reuses it): connect screen on first run,
// top bar (project crumb, Ctrl K search, daemon health pill, global seq), left rail
// (projects, views, departments) and view routing. Daemon-down shows a banner; the app stays usable.
import { useEffect, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError, bridge, get, unreachable, type Health } from './api'
import { reconnectNow, startFollow, useLive } from './store'
import { ThreadsView, LiveOutput, roleColor } from './Threads'
import { ApprovalsView } from './Approvals'
import { FindingsView } from './Findings'
import { DiffsView, MachinesView, SettingsView } from './Others'

/** View is one entry of the rail's Views list. */
export type View = 'Threads' | 'Live output' | 'Diffs' | 'Approvals' | 'Findings' | 'Machines' | 'Settings'
const VIEWS: View[] = ['Threads', 'Live output', 'Diffs', 'Approvals', 'Findings', 'Machines', 'Settings']
const ROLES = ['coordinator', 'ops', 'builder', 'security', 'auditor']

/** App picks the connect screen or the shell depending on whether a token is stored. */
export function App() {
  const settings = useQuery({ queryKey: ['settings'], queryFn: () => bridge().getSettings(), staleTime: Infinity })
  if (!settings.data) return null
  if (!settings.data.hasToken) return <Connect url={settings.data.url} />
  return <Shell url={settings.data.url} />
}

// --- Connect screen (first run, or after "Change token") ---

/** Connect takes the daemon URL and token, stores them with safeStorage and checks them against /v1/health. */
export function Connect({ url }: { url: string }) {
  const qc = useQueryClient()
  const [addr, setAddr] = useState(url)
  const [token, setToken] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)

  // submit saves the settings, then verifies the token; a rejected token is forgotten again.
  async function submit(e: FormEvent) {
    e.preventDefault()
    setBusy(true)
    setError('')
    try {
      await bridge().saveSettings(addr.trim(), token.trim())
      try {
        await get<Health>('/v1/health')
      } catch (err) {
        if (err instanceof ApiError && err.status === 401) {
          await bridge().forgetToken()
          throw new Error('The daemon rejected this token. Check SYNCH_TOKEN and try again.')
        }
        // Unreachable is fine here: the shell shows the daemon-down banner and keeps retrying.
      }
      setToken('')
      await qc.invalidateQueries()
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      setError(msg.replace(/^Error invoking remote method '[^']+': (Error: )?/, ''))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="connect">
      <form className="connect-card" onSubmit={submit}>
        <div className="brand">
          <span className="logo">S</span> Synch
        </div>
        <h1>Connect to your daemon</h1>
        <p className="muted">
          The desktop talks to the local <code>synch daemon</code> through the same authenticated API as the CLI. Closing the
          desktop leaves the daemon running.
        </p>
        <label>
          Daemon URL
          <input className="mono" value={addr} onChange={(e) => setAddr(e.target.value)} spellCheck={false} />
        </label>
        <label>
          Token
          <input
            className="mono"
            type="password"
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="value of SYNCH_TOKEN"
            autoFocus
            required
          />
        </label>
        <p className="hint">Stored encrypted by your OS (Electron safeStorage). Never written in plain text or logged.</p>
        {error && <p className="error-text">{error}</p>}
        <button className="btn primary wide" disabled={busy || !token.trim()}>
          {busy ? 'Connecting…' : 'Connect'}
        </button>
      </form>
    </div>
  )
}

// --- Shell ---

/** Shell renders the top bar, rail, banner and the selected view. */
export function Shell({ url }: { url: string }) {
  const live = useLive()
  const [view, setView] = useState<View>('Threads')
  const [search, setSearch] = useState('')
  const [project, setProject] = useState('')
  const searchRef = useRef<HTMLInputElement>(null)

  useEffect(startFollow, [])

  // Ctrl K (or Cmd K) focuses search, as in the design.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.ctrlKey || e.metaKey) && e.key.toLowerCase() === 'k') {
        e.preventDefault()
        searchRef.current?.focus()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [])

  const health = useQuery({ queryKey: ['health'], queryFn: () => get<Health>('/v1/health'), refetchInterval: 5000, retry: false })

  // live is mutated in place by the store, so derived values are computed on each render.
  const counts = new Map<string, number>()
  for (const t of live.threads.values()) if (t.projectId) counts.set(t.projectId, (counts.get(t.projectId) ?? 0) + 1)
  const projects = [...counts].sort((a, b) => a[0].localeCompare(b[0]))
  // Default to the busiest project, as the design opens on the main one.
  const current = project || [...projects].sort((a, b) => b[1] - a[1])[0]?.[0] || ''

  const roleCounts = new Map<string, number>()
  for (const t of live.threads.values()) if (t.projectId === current) roleCounts.set(t.role, (roleCounts.get(t.role) ?? 0) + 1)

  // Rail badges: waiting approvals and grouped findings.
  const badge: Partial<Record<View, number>> = {
    Approvals: live.approvalRequests.filter((r) => r.status === 'pending').length,
    Findings: live.findings.size,
  }

  const down = health.isError && unreachable(health.error)
  const rejected = health.error instanceof ApiError && health.error.status === 401
  const seq = Math.max(live.cursor, health.data?.head_global_seq ?? 0)

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">S</span> Synch <span className="crumb">/</span>
          <span className="crumb-project">{current || 'no project'}</span>
        </div>
        <div className="top-right">
          <label className="search">
            <SearchIcon />
            <input ref={searchRef} value={search} onChange={(e) => setSearch(e.target.value)} placeholder="Search threads, run a command" />
            <kbd>Ctrl K</kbd>
          </label>
          <HealthPill health={health.data} down={down} rejected={rejected} />
          <span className="seq mono">
            seq <b>{seq.toLocaleString('en-US').replaceAll(',', ' ')}</b>
          </span>
        </div>
      </header>
      <nav className="rail">
        <h6>Projects</h6>
        {projects.length === 0 && <p className="rail-empty">No projects yet</p>}
        {projects.map(([name, n]) => (
          <button key={name} className={`rail-item ${name === current ? 'active strong' : ''}`} onClick={() => setProject(name)}>
            <span>{name}</span>
            <span className="count">{n}</span>
          </button>
        ))}
        <h6>Views</h6>
        {VIEWS.map((v) => (
          <button key={v} className={`rail-item ${v === view ? 'active' : ''}`} onClick={() => setView(v)}>
            {v}
            {badge[v] ? <span className={v === 'Approvals' ? 'badge amber' : 'badge'}>{badge[v]}</span> : null}
          </button>
        ))}
        <h6>Departments</h6>
        {ROLES.filter((r) => r !== 'auditor' || roleCounts.has(r)).map((r) => (
          <div key={r} className="rail-item static">
            <span>
              <i className="dot" style={{ background: roleColor(r) }} />
              {r[0].toUpperCase() + r.slice(1)}
            </span>
            <span className="count">{roleCounts.get(r) ?? 0}</span>
          </div>
        ))}
      </nav>
      <main className="content">
        {(down || rejected) && (
          <Banner>
            {rejected ? (
              <>
                <span>
                  The daemon at <code>{url}</code> rejected the stored token.
                </span>
                <span className="grow" />
                <button className="btn" onClick={() => void bridge().forgetToken().then(() => location.reload())}>
                  Change token
                </button>
              </>
            ) : (
              <>
                <span>
                  Can't reach the daemon at <code>{url}</code>. Showing the last known state; retrying every 5 s.
                </span>
                <span className="grow" />
                <button
                  className="btn"
                  onClick={() => {
                    void health.refetch()
                    reconnectNow()
                  }}
                >
                  Retry now
                </button>
              </>
            )}
          </Banner>
        )}
        {view === 'Threads' && <ThreadsView project={current} search={search} />}
        {view === 'Live output' && (
          <section className="page">
            <div className="page-head">
              <h1>Live output</h1>
              <span className="sub mono">all threads · {live.output.length} lines</span>
            </div>
            <LiveOutput lines={live.output} streaming={live.streaming} tall />
          </section>
        )}
        {view === 'Approvals' && <ApprovalsView />}
        {view === 'Findings' && <FindingsView />}
        {view === 'Diffs' && <DiffsView />}
        {view === 'Machines' && <MachinesView />}
        {view === 'Settings' && <SettingsView url={url} />}
      </main>
    </div>
  )
}

// HealthPill shows the daemon state from /v1/health: OK, degraded (disk pressure), down or unauthorized.
function HealthPill({ health, down, rejected }: { health?: Health; down: boolean; rejected: boolean }) {
  if (rejected) return <span className="pill red">● Token rejected</span>
  if (down) return <span className="pill red">● Daemon down</span>
  if (!health) return <span className="pill muted">● Connecting</span>
  if (health.status !== 'ok' || health.disk_pressure)
    return <span className="pill amber">● Daemon {health.disk_pressure ? 'disk pressure' : 'degraded'}</span>
  return <span className="pill green">● Daemon OK</span>
}

// Banner is the full-width warning strip above the current view.
function Banner({ children }: { children: ReactNode }) {
  return (
    <div className="banner" role="alert">
      <span className="banner-icon">!</span>
      {children}
    </div>
  )
}

// SearchIcon is the magnifier in the search box.
function SearchIcon() {
  return (
    <svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.6" aria-hidden>
      <circle cx="7" cy="7" r="5" />
      <path d="m11 11 3.5 3.5" />
    </svg>
  )
}
