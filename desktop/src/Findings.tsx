// Findings view (docs/design-reference/findings.png): severity chips, findings table and a detail panel.
// Rows come from GET /v1/findings (grouped by the daemon); the severity and status chips are the
// `severity` and `status` query parameters. Triage (confirm, false positive, accepted risk) has no write
// endpoint, so those buttons stay disabled; "Create fix thread" uses POST /v1/threads.
import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError, command, get, type CommandResult, type FindingGroup } from './api'
import { useLive } from './store'

const SEVERITIES = ['critical', 'high', 'medium', 'low', 'info'] as const
const STATUSES = ['open', 'confirmed', 'fixed', 'false_positive', 'accepted_risk'] as const
const SEV_ICON: Record<string, string> = { critical: '▲▲', high: '▲', medium: '●', low: '▽', info: 'i' }
const SEV_TONE: Record<string, string> = { critical: 'red', high: 'amber', medium: 'amber', low: 'blue', info: 'plain', unknown: 'plain' }
const FIX: Record<string, { label: string; tone: string }> = {
  open: { label: 'open', tone: 'amber' },
  confirmed: { label: 'confirmed', tone: 'red' },
  fixed: { label: 'fixed', tone: 'green' },
  false_positive: { label: 'false positive', tone: 'plain' },
  accepted_risk: { label: 'accepted risk', tone: 'violet' },
}

// short abbreviates a digest or id for display.
const short = (s: string) => (s.length > 22 ? `${s.slice(0, 14)}…${s.slice(-4)}` : s)

// SevPill is the severity pill with its icon.
function SevPill({ s }: { s: string }) {
  return (
    <span className={`pill ${SEV_TONE[s] ?? 'plain'}`}>
      <span className="sev-icon">{SEV_ICON[s] ?? '?'}</span> {s}
    </span>
  )
}

// FixPill is the fix-status pill.
function FixPill({ s }: { s: string }) {
  const f = FIX[s] ?? { label: s, tone: 'plain' }
  return <span className={`pill ${f.tone}`}>{f.label}</span>
}

// useFindings reads GET /v1/findings with the severity and status filters as query parameters.
function useFindings(severity: string, status: string) {
  const qs = new URLSearchParams()
  if (severity) qs.set('severity', severity)
  if (status) qs.set('status', status)
  const q = qs.toString()
  return useQuery({ queryKey: ['findings', severity, status], queryFn: () => get<{ findings: FindingGroup[] }>(`/v1/findings${q ? '?' + q : ''}`), refetchInterval: 15000, retry: false })
}

/** FindingsView shows the finding groups returned by the daemon, filtered by severity and status. */
export function FindingsView() {
  const [severity, setSeverity] = useState('')
  const [status, setStatus] = useState('')
  const [sel, setSel] = useState('')
  const q = useFindings(severity, status)
  const unfiltered = useFindings('', '') // chip counts always come from the whole list
  const rows = q.data?.findings ?? []
  const all = unfiltered.data?.findings ?? []
  const cur = rows.find((f) => f.key === sel) ?? rows[0]
  const count = (s: string) => all.filter((f) => f.severity === s).length
  const occurrences = all.reduce((n, f) => n + f.occurrences, 0)

  return (
    <section className="page">
      <div className="page-head">
        <h1>Findings</h1>
        <span className="pill">
          <i className="sq" style={{ background: '#7c80f2' }} />
          security
        </span>
        <span className="sub mono">
          {all.length} groups from {occurrences} findings · {occurrences - all.length} duplicates grouped
        </span>
      </div>
      <div className="filters">
        <button className={`chip ${!severity ? 'on' : ''}`} onClick={() => setSeverity('')}>
          All <span className="mono">{all.length}</span>
        </button>
        {SEVERITIES.map((s) => (
          <button key={s} className={`chip ${severity === s ? 'on' : ''}`} onClick={() => setSeverity(s)}>
            <span className={`sev-icon ${SEV_TONE[s]}-text`}>{SEV_ICON[s]}</span> {s} <span className="mono">{count(s)}</span>
          </button>
        ))}
        <span className="grow" />
        <label className="inline small">
          Status
          <select value={status} onChange={(e) => setStatus(e.target.value)}>
            <option value="">any</option>
            {STATUSES.map((s) => (
              <option key={s} value={s}>
                {FIX[s].label}
              </option>
            ))}
          </select>
        </label>
      </div>
      <div className="split">
        <div className="table-wrap">
          <table className="grid">
            <thead>
              <tr>
                <th>Severity</th>
                <th>Source</th>
                <th>Tool</th>
                <th>Rule</th>
                <th>Location</th>
                <th>Seen</th>
                <th>Fix status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((f) => (
                <tr key={f.key} className={f === cur ? 'selected' : ''} onClick={() => setSel(f.key)}>
                  <td>
                    <SevPill s={f.severity} />
                  </td>
                  <td className="mono dim small">{f.source.replace('_', ' ')}</td>
                  <td className="mono dim">{f.tool}</td>
                  <td className="mono strong">{f.rule ?? '—'}</td>
                  <td className="mono dim small">{f.location ?? '—'}</td>
                  <td className="mono dim small">{f.occurrences > 1 ? `×${f.occurrences}` : '1×'}</td>
                  <td>
                    <FixPill s={f.status} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && (
            <p className="empty">
              {q.isError ? 'Findings could not be loaded.' : all.length === 0 ? 'No findings recorded yet. They appear here when a scan or review records finding.recorded events.' : 'No findings match these filters.'}
            </p>
          )}
        </div>
        {cur ? <Detail key={cur.key} f={cur} /> : <aside className="detail" />}
      </div>
    </section>
  )
}

// Detail is the right panel: facts, grouped provenance and fix-status actions.
function Detail({ f }: { f: FindingGroup }) {
  const qc = useQueryClient()
  const live = useLive()
  const [last, setLast] = useState<{ ok: boolean; text: string } | null>(null)
  const project = f.threads.map((id) => live.threads.get(id)?.projectId).find(Boolean)
  const fix = useMutation({
    mutationFn: (): Promise<CommandResult> => {
      if (!project) throw new Error('The finding’s thread has no project yet.')
      return command('/v1/threads', { project_id: project, brief: `Fix ${f.rule ?? f.key} (${f.location ?? 'location unknown'}). ${f.summary ?? ''} Finding group ${f.key}.` })
    },
    onSuccess: (c) => {
      setLast({ ok: true, text: `${c.receipt.command} accepted · receipt ${c.receipt.receipt_id} · request ${c.requestId}` })
      void qc.invalidateQueries({ queryKey: ['threads'] })
    },
    onError: (e: Error) => setLast({ ok: false, text: e instanceof ApiError ? `${e.code}: ${e.message}` : e.message }),
  })
  const noApi = 'The API has no finding-triage endpoint yet (open question)'
  return (
    <aside className="detail">
      <div className="detail-head find-head">
        <h2 className="mono">{f.rule ?? f.key}</h2>
        <div className="pills">
          <SevPill s={f.severity} />
          <FixPill s={f.status} />
        </div>
      </div>
      <div className="card kvs tight">
        <Fact k="Source" v={f.source.replace('_', ' ')} />
        <Fact k="Tool" v={f.tool} />
        <Fact k="Location" v={f.location ?? '—'} />
        {f.fingerprint && <Fact k="Fingerprint" v={short(f.fingerprint)} />}
        <Fact k="Threads" v={f.threads.map((t) => t.slice(-6)).join(', ')} />
        <Fact k="First seen" v={f.first_seen} />
        <Fact k="Last seen" v={f.last_seen} />
      </div>
      <h6 className="section">
        Grouped from {f.occurrences} finding{f.occurrences === 1 ? '' : 's'} · provenance kept
      </h6>
      {f.provenance.map((p, i) => (
        <div key={i} className="card prov mono small">
          {Object.entries(p)
            .map(([k, v]) => `${k} ${typeof v === 'string' ? short(v) : JSON.stringify(v)}`)
            .join(' · ')}
        </div>
      ))}
      <h6 className="section">Evidence · redacted</h6>
      <pre className="evidence-box mono small">{f.summary ?? 'No summary recorded.'}</pre>
      <h6 className="section">Fix status</h6>
      <div className="actions pad-x">
        <button className="btn primary" disabled={fix.isPending || !project} onClick={() => fix.mutate()}>
          Create fix thread
        </button>
        <button className="btn" disabled title={noApi}>
          Confirm
        </button>
        <button className="btn" disabled title={noApi}>
          False positive…
        </button>
        <button className="btn" disabled title={noApi}>
          Accept risk…
        </button>
      </div>
      <p className="muted small pad-x">False positive and accepted risk need a reason; both are recorded with your name.</p>
      {last && <p className={`receipt mono pad-x ${last.ok ? '' : 'error-text'}`}>{last.text}</p>}
    </aside>
  )
}

// Fact is one label/value row of the detail card.
function Fact({ k, v }: { k: string; v: string }) {
  return (
    <div className="kv">
      <span>{k}</span>
      <div className="mono">{v}</div>
    </div>
  )
}
