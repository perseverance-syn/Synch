// Findings view (docs/design-reference/findings.png): severity chips, findings table and a detail panel.
// The API has no findings read endpoint yet, so the table is folded from finding.recorded events in the
// live store (duplicates grouped by fingerprint, provenance kept). Triage (confirm, false positive,
// accepted risk) has no write endpoint either, so those buttons are disabled; "Create fix thread" uses POST /v1/threads.
import { useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { ApiError, command, type CommandResult } from './api'
import { useLive, type Finding } from './store'

const SEVERITIES = ['critical', 'high', 'medium', 'low', 'info'] as const
const SEV_ICON: Record<string, string> = { critical: '▲▲', high: '▲', medium: '●', low: '▽', info: 'i' }
const SEV_TONE: Record<string, string> = { critical: 'red', high: 'amber', medium: 'amber', low: 'blue', info: 'plain', unknown: 'plain' }
const FIX: Record<string, { label: string; tone: string }> = {
  open: { label: 'open', tone: 'amber' },
  confirmed: { label: 'confirmed', tone: 'red' },
  fixed: { label: 'fixed', tone: 'green' },
  false_positive: { label: 'false positive', tone: 'plain' },
  accepted_risk: { label: 'accepted risk', tone: 'violet' },
}

// sevRank orders severities from critical down; unknown sorts last.
const sevRank = (s: string) => ((SEVERITIES as readonly string[]).indexOf(s) === -1 ? 9 : (SEVERITIES as readonly string[]).indexOf(s))

// status returns the fix status of a finding; a finding with no triage is open.
const status = (f: Finding) => f.triage?.status ?? 'open'

// where renders the finding location as path:line, with the image layer or commit when present.
function where(f: Finding): string {
  const l = f.location
  return `${l.path}${l.start_line ? ':' + l.start_line : ''}${l.image_layer ? ' ' + l.image_layer : ''}${l.commit ? ' @ ' + l.commit.slice(0, 7) : ''}`
}

// short abbreviates a digest for display.
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

/** FindingsView shows grouped scanner findings from the event stream, filtered by severity. */
export function FindingsView() {
  const live = useLive()
  const all = [...live.findings.values()].sort((a, b) => sevRank(a.severity) - sevRank(b.severity))
  const [filter, setFilter] = useState('all')
  const [sel, setSel] = useState('')
  const rows = filter === 'all' ? all : all.filter((f) => f.severity === filter)
  const cur = all.find((f) => f.fingerprint === sel) ?? rows[0]
  const count = (s: string) => all.filter((f) => f.severity === s).length
  const revision = all.length ? all[all.length - 1].revision : ''

  return (
    <section className="page">
      <div className="page-head">
        <h1>Findings</h1>
        <span className="pill">
          <i className="sq" style={{ background: '#7c80f2' }} />
          security
        </span>
        <span className="sub mono">
          {revision && `revision ${revision.slice(0, 7)} · `}
          {all.length} findings from {live.rawFindings} raw · {live.rawFindings - all.length} duplicates grouped
        </span>
      </div>
      <div className="filters">
        <button className={`chip ${filter === 'all' ? 'on' : ''}`} onClick={() => setFilter('all')}>
          All <span className="mono">{all.length}</span>
        </button>
        {SEVERITIES.map((s) => (
          <button key={s} className={`chip ${filter === s ? 'on' : ''}`} onClick={() => setFilter(s)}>
            <span className={`sev-icon ${SEV_TONE[s]}-text`}>{SEV_ICON[s]}</span> {s} <span className="mono">{count(s)}</span>
          </button>
        ))}
      </div>
      <div className="split">
        <div className="table-wrap">
          <table className="grid">
            <thead>
              <tr>
                <th>Severity</th>
                <th>Tool</th>
                <th>Rule</th>
                <th>Component</th>
                <th>Location</th>
                <th>Scans</th>
                <th>Fix status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((f) => (
                <tr key={f.fingerprint} className={f === cur ? 'selected' : ''} onClick={() => setSel(f.fingerprint)}>
                  <td>
                    <SevPill s={f.severity} />
                  </td>
                  <td className="mono dim">{f.tool.name}</td>
                  <td className="mono strong">{f.rule.id}</td>
                  <td>{f.component.type === 'secret' ? 'secret (redacted)' : `${f.component.name}${f.component.version ? ' ' + f.component.version : ''}`}</td>
                  <td className="mono dim small">{where(f)}</td>
                  <td className="mono dim small">{f.provenance.length > 1 ? `×${f.provenance.length} scans` : '1 scan'}</td>
                  <td>
                    <FixPill s={status(f)} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {rows.length === 0 && (
            <p className="empty">
              {all.length === 0 ? 'No scanner findings recorded yet. They appear here when a scan records finding.recorded events.' : 'No findings at this severity.'}
            </p>
          )}
        </div>
        {cur ? <Detail key={cur.fingerprint} f={cur} /> : <aside className="detail" />}
      </div>
    </section>
  )
}

// Detail is the right panel: facts, grouped scans, redacted evidence and fix-status actions.
function Detail({ f }: { f: Finding }) {
  const qc = useQueryClient()
  const live = useLive()
  const [last, setLast] = useState<{ ok: boolean; text: string } | null>(null)
  const project = live.threads.get(f.threadId)?.projectId
  const fix = useMutation({
    mutationFn: (): Promise<CommandResult> => {
      if (!project) throw new Error('The finding’s thread has no project yet.')
      const at = `${f.location.path}${f.location.start_line ? ':' + f.location.start_line : ''}`
      const to = f.component.fixed_version ? ` Upgrade to ${f.component.fixed_version}.` : ''
      return command('/v1/threads', { project_id: project, brief: `Fix ${f.rule.id} in ${f.component.name} (${at}).${to} Finding fingerprint ${f.fingerprint}.` })
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
        <h2 className="mono">{f.rule.id}</h2>
        <div className="pills">
          <SevPill s={f.severity} />
          <FixPill s={status(f)} />
        </div>
      </div>
      <div className="card kvs tight">
        <Fact k="Component" v={f.component.type === 'secret' ? 'secret (redacted)' : `${f.component.name}${f.component.version ? ' ' + f.component.version : ''}`} />
        <Fact k="Tool" v={`${f.tool.name} ${f.tool.version}`} />
        <Fact k="Location" v={where(f)} />
        {f.component.fixed_version && <Fact k="Fixed in" v={f.component.fixed_version} />}
        <Fact k="Fingerprint" v={short(f.fingerprint)} />
        <Fact k="Revision" v={f.revision.slice(0, 7)} />
      </div>
      <h6 className="section">
        Grouped from {f.provenance.length} scan{f.provenance.length === 1 ? '' : 's'} · provenance kept
      </h6>
      {f.provenance.map((p) => (
        <div key={p.scan_id + p.report.digest} className="card prov mono small">
          scan {short(p.scan_id)} · {p.tool}
          {f.tool.db_version ? ` · db ${f.tool.db_version}` : ''}
          <br />
          report {short(p.report.digest)} · redacted{p.native_id ? ` · native id ${p.native_id}` : ''}
        </div>
      ))}
      <h6 className="section">Evidence · redacted</h6>
      <pre className="evidence-box mono small">
        {f.evidence.summary}
        {f.evidence.snippet_redacted ? '\n' + f.evidence.snippet_redacted : ''}
        {f.component.fixed_version ? <span className="ok-text">{`\nUpgrade to v${f.component.fixed_version}.`}</span> : null}
      </pre>
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
