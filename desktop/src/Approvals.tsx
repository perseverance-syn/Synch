// Approvals view (docs/design-reference/approvals.png): pending approval requests, the exact bounded
// approval the operator is about to give, and recently decided approvals. Requests come from
// policy.decided events with outcome waiting_approval; the approval itself is POST /v1/threads/{id}/approvals.
// Evidence (the five check words, audit) is shown beside it but is never part of the approval.
import { useEffect, useState, type ReactNode } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError, command, get, type CommandResult, type ThreadView } from './api'
import { useLive, type ApprovalRecord, type ApprovalRequest } from './store'
import { AuditPill, CHECKS, EXECUTION, roleColor } from './Threads'

/** OPERATIONAL actions must bind the plan digest and state rollback and irreversible effects (openapi CreateApproval). */
const OPERATIONAL = ['net.firewall_change', 'net.network_change', 'deploy.production']

// useNow re-renders once a second so the expiry countdown ticks.
function useNow(): number {
  const [now, setNow] = useState(Date.now())
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000)
    return () => clearInterval(t)
  }, [])
  return now
}

// utc formats an instant as HH:MM UTC.
const utc = (ms: number) => new Date(ms).toISOString().slice(11, 16) + ' UTC'

// left renders the time remaining as "11 min 40 s", or "expired".
function left(ms: number): string {
  if (ms <= 0) return 'expired'
  const s = Math.floor(ms / 1000)
  return s >= 3600 ? `${Math.floor(s / 3600)} h ${Math.floor((s % 3600) / 60)} min` : `${Math.floor(s / 60)} min ${s % 60} s`
}

// short abbreviates a long id or digest with an ellipsis, as the design does.
const short = (s: string, n = 14) => (s.length > n + 8 ? `${s.slice(0, n)}…${s.slice(-6)}` : s)

/** ApprovalsView lists pending requests and recent decisions and shows the selected one in full. */
export function ApprovalsView() {
  const live = useLive()
  const pending = live.approvalRequests.filter((r) => r.status === 'pending')
  const decided = [...live.approvals].reverse().slice(0, 20)
  const [sel, setSel] = useState('')
  // Selection is a request event id or an approval id; fall back to the first pending request.
  // An approved request stays selected so its receipt remains on screen until the operator moves on.
  const request = live.approvalRequests.find((r) => r.id === sel) ?? (decided.some((a) => a.id === sel) ? undefined : pending[0])
  const record = request ? undefined : decided.find((a) => a.id === sel)

  return (
    <section className="page">
      <div className="page-head">
        <h1>Approvals</h1>
        <span className="pill amber">{pending.length} pending</span>
        <span className="sub-text">Approval is separate from audit and acceptance.</span>
      </div>
      <div className="appr">
        <div className="appr-list">
          {pending.length === 0 && <p className="empty small">No approvals are waiting.</p>}
          {pending.map((r) => (
            <button key={r.id} className={`appr-item ${r === request ? 'on' : ''}`} onClick={() => setSel(r.id)}>
              <b>{r.action}</b>
              <span className="mono muted small">
                {r.target || r.machine} · thread {short(r.threadId, 8)}
              </span>
              <span className="pills">
                <span className="pill amber">Approval needed</span>
                <span className="pill mono">waiting since {utc(Date.parse(r.at)).slice(0, 5)}</span>
              </span>
            </button>
          ))}
          <h6 className="section">Recently decided</h6>
          {decided.length === 0 && <p className="muted small pad-x">Nothing decided yet.</p>}
          {decided.map((a) => (
            <button key={a.id} className={`appr-done mono ${a === record ? 'on' : ''}`} onClick={() => setSel(a.id)}>
              {a.actions.join(', ')} · <span className={a.voided ? 'muted' : 'ok-text'}>{a.voided ? `void (${a.voided.replaceAll('_', ' ')})` : 'approved'}</span>{' '}
              {utc(Date.parse(a.approved_at)).slice(0, 5)} · by {a.approver.replace('operator:', '')}
            </button>
          ))}
        </div>
        <div className="appr-detail">
          {request && <Draft key={request.id} r={request} onApproved={() => setSel(request.id)} />}
          {record && <Decided a={record} />}
          {!request && !record && (
            <div className="coming">
              <div className="coming-icon">◇</div>
              <h2>Nothing to approve</h2>
              <p className="muted">An approval request appears here when policy asks for a human before an action runs.</p>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}

// Line is one label/value row of the approval card.
function Line({ k, children }: { k: string; children: ReactNode }) {
  return (
    <div className="kv">
      <span>{k}</span>
      <div>{children}</div>
    </div>
  )
}

// Fields renders the approval card shared by a draft and a recorded approval.
function Fields(p: { target: string; revision?: string; plan?: string; worktree?: string; onWorktree?: (v: string) => void; actions: string[]; expires: number; operational: boolean }) {
  const now = useNow()
  return (
    <div className="card kvs">
      <Line k="Target">
        <span className="mono">{p.target || '—'}</span>
      </Line>
      <Line k="Revision">
        {p.revision ? (
          <span className="mono">{p.revision}</span>
        ) : (
          <span className="mono">
            not used <span className="muted">({p.operational ? 'production deploys bind to the plan digest' : 'bound to the plan digest'})</span>
          </span>
        )}
      </Line>
      <Line k="Plan digest">
        {p.plan ? (
          <span className="mono">
            {short(p.plan)} <span className="muted">· {p.operational ? 'matches plan shown' : 'bound'}</span>
          </span>
        ) : (
          <span className="muted mono">no plan revision recorded</span>
        )}
      </Line>
      <Line k="Worktree digest">
        {p.worktree || !p.onWorktree ? (
          <span className="mono">{p.worktree ? short(p.worktree) : 'unavailable'}</span>
        ) : (
          // The API shows no worktree digest for this thread yet, so the operator pastes the digest they reviewed.
          <input className="mono wt-input" placeholder="API shows none yet: paste the sha256 you reviewed" onChange={(e) => p.onWorktree?.(e.target.value.trim())} />
        )}
      </Line>
      <Line k="Actions granted">
        <span className="pills">
          {p.actions.map((a) => (
            <span key={a} className="pill mono">
              {a}
            </span>
          ))}
        </span>
      </Line>
      <Line k="Expires">
        <b>{utc(p.expires)}</b> <span className="mono muted small">{new Date(p.expires).toISOString().replace(/\.\d+Z$/, 'Z')}</span>{' '}
        <b className="amber-text">{p.expires > now ? `in ${left(p.expires - now)}` : 'expired'}</b>
      </Line>
    </div>
  )
}

// Header shows the title row: action name, state pill, department and execution mode.
function Header({ title, threadId, extra }: { title: string; threadId: string; extra: ReactNode }) {
  const { data } = useQuery({ queryKey: ['thread', threadId], queryFn: () => get<ThreadView>(`/v1/threads/${threadId}`), retry: false })
  const live = useLive()
  const role = live.threads.get(threadId)?.role ?? data?.role
  return (
    <div className="appr-title">
      <h2>{title}</h2>
      {extra}
      {role && (
        <span className="pill">
          <i className="sq" style={{ background: roleColor(role) }} />
          {role}
        </span>
      )}
      {data?.execution && <span className="pill amber">{EXECUTION[data.execution]}</span>}
    </div>
  )
}

// Evidence shows the thread's check words and audit status, separately from the approval.
function Evidence({ threadId }: { threadId: string }) {
  const live = useLive()
  const t = live.threads.get(threadId)
  const checks = [...(t?.checks ?? [])]
  return (
    <div className="card">
      <h6 className="section flush">Evidence for this plan · shown separately from approval</h6>
      <div className="evidence">
        {checks.length === 0 && !t?.audit && <span className="muted small">No checks recorded for this thread yet.</span>}
        {checks.map(([name, c]) => (
          <span key={name} className="ev">
            <span className="mono">{name}</span>
            <span className={`pill ${CHECKS[c.status]?.tone ?? ''}`}>
              {CHECKS[c.status]?.icon} {CHECKS[c.status]?.label ?? c.status}
            </span>
            {c.reason && <span className="muted small">{c.reason}</span>}
          </span>
        ))}
        {t?.audit && (
          <span className="ev">
            <span className="mono">audit</span>
            <AuditPill status={t.audit} />
          </span>
        )}
      </div>
    </div>
  )
}

// Draft is the pending request: the approval the operator is about to give, with Approve exactly this.
function Draft({ r, onApproved }: { r: ApprovalRequest; onApproved: () => void }) {
  const qc = useQueryClient()
  const now = useNow()
  const live = useLive()
  const thread = useQuery({ queryKey: ['thread', r.threadId], queryFn: () => get<ThreadView>(`/v1/threads/${r.threadId}`), retry: false })
  const [minutes, setMinutes] = useState(15)
  const [rollback, setRollback] = useState('')
  const [irreversible, setIrreversible] = useState('')
  const [typedWorktree, setTypedWorktree] = useState('')
  const [result, setResult] = useState<{ ok: boolean; text: string } | null>(null)
  const operational = OPERATIONAL.includes(r.action)
  const plan = live.planDigests.get(r.threadId) ?? thread.data?.plan_digest
  const revision = operational ? undefined : thread.data?.revision
  // The expiry counts from when the operator opened this request, so the countdown shown is the one sent.
  const [opened] = useState(Date.now())
  const expires = opened + minutes * 60_000

  const approve = useMutation({
    mutationFn: (): Promise<CommandResult> => {
      const worktree = thread.data?.worktree_digest ?? typedWorktree
      if (!/^sha256:[0-9a-f]{64}$/.test(worktree)) throw new Error('Worktree digest must be sha256: followed by 64 hex characters.')
      if (!revision && !plan) throw new Error('There is no plan digest or revision to bind the approval to.')
      if (operational && (!rollback.trim() || !plan)) throw new Error('This action must bind the plan digest and state a rollback.')
      const body: Record<string, unknown> = {
        worktree_digest: worktree,
        target: r.target || r.machine,
        actions: [r.action],
        expires_at: new Date(expires).toISOString(),
        ...(revision ? { revision } : { plan_digest: plan }),
      }
      if (rollback.trim()) body.rollback = rollback.trim()
      if (operational || irreversible.trim())
        body.irreversible_effects = irreversible
          .split('\n')
          .map((l) => l.trim())
          .filter(Boolean)
      return command(`/v1/threads/${r.threadId}/approvals`, body)
    },
    onSuccess: (c) => {
      onApproved()
      setResult({ ok: true, text: `approval accepted${c.receipt.result?.approval_id ? ' (' + c.receipt.result.approval_id + ')' : ''} · receipt ${c.receipt.receipt_id} · request ${c.requestId}${c.replayed ? ' (replayed)' : ''}` })
      void qc.invalidateQueries({ queryKey: ['threads'] })
    },
    onError: (e: Error) => setResult({ ok: false, text: e instanceof ApiError ? `${e.code}: ${e.message}` : e.message }),
  })

  return (
    <>
      <Header title={r.action} threadId={r.threadId} extra={r.status === 'approved' ? <span className="pill green">Approved</span> : <span className="pill amber">Approval needed</span>} />
      <Fields target={r.target || r.machine} revision={revision} plan={plan} worktree={thread.data?.worktree_digest} onWorktree={thread.isSuccess ? setTypedWorktree : undefined} actions={[r.action]} expires={expires} operational={operational} />
      <div className="two">
        <div className="card">
          <h6 className="section flush">↺ Rollback{operational ? ' (required)' : ''}</h6>
          <textarea rows={3} value={rollback} onChange={(e) => setRollback(e.target.value)} placeholder="Rollback command or verified checkpoint ref" />
        </div>
        <div className="card danger-card">
          <h6 className="section flush">△ Irreversible effects (one per line)</h6>
          <textarea rows={3} value={irreversible} onChange={(e) => setIrreversible(e.target.value)} placeholder="External effects rollback cannot undo. Leave empty to state there are none." />
        </div>
      </div>
      <Evidence threadId={r.threadId} />
      <div className="actions appr-actions">
        <button className="btn primary big" disabled={approve.isPending || expires <= now || r.status !== 'pending'} onClick={() => approve.mutate()}>
          Approve exactly this
        </button>
        <button className="btn danger big" disabled title="The API has no reject-approval endpoint yet (open question)">
          Reject
        </button>
        <button className="btn big" disabled title="The API has no request-changes endpoint yet (open question)">
          Ask for changes
        </button>
        <label className="inline small">
          Expires in
          <input type="number" min={1} max={1440} value={minutes} onChange={(e) => setMinutes(Math.max(1, Number(e.target.value) || 1))} className="mono mins" /> min
        </label>
        <span className="muted small grow">Covers only the target, actions and digest above, until expiry. A changed plan invalidates it.</span>
      </div>
      {result && <p className={`receipt mono ${result.ok ? '' : 'error-text'}`}>{result.text}</p>}
    </>
  )
}

// Decided shows a recorded approval read-only, with its void reason if it was voided.
function Decided({ a }: { a: ApprovalRecord }) {
  return (
    <>
      <Header title={a.actions.join(', ')} threadId={a.thread} extra={<span className={`pill ${a.voided ? '' : 'green'}`}>{a.voided ? `Void · ${a.voided.replaceAll('_', ' ')}` : 'Approved'}</span>} />
      <Fields target={a.target} revision={a.revision} plan={a.plan_digest} worktree={a.worktree_digest} actions={a.actions} expires={a.voided ? 0 : Date.parse(a.expires_at)} operational={OPERATIONAL.some((x) => a.actions.includes(x))} />
      <div className="two">
        <div className="card">
          <h6 className="section flush">↺ Rollback</h6>
          <p className="small">{a.rollback ? <code>{a.rollback}</code> : <span className="muted">none stated</span>}</p>
        </div>
        <div className="card danger-card">
          <h6 className="section flush">△ Irreversible effects</h6>
          {(a.irreversible_effects ?? []).length ? (
            <ul>
              {a.irreversible_effects?.map((x) => (
                <li key={x}>{x}</li>
              ))}
            </ul>
          ) : (
            <p className="small muted">none stated</p>
          )}
        </div>
      </div>
      <Evidence threadId={a.thread} />
      <p className="muted small">
        Approved by <span className="mono">{a.approver}</span> at <span className="mono">{a.approved_at}</span>.
      </p>
    </>
  )
}
