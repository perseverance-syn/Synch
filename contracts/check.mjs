#!/usr/bin/env node
// Runs every contract check. Usage: node contracts/check.mjs   (Node >= 20, network for first npx fetch)
// Exits non-zero on any unexpected result. Tool versions are pinned below.
import { execSync } from 'node:child_process';
import { readFileSync, readdirSync, statSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { join, dirname, relative, isAbsolute } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const ROOT = dirname(fileURLToPath(import.meta.url));
const AJV = 'npx -y -p ajv-cli@5.0.0 -p ajv-formats@3.0.1 ajv';
const REDOCLY = 'npx -y @redocly/cli@2.79.0';
const SPEC = '--spec=draft2020 -c ajv-formats';
const SCHEMAS = {
  envelope: 'events/envelope.schema.json',
  runner: 'events/runner-event.schema.json',
  policy: 'policy/rule-file.schema.json',
  finding: 'drivers/finding.schema.json',
  catalogue: 'events/catalogue.schema.json',
};
const REFS = `-r ${SCHEMAS.envelope} -r ${SCHEMAS.policy} -r ${SCHEMAS.finding}`;
const refsFor = (k) => (k === 'catalogue' ? REFS : k === 'runner' ? `${REFS} -r ${SCHEMAS.catalogue}` : '');

// Producer cross-field checks that JSON Schema cannot express (events/catalogue.md section 4, policy/rule-file.md section 5.1).
const ULID_OF_STREAM = (s) => (typeof s === 'string' && s.startsWith('thread:') ? s.slice(7) : null);
// ctx = producer state the check needs (fixture field "context"): { grants: [{id, thread}], terminal_threads: [ulid] }.
function crossChecks(kind, d, ctx = {}) {
  const v = [];
  const approvals = (list) => (list ?? []).forEach((a) => { if (a.approved_at && a.expires_at && !(Date.parse(a.expires_at) > Date.parse(a.approved_at))) v.push('P1 expires_at must be after approved_at'); });
  if (kind === 'policy') approvals(d.approvals);
  if (kind === 'catalogue') {
    const st = ULID_OF_STREAM(d.stream);
    if (st && d.thread_id !== st) v.push('C1 thread_id must equal the stream owner');
    const p = d.payload ?? {};
    if (d.type === 'thread.created' && p.thread_id !== d.thread_id) v.push('C2 thread.created payload.thread_id must equal thread_id');
    if ((d.type === 'approval.recorded' || d.type === 'grant.issued') && p.thread !== d.thread_id) v.push('C3 record thread must equal thread_id');
    if (d.source && d.actor?.id !== d.source.runner) v.push('C4 actor.id must equal source.runner');
    for (const e of [p.resource_evidence, p.absence_lookup]) if (e && e.client_token !== `synch-${p.external_request_id}`) v.push('C5 client_token must be synch-<external_request_id>');
    if (d.type === 'child.assignment_recorded' && p.parent_thread_id !== d.thread_id) v.push('C7 parent_thread_id must equal thread_id');
    if (d.type === 'audit.setting_recorded' && p.thread_id !== d.thread_id) v.push('C7 payload.thread_id must equal thread_id');
    if (d.type === 'grant.revoked') {
      const g = (ctx.grants ?? []).find((x) => x.id === p.grant_id);
      if (!g) v.push('C6 grant_id must resolve to a recorded grant');
      else if (p.cause === 'thread_ended') {
        if (p.ended_thread !== g.thread) v.push('C6 ended_thread must be the grant thread');
        if (!(ctx.terminal_threads ?? []).includes(p.ended_thread)) v.push('C6 ended_thread must be terminal before the event');
      }
    }
    if (d.type === 'approval.recorded') approvals([p]);
  }
  return v;
}

let failures = 0;
const report = (pass, msg, detail = '') => {
  console.log(`${pass ? 'PASS' : 'FAIL'}  ${msg}`);
  if (!pass) { failures++; if (detail) console.log(detail.trim().split('\n').slice(0, 15).map((l) => '      ' + l).join('\n')); }
};
const sh = (cmd) => {
  try { return { code: 0, out: execSync(cmd, { cwd: ROOT, stdio: 'pipe', encoding: 'utf8' }) }; }
  catch (e) { return { code: e.status ?? 1, out: `${e.stdout ?? ''}${e.stderr ?? ''}` }; }
};
const walk = (d) => readdirSync(d).flatMap((f) => {
  const p = join(d, f);
  return statSync(p).isDirectory() ? (f === 'node_modules' ? [] : walk(p)) : [p];
});

// 1. JSON parse
for (const f of walk(ROOT).filter((p) => p.endsWith('.json'))) {
  let err = '';
  try { JSON.parse(readFileSync(f, 'utf8')); } catch (e) { err = e.message; }
  report(!err, `parse ${relative(ROOT, f)}`, err);
}

// 2. Meta-validation (compile) of every schema
for (const [k, s] of Object.entries(SCHEMAS)) {
  const r = sh(`${AJV} compile ${SPEC} ${refsFor(k)} -s ${s}`);
  report(r.code === 0, `compile ${s}`, r.out);
}

// 3. Positive example files
// Classify an Ajv run: 'valid' / 'invalid' only when Ajv printed its verdict line for this data file;
// anything else (read error, schema compile error, bad invocation) is 'error' and fails the run.
const classify = (r, file) => {
  const norm = (s) => s.replaceAll('\\', '/');
  const lines = norm(r.out).split('\n').map((l) => l.trim());
  const f = norm(file);
  if (r.code === 0 && lines.includes(`${f} valid`)) return 'valid';
  if (r.code !== 0 && lines.some((l) => l === `${f} invalid`)) return 'invalid';
  return 'error';
};
const validate = (k, file, ctx) => {
  const r = sh(`${AJV} validate ${SPEC} --strict=false ${refsFor(k)} -s "${SCHEMAS[k]}" -d "${file}"`);
  let got = classify(r, file);
  let why = r.out;
  if (got === 'valid') {
    const v = crossChecks(k, JSON.parse(readFileSync(isAbsolute(file) ? file : join(ROOT, file), 'utf8')), ctx);
    if (v.length) { got = 'invalid'; why = v.join('\n'); }
  }
  return { got, out: why };
};
const examples = [
  ...readdirSync(join(ROOT, 'policy/examples')).map((f) => ['policy', `policy/examples/${f}`]),
  ...readdirSync(join(ROOT, 'examples/events')).map((f) => ['catalogue', `examples/events/${f}`]),
];
for (const [k, f] of examples) {
  const r = validate(k, f);
  report(r.got === 'valid', `valid   ${f} (got ${r.got})`, r.out);
}

const tmp = mkdtempSync(join(tmpdir(), 'synch-contracts-'));

// 4. OpenAPI 3.1: header guard, lint, then extract component schemas for the fixture cases
{
  const head = readFileSync(join(ROOT, 'api/openapi.yaml'), 'utf8');
  report(/^openapi: 3\.1\.\d+\s*$/m.test(head) && /^paths:/m.test(head) && /^components:/m.test(head), 'openapi.yaml declares openapi 3.1.x with paths and components');
  const r = sh(`${REDOCLY} lint api/openapi.yaml`);
  report(r.code === 0 && !/warning/i.test(r.out), 'redocly lint api/openapi.yaml (no errors, no warnings)', r.out);
  const b = sh(`${REDOCLY} bundle api/openapi.yaml --dereferenced --ext json -o "${join(tmp, 'openapi.json')}"`);
  report(b.code === 0, 'redocly bundle --dereferenced', b.out);
  if (b.code === 0) {
    const comps = JSON.parse(readFileSync(join(tmp, 'openapi.json'), 'utf8')).components.schemas;
    for (const [name, s] of Object.entries(comps)) {
      const file = join(tmp, `openapi-${name}.schema.json`);
      writeFileSync(file, JSON.stringify({ $schema: 'https://json-schema.org/draft/2020-12/schema', ...s }));
      SCHEMAS[`openapi#${name}`] = file;
    }
  }
}

// 5. Fixture cases (positive and negative)
const fx = JSON.parse(readFileSync(join(ROOT, 'tests/cases.json'), 'utf8'));
const expand = (v) => {
  if (Array.isArray(v)) return v.map(expand);
  if (v === null || typeof v !== 'object') return v;
  let out = {};
  if (v.$base) out = structuredClone(fx.bases[v.$base]);
  for (const [key, val] of Object.entries(v)) if (key !== '$base' && key !== '$delete') out[key] = expand(val);
  for (const key of v.$delete ?? []) delete out[key];
  return out;
};
try {
  fx.cases.forEach((c, i) => {
    if (!SCHEMAS[c.schema]) { report(false, `unknown schema ${c.schema} for case ${c.name}`); return; }
    const data = expand(c.base ? { $base: c.base, ...c.data } : c.data);
    const file = join(tmp, `case-${i}.json`);
    writeFileSync(file, JSON.stringify(data));
    const r = validate(c.schema, file, c.context);
    const got = r.got;
    report(got === c.expect, `${c.expect === 'valid' ? 'valid  ' : 'reject '} ${c.name} (${c.schema}: got ${got})`, got === c.expect ? '' : r.out);
  });
} finally { rmSync(tmp, { recursive: true, force: true }); }

// 6. Receipt admission + payload digest vectors (reference implementation of receipts.md sections 2-3)
class Reject extends Error {}
const fail = (reason) => { throw new Reject(reason); };
function parseAdmitted(text) {
  let i = 0;
  const ws = () => { while (i < text.length && ' \t\n\r'.includes(text[i])) i++; };
  const str = () => {
    const m = /^"(?:[^"\\\u0000-\u001f]|\\["\\/bfnrt]|\\u[0-9a-fA-F]{4})*"/.exec(text.slice(i));
    if (!m) fail('malformed_json');
    i += m[0].length;
    const s = JSON.parse(m[0]);
    if (!s.isWellFormed()) fail('invalid_unicode');
    return s;
  };
  const val = () => {
    ws();
    const c = text[i];
    if (c === '{') {
      // Null-prototype object: every member name (incl. __proto__, constructor) becomes an own property.
      i++; const o = Object.create(null); const seen = new Set(); ws();
      if (text[i] === '}') { i++; return o; }
      for (;;) {
        ws(); if (text[i] !== '"') fail('malformed_json');
        const k = str(); if (seen.has(k)) fail('duplicate_member'); seen.add(k);
        ws(); if (text[i++] !== ':') fail('malformed_json');
        o[k] = val(); ws();
        const d = text[i++]; if (d === '}') return o; if (d !== ',') fail('malformed_json');
      }
    }
    if (c === '[') {
      i++; const a = []; ws();
      if (text[i] === ']') { i++; return a; }
      for (;;) { a.push(val()); ws(); const d = text[i++]; if (d === ']') return a; if (d !== ',') fail('malformed_json'); }
    }
    if (c === '"') return str();
    for (const [lit, v] of [['true', true], ['false', false], ['null', null]]) if (text.startsWith(lit, i)) { i += lit.length; return v; }
    const m = /^-?(0|[1-9][0-9]*)([.eE][-+0-9.eE]*)?/.exec(text.slice(i));
    if (!m) fail('malformed_json');
    i += m[0].length;
    if (m[2] || m[0] === '-0') fail('number_not_integer');
    const n = Number(m[0]);
    if (!Number.isSafeInteger(n)) fail('number_out_of_range');
    return n;
  };
  const v = val(); ws();
  if (i !== text.length) fail('malformed_json');
  return v;
}
const BODY_RULE = { '/v1/grants/{grant_id}/revoke': 'none' }; // every other mutating route: 'object'
function admit(vec) {
  const rule = BODY_RULE[vec.route] ?? 'object';
  let body = null;
  if (!vec.body_absent) {
    const bytes = vec.body_hex !== undefined ? Buffer.from(vec.body_hex, 'hex') : Buffer.from(vec.body_text, 'utf8');
    if (rule === 'none') fail('body_not_allowed');
    let text;
    try { text = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(bytes); } catch { fail('invalid_utf8'); }
    body = parseAdmitted(text);
  }
  if (rule === 'object' && (body === null || typeof body !== 'object' || Array.isArray(body))) fail('body_must_be_object');
  return body;
}
const jcs = (v) => (v === null || typeof v !== 'object' ? JSON.stringify(v)
  : Array.isArray(v) ? `[${v.map(jcs).join(',')}]`
  : `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${jcs(v[k])}`).join(',')}}`);
const digest = (vec, body) => 'sha256:' + createHash('sha256')
  .update(jcs({ method: vec.method, route: vec.route, path_params: vec.path_params ?? {}, body }), 'utf8').digest('hex');

const vectors = JSON.parse(readFileSync(join(ROOT, 'tests/digest-vectors.json'), 'utf8')).vectors;
const byName = {};
for (const v of vectors) {
  let got;
  try { got = { digest: digest(v, admit(v)) }; } catch (e) { if (!(e instanceof Reject)) throw e; got = { reject: e.message }; }
  byName[v.name] = got;
  const pass = v.expect.reject ? got.reject === v.expect.reject : got.digest === v.expect.digest;
  report(pass, `vector  ${v.name} -> ${got.reject ? 'reject ' + got.reject : got.digest}`);
}
for (const v of vectors) {
  if (v.same_as) report(byName[v.name].digest === byName[v.same_as].digest, `vector  ${v.name} digest == ${v.same_as}`);
  if (v.differs_from) report(byName[v.name].digest !== byName[v.differs_from].digest, `vector  ${v.name} digest != ${v.differs_from}`);
}

console.log(failures ? `\n${failures} check(s) FAILED` : '\nALL CHECKS PASSED');
process.exit(failures ? 1 : 0);
