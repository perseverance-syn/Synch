// Electron main process: owns the window, the daemon settings and every HTTP call to the daemon.
// The renderer never sees the token; it asks this process to make API calls and to follow SSE streams
// over IPC. This also avoids CORS, since the daemon serves loopback clients, not browser origins.
// Closing the window only quits the desktop; the daemon is a separate process and keeps running.
const { app, BrowserWindow, ipcMain, safeStorage } = require('electron')
const fs = require('node:fs')
const path = require('node:path')

const DEFAULT_URL = 'http://127.0.0.1:7420'

// --- Settings: daemon URL in plain JSON, token encrypted with safeStorage (OS keychain / DPAPI) ---

// settingsPath returns the settings file in the per-user app data directory.
function settingsPath() {
  return path.join(app.getPath('userData'), 'settings.json')
}

// readSettings loads the stored URL and encrypted token, or defaults when none are stored yet.
function readSettings() {
  try {
    return JSON.parse(fs.readFileSync(settingsPath(), 'utf8'))
  } catch {
    return { url: DEFAULT_URL }
  }
}

let cached = null // { url, token } decrypted once per process (U2: read once from settings)

// connection returns the daemon URL and decrypted token, reading the settings file only once.
async function connection() {
  if (cached) return cached
  const stored = readSettings()
  let token = ''
  if (stored.token && (await safeStorage.isAsyncEncryptionAvailable())) {
    try {
      token = (await safeStorage.decryptStringAsync(Buffer.from(stored.token, 'base64'))).result
    } catch {
      token = '' // unreadable on this machine/user: treat as first run
    }
  }
  cached = { url: stored.url || DEFAULT_URL, token }
  return cached
}

// loopbackURL accepts only http URLs on a loopback host: the API listens on loopback only
// and remote daemons are reached through an SSH tunnel (openapi.yaml, dec. 16).
function loopbackURL(raw) {
  const u = new URL(raw)
  if (u.protocol !== 'http:' || !['127.0.0.1', 'localhost', '[::1]'].includes(u.hostname)) {
    throw new Error('The daemon URL must be http:// on 127.0.0.1, localhost or [::1].')
  }
  return u.origin
}

// saveSettings encrypts the token and writes the settings file. It refuses to store a token in
// plain text when OS encryption is unavailable. An empty token keeps the stored one.
async function saveSettings(_e, { url, token }) {
  const origin = loopbackURL(url)
  if (!(await safeStorage.isAsyncEncryptionAvailable())) throw new Error('OS secure storage is unavailable; the token was not saved.')
  const nextToken = token || (await connection()).token
  const record = { url: origin, token: (await safeStorage.encryptStringAsync(nextToken)).toString('base64') }
  fs.mkdirSync(path.dirname(settingsPath()), { recursive: true })
  fs.writeFileSync(settingsPath(), JSON.stringify(record), { mode: 0o600 })
  cached = { url: origin, token: nextToken }
}

// forgetToken removes the stored token so the app returns to the connect screen.
async function forgetToken() {
  const { url } = await connection()
  fs.mkdirSync(path.dirname(settingsPath()), { recursive: true })
  fs.writeFileSync(settingsPath(), JSON.stringify({ url }), { mode: 0o600 })
  cached = { url, token: '' }
}

// --- API proxy: one request per call; receipt headers are passed back to the renderer ---

// apiRequest performs one authenticated call. An unreachable daemon resolves to { unreachable: true }
// rather than throwing, so the renderer can show its banner and keep working.
async function apiRequest(_e, { method, path: p, body, requestId }) {
  const { url, token } = await connection()
  const headers = { Authorization: `Bearer ${token}` }
  if (body !== undefined) headers['Content-Type'] = 'application/json'
  if (requestId) headers['Synch-Request-Id'] = requestId
  try {
    const res = await fetch(url + p, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      redirect: 'error',
      signal: AbortSignal.timeout(10000),
    })
    return {
      status: res.status,
      body: await res.text(),
      receiptId: res.headers.get('Synch-Receipt-Id'),
      replayed: res.headers.get('Synch-Receipt-Replayed') === 'true',
    }
  } catch (err) {
    return { unreachable: true, message: String(err && err.cause ? err.cause.code || err.cause.message : err) }
  }
}

// --- SSE follow: main parses frames; the renderer owns the cursor, gap and resync rules ---

const streams = new Map() // stream handle -> AbortController
let nextStream = 1

// openStream starts one SSE connection resuming after `after` (sent as Last-Event-ID) and forwards
// each frame to the renderer as 'stream:frame'. It always ends with one 'end' or 'error' frame
// unless the renderer closed it.
async function openStream(e, { stream, after }) {
  const handle = nextStream++
  const ctl = new AbortController()
  streams.set(handle, ctl)
  const send = (frame) => {
    if (!e.sender.isDestroyed()) e.sender.send('stream:frame', { handle, ...frame })
  }
  const { url, token } = await connection()
  const headers = { Authorization: `Bearer ${token}`, Accept: 'text/event-stream' }
  if (after > 0) headers['Last-Event-ID'] = String(after)
  ;(async () => {
    try {
      const res = await fetch(`${url}/v1/streams/${encodeURIComponent(stream)}/events`, { headers, signal: ctl.signal })
      if (res.status !== 200) {
        let code = ''
        try {
          code = JSON.parse(await res.text()).code || ''
        } catch {}
        return send({ kind: 'error', status: res.status, code })
      }
      const decoder = new TextDecoder()
      let buf = ''
      for await (const chunk of res.body) {
        buf += decoder.decode(chunk, { stream: true }).replace(/\r\n/g, '\n')
        let cut
        // One SSE frame ends at a blank line; comment lines (heartbeats) carry no fields.
        while ((cut = buf.indexOf('\n\n')) >= 0) {
          const frame = { event: 'message', id: '', data: [] }
          for (const line of buf.slice(0, cut).split('\n')) {
            const colon = line.indexOf(':')
            if (colon === 0) continue
            const field = colon < 0 ? line : line.slice(0, colon)
            const value = colon < 0 ? '' : line.slice(colon + 1).replace(/^ /, '')
            if (field === 'event') frame.event = value
            else if (field === 'id') frame.id = value
            else if (field === 'data') frame.data.push(value)
          }
          buf = buf.slice(cut + 2)
          if (frame.data.length || frame.event !== 'message') {
            send({ kind: 'frame', event: frame.event, id: frame.id, data: frame.data.join('\n') })
          }
        }
      }
      send({ kind: 'end' })
    } catch {
      if (!ctl.signal.aborted) send({ kind: 'error', status: 0, code: 'unreachable' })
    } finally {
      streams.delete(handle)
    }
  })()
  return handle
}

// closeStream aborts one SSE connection opened by openStream.
function closeStream(_e, handle) {
  const ctl = streams.get(handle)
  if (ctl) ctl.abort()
  streams.delete(handle)
}

// --- Window ---

// openWindow creates the locked-down window and loads the Vite build from disk.
function openWindow() {
  const window = new BrowserWindow({
    title: 'Synch',
    width: 1600,
    height: 1000,
    backgroundColor: '#111214',
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, 'preload.cjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: true,
      webSecurity: true,
    },
  })
  // No remote content: block navigation away from the bundled page and every new window.
  window.webContents.on('will-navigate', (event) => event.preventDefault())
  window.webContents.setWindowOpenHandler(() => ({ action: 'deny' }))
  return window.loadFile(path.join(__dirname, 'dist', 'index.html'))
}

ipcMain.handle('settings:get', async () => {
  const { url, token } = await connection()
  return { url, hasToken: token !== '' }
})
ipcMain.handle('settings:save', saveSettings)
ipcMain.handle('settings:forget', forgetToken)
ipcMain.handle('api:request', apiRequest)
ipcMain.handle('stream:open', openStream)
ipcMain.handle('stream:close', closeStream)

app.whenReady().then(openWindow)
app.on('activate', () => {
  if (BrowserWindow.getAllWindows().length === 0) openWindow()
})
app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})
